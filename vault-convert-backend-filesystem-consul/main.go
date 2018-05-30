package main

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	consul "github.com/hashicorp/consul/command/kv/impexp"
	vault "github.com/hashicorp/vault/physical"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

func main() {
	app := kingpin.New("vault-convert-backend-filesystem-consul",
		"Convert Vault data from a filesystem storage backend to a Consul storage backend.\n\n"+
			"Input must be a quiesced filesystem tree.\n\n"+
			"Output will be a JSON-serialised Consul KV tree.  This file may be imported into a Consul KV store with 'consul kv import'.\n\n").
		UsageTemplate(kingpin.CompactUsageTemplate)
	consulPath := app.Flag("consul-path",
		"Consul key prefix for Vault data.  See https://www.vaultproject.io/docs/configuration/storage/consul.html#path").
		Default("vault").String()
	inputPath := app.Arg("filesystem-input",
		"Local filesystem path to an existing directory that contains a Vault filesystem storage backend.").
		Required().String()
	outputPath := app.Arg("consul-output",
		"Local filesystem path to the output file.  Any existing file will be overwritten.").
		Required().String()

	kingpin.MustParse(app.Parse(os.Args[1:]))

	c, err := NewConverter(*inputPath, *outputPath, *consulPath)
	if err != nil {
		app.Fatalf("%v", err)
	}
	defer c.Close() // nolint: errcheck
	if err := c.ConvertAll(); err != nil {
		app.Fatalf("%v", err)
	}
}

type Converter struct {
	walker     *Walker
	outputFile io.WriteCloser
	output     *bufio.Writer
	keyPrefix  string
	count      uint64
	firstError error
}

func NewConverter(inputPath, outputPath, consulPath string) (*Converter, error) {
	keyPrefix := path.Clean(consulPath)
	if keyPrefix == "/" || keyPrefix == "." {
		return nil, fmt.Errorf("invalid Consul path: %v", consulPath)
	}

	f, err := os.OpenFile(outputPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return nil, err
	}

	return &Converter{
		walker:     NewWalker(inputPath),
		outputFile: f,
		output:     bufio.NewWriter(f),
		keyPrefix:  keyPrefix,
	}, nil
}

func (c *Converter) Close() error {
	if err := c.output.Flush(); err != nil {
		return err
	}
	return c.outputFile.Close()
}

func (c *Converter) ConvertAll() error {
	for c.Convert() {
	}
	return c.Err()
}

func (c *Converter) Convert() bool {
	datum, err := c.walker.Next()
	if err != nil {
		c.setError(err)
		return false
	}
	if datum == nil {
		trailer := "\n]\n"
		if _, err := c.output.WriteString(trailer); err != nil {
			c.setError(err)
		}
		return false
	}
	c.count++

	var header string
	if c.count == 1 {
		header = "[\n\t"
	} else {
		header = ",\n\t"
	}
	if _, err := c.output.WriteString(header); err != nil {
		c.setError(err)
		return false
	}

	if err := c.convert(datum); err != nil {
		c.setError(fmt.Errorf("while processing input entry #%d: %v", c.count, err))
		return false
	}

	return true
}

func (c *Converter) convert(datum *WalkResult) error {
	f, openError := os.Open(datum.Path)
	if openError != nil {
		return openError
	}
	defer f.Close() // nolint: errcheck

	vaultEntry := &vault.Entry{}
	if err := json.NewDecoder(f).Decode(&vaultEntry); err != nil {
		return err
	}

	consulEntry := convertEntry(vaultEntry, c.keyPrefix)

	compacted, marshalError := json.Marshal(consulEntry)
	if marshalError != nil {
		return marshalError
	}

	indented := &bytes.Buffer{}
	if err := json.Indent(indented, compacted, "\t", "\t"); err != nil {
		return err
	}

	if _, err := c.output.Write(indented.Bytes()); err != nil {
		return err
	}

	return nil
}

func (c *Converter) Err() error {
	return c.firstError
}

func (c *Converter) setError(err error) {
	if c.firstError == nil {
		c.firstError = err
	}
}

func convertEntry(entry *vault.Entry, keyPrefix string) *consul.Entry {
	return &consul.Entry{
		Key:   keyAddPrefix(entry.Key, keyPrefix),
		Value: base64.StdEncoding.EncodeToString(entry.Value),
	}
}

func keyAddPrefix(key, prefix string) string {
	if prefix == "" {
		return key
	}
	pe := strings.Split(prefix, "/")
	ke := strings.Split(key, "/")
	elems := make([]string, 0, len(pe)+len(ke))
	elems = append(elems, pe...)
	elems = append(elems, ke...)
	return strings.Join(elems, "/")
}
