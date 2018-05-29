package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	consul "github.com/hashicorp/consul/command/kv/impexp"
	vault "github.com/hashicorp/vault/physical"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

func main() {
	app := kingpin.New("vault-convert-backend-consul-filesystem",
		"Convert Vault data from a Consul storage backend to a filesystem storage backend.\n\n"+
			"Input must be a JSON-serialised Consul KV tree.  Consul will output KV data in this format with 'consul kv export'.\n\n"+
			"Output will be a filesystem tree.  The root of this tree may be loaded into Vault's filesystem storage backend.\n\n"+
			"Example:\n\n"+
			"    consul kv export vault >vault.json\n"+
			"    vault-backend-convert-consul-file vault.json backend\n").
		UsageTemplate(kingpin.CompactUsageTemplate)
	consulPath := app.Flag("consul-path",
		"Consul key prefix for Vault data.  See https://www.vaultproject.io/docs/configuration/storage/consul.html#path").
		Default("vault").String()
	inputPath := app.Arg("consul-input",
		"Local filesystem path to an existing file that contains a JSON-serialised Consul KV export.").
		Required().String()
	outputPath := app.Arg("filesystem-output",
		"Local filesystem path to the output directory.  The directory will be created if it does not exist.").
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
	inputFile      io.ReadCloser
	input          *json.Decoder
	outputRootPath string
	keyPrefix      string
	count          uint64
	firstErr       error
}

func NewConverter(inputPath, outputPath, consulPath string) (*Converter, error) {
	keyPrefix := path.Clean(consulPath)
	if keyPrefix == "/" || keyPrefix == "." {
		return nil, fmt.Errorf("invalid Consul path: %v", consulPath)
	}

	p := filepath.Clean(outputPath)
	if err := os.MkdirAll(p, 0700); err != nil {
		return nil, err
	}
	if err := os.Chmod(p, 0700); err != nil {
		return nil, err
	}

	f, err := os.Open(inputPath)
	if err != nil {
		return nil, err
	}

	d := json.NewDecoder(f)
	t, err := d.Token()
	if err != nil {
		f.Close() // nolint: errcheck
		return nil, err
	}
	if t != json.Delim('[') {
		f.Close() // nolint: errcheck
		return nil, fmt.Errorf("expected JSON token: '[', got: %s", t)
	}

	return &Converter{
		inputFile:      f,
		input:          d,
		outputRootPath: p,
		keyPrefix:      keyPrefix,
	}, nil
}

func (c *Converter) Close() error {
	return c.inputFile.Close()
}

func (c *Converter) ConvertAll() error {
	for c.Convert() {
	}
	return c.Err()
}

func (c *Converter) Convert() bool {
	if !c.input.More() {
		return false
	}
	c.count++

	if err := c.convert(); err != nil {
		c.setErr(fmt.Errorf("while processing input entry #%d: %s", c.count, err))
		return false
	}

	return true
}

func (c *Converter) convert() error {
	consulEntry := &consul.Entry{}
	if err := c.input.Decode(consulEntry); err != nil {
		return err
	}

	if !keyHasPrefix(consulEntry.Key, c.keyPrefix) {
		// This data does not belong to Vault.  Skip silently.
		return nil
	}

	vaultEntry, err := convertEntry(consulEntry, c.keyPrefix)
	if err != nil {
		return err
	}

	f, err := openFile(c.outputRootPath, vaultEntry.Key)
	if err != nil {
		return err
	}
	defer f.Close()

	if err := json.NewEncoder(f).Encode(vaultEntry); err != nil {
		return err
	}

	return nil
}

func (c *Converter) Err() error {
	return c.firstErr
}

func (c *Converter) setErr(err error) {
	if c.firstErr == nil {
		c.firstErr = err
	}
}

func openFile(root, key string) (*os.File, error) {
	kdir, kbase := path.Split(key)
	// Vault's file backend expects files to be named with a leading underscore.
	p := filepath.Join(root, kdir, "_"+kbase)
	if err := os.MkdirAll(filepath.Dir(p), 0700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(p, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func convertEntry(entry *consul.Entry, keyPrefix string) (*vault.Entry, error) {
	v, err := base64.StdEncoding.DecodeString(entry.Value)
	if err != nil {
		return nil, err
	}

	return &vault.Entry{
		Key:   keyStripPrefix(entry.Key, keyPrefix),
		Value: v,
	}, nil
}

func keyHasPrefix(key, prefix string) bool {
	ke := strings.Split(key, "/")
	pe := strings.Split(prefix, "/")
	if len(ke) < len(pe) {
		return false
	}
	for i := range pe {
		if ke[i] != pe[i] {
			return false
		}
	}
	return true
}

func keyStripPrefix(key, prefix string) string {
	ke := strings.Split(key, "/")
	pe := strings.Split(prefix, "/")
	if len(ke) < len(pe) {
		return strings.Join(ke, "/")
	}
	for i := range pe {
		if ke[i] != pe[i] {
			return strings.Join(ke, "/")
		}
	}
	return strings.Join(ke[len(pe):], "/")
}
