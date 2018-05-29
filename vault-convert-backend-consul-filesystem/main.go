package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"

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
	inputPath := app.Arg("consul-input",
		"Local filesystem path to an existing file that contains a JSON-serialised Consul KV export.").
		Required().String()
	outputPath := app.Arg("file-output",
		"Local filesystem path to the output directory.  The directory will be created if it does not exist.").
		Required().String()

	kingpin.MustParse(app.Parse(os.Args[1:]))

	c, err := NewConverter(*inputPath, *outputPath)
	if err != nil {
		app.Fatalf("%v", err)
	}
	defer c.Close() // nolint: errcheck
	if err := c.ConvertAll(); err != nil {
		app.Fatalf("%v", err)
	}
}

type Converter struct {
	input          io.ReadCloser
	outputRootPath string
	decoder        *json.Decoder
	count          uint64
	firstErr       error
}

func NewConverter(inputPath, outputPath string) (*Converter, error) {
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
		input:          f,
		outputRootPath: p,
		decoder:        d,
	}, nil
}

func (c *Converter) Close() error {
	return c.input.Close()
}

func (c *Converter) ConvertAll() error {
	for c.Convert() {
	}
	return c.Err()
}

func (c *Converter) Convert() bool {
	if !c.decoder.More() {
		return false
	}
	c.count++

	consulEntry := &consul.Entry{}
	if err := c.decoder.Decode(consulEntry); err != nil {
		c.setErr(fmt.Errorf("entry %d: %s", c.count, err))
		return false
	}

	vaultEntry, err := convertEntry(consulEntry)
	if err != nil {
		c.setErr(fmt.Errorf("entry %d: %v", c.count, err))
		return false
	}

	if err := sanityCheckEntry(vaultEntry); err != nil {
		c.setErr(fmt.Errorf("entry %d: %v", c.count, err))
		return false
	}

	f, err := openFile(c.outputRootPath, vaultEntry.Key)
	if err != nil {
		c.setErr(fmt.Errorf("entry %d: %v", c.count, err))
		return false
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	if err := enc.Encode(vaultEntry); err != nil {
		c.setErr(fmt.Errorf("entry %d: %v", c.count, err))
		return false
	}

	return true
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

func sanityCheckEntry(entry *vault.Entry) error {
	if len(entry.Key) == 0 {
		return errors.New("key is empty")
	}
	if len(entry.Value) == 0 {
		return errors.New("value is empty")
	}
	return nil
}

func convertEntry(entry *consul.Entry) (*vault.Entry, error) {
	d, err := base64.StdEncoding.DecodeString(entry.Value)
	if err != nil {
		return nil, err
	}

	return &vault.Entry{
		Key:   entry.Key,
		Value: d,
	}, nil
}
