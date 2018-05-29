package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"

	"github.com/hashicorp/vault/physical"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

func main() {
	app := kingpin.New("vault-backend-convert-consul-file",
		"Convert Vault data from Consul KV format to file format.\n\n"+
			"Input must be a JSON-serialised Consul KV tree.  Consul will output KV data in this format with 'consul kv export'.\n\n"+
			"Output will be a filesystem tree.  The root of this tree may be loaded into Vault's file backend.\n\n"+
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

	var entry physical.Entry

	if err := c.decoder.Decode(&entry); err != nil {
		c.setErr(fmt.Errorf("entry %d: %s", c.count, err))
		return false
	}
	// Sanity check.
	if len(entry.Key) == 0 {
		c.setErr(fmt.Errorf("entry %d: key was empty", c.count))
		return false
	}
	if len(entry.Value) == 0 {
		c.setErr(fmt.Errorf("entry %d: value was empty", c.count))
		return false
	}

	kDir, kBase := path.Split(entry.Key)
	p := filepath.Join(c.outputRootPath, kDir, "_"+kBase)
	d := filepath.Dir(p)
	if d != "." {
		if err := os.MkdirAll(d, 0700); err != nil {
			c.setErr(err)
			return false
		}
	}

	f, err := os.OpenFile(p, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		c.setErr(err)
		return false
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	if err := enc.Encode(&entry); err != nil {
		c.setErr(err)
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
