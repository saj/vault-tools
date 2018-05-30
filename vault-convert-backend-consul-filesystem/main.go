package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	consul "github.com/hashicorp/consul/command/kv/impexp"
	hclog "github.com/hashicorp/go-hclog"
	vault "github.com/hashicorp/vault/physical"
	"github.com/hashicorp/vault/physical/file"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

const progname = "vault-convert-backend-consul-filesystem"

func main() {
	app := kingpin.New(progname,
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := convert(ctx, *inputPath, *outputPath, *consulPath); err != nil {
		app.Fatalf("%v", err)
	}
}

func convert(ctx context.Context, inputPath, outputPath, consulPath string) (err error) {
	cb, openError := openConsulBackend(inputPath, consulPath)
	if openError != nil {
		return openError
	}
	defer func() {
		if closeError := cb.Close(); closeError != nil && err == nil {
			err = closeError
		}
	}()

	fb, openError := openFilesystemBackend(outputPath)
	if openError != nil {
		return openError
	}
	defer func() {
		if closeError := fb.Close(); closeError != nil && err == nil {
			err = closeError
		}
	}()

conversion:
	for {
		ce, readError := cb.ReadEntry()
		if readError != nil {
			if readError == io.EOF {
				break conversion
			}
			return readError
		}

		ve, conversionError := convertEntry(ce, consulPath)
		if conversionError != nil {
			return conversionError
		}

		if writeError := fb.WriteEntry(ctx, ve); writeError != nil {
			return writeError
		}
	}
	return nil
}

type filesystemBackend struct {
	backend vault.Backend
}

func openFilesystemBackend(backendPath string) (*filesystemBackend, error) {
	logger := hclog.New(&hclog.LoggerOptions{
		Name:  progname,
		Level: hclog.LevelFromString("INFO"),
	})

	conf := map[string]string{"path": backendPath}

	backend, err := file.NewFileBackend(conf, logger)
	if err != nil {
		return nil, err
	}

	return &filesystemBackend{backend: backend}, nil
}

func (be *filesystemBackend) Close() error {
	return nil
}

func (be *filesystemBackend) WriteEntry(ctx context.Context, entry *vault.Entry) error {
	return be.backend.Put(ctx, entry)
}

type consulBackend struct {
	file      io.ReadCloser
	decoder   *json.Decoder
	keyPrefix string
}

func openConsulBackend(backendPath, consulPath string) (*consulBackend, error) {
	keyPrefix := path.Clean(consulPath)
	if keyPrefix == "/" || keyPrefix == "." {
		return nil, fmt.Errorf("invalid Consul path: %v", consulPath)
	}

	f, err := os.Open(backendPath)
	if err != nil {
		return nil, err
	}

	be := &consulBackend{
		file:      f,
		decoder:   json.NewDecoder(f),
		keyPrefix: keyPrefix,
	}

	if err := be.eatHeader(); err != nil {
		f.Close() // nolint: errcheck
		return nil, err
	}
	return be, nil
}

func (be *consulBackend) Close() error {
	return be.file.Close()
}

func (be *consulBackend) ReadEntry() (*consul.Entry, error) {
	entry := &consul.Entry{}
scan:
	for {
		if !be.decoder.More() {
			return nil, io.EOF
		}

		if err := be.decoder.Decode(entry); err != nil {
			return nil, err
		}

		if keyHasPrefix(entry.Key, be.keyPrefix) {
			break scan
		}
	}
	return entry, nil
}

// eatHeader positions a new file cursor at the start of the key-value object
// sequence.
func (be *consulBackend) eatHeader() error {
	t, err := be.decoder.Token()
	if err != nil {
		return err
	}
	if t != json.Delim('[') {
		return fmt.Errorf("expected JSON token: '[', got: %s", t)
	}
	return nil
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
