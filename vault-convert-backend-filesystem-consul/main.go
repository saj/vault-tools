package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strings"

	consul "github.com/hashicorp/consul/command/kv/impexp"
	hclog "github.com/hashicorp/go-hclog"
	vault "github.com/hashicorp/vault/physical"
	"github.com/hashicorp/vault/physical/file"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

const progname = "vault-convert-backend-filesystem-consul"

func main() {
	app := kingpin.New(progname,
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := convert(ctx, *inputPath, *outputPath, *consulPath); err != nil {
		app.Fatalf("%v", err)
	}
}

func convert(ctx context.Context, inputPath, outputPath, consulPath string) (err error) {
	fb, openError := openFilesystemBackend(ctx, inputPath)
	if openError != nil {
		return openError
	}
	defer func() {
		if closeError := fb.Close(); closeError != nil && err == nil {
			err = closeError
		}
	}()

	cb, openError := openConsulBackend(outputPath, consulPath)
	if openError != nil {
		return openError
	}
	defer func() {
		if closeError := cb.Close(); closeError != nil && err == nil {
			err = closeError
		}
	}()

conversion:
	for {
		ve, readError := fb.ReadEntry(ctx)
		if readError != nil {
			if readError == io.EOF {
				break conversion
			}
			return readError
		}

		ce, conversionError := convertEntry(ve, consulPath)
		if conversionError != nil {
			return conversionError
		}

		if writeError := cb.WriteEntry(ce); writeError != nil {
			return writeError
		}
	}
	return nil
}

type filesystemBackend struct {
	backend vault.Backend
	walker  *filesystemWalker
}

func openFilesystemBackend(ctx context.Context, backendPath string) (*filesystemBackend, error) {
	logger := hclog.New(&hclog.LoggerOptions{
		Name:  progname,
		Level: hclog.LevelFromString("INFO"),
	})

	conf := map[string]string{"path": backendPath}

	backend, err := file.NewFileBackend(conf, logger)
	if err != nil {
		return nil, err
	}

	return &filesystemBackend{
		backend: backend,
		walker:  newFilesystemWalker(ctx, backend, ""),
	}, nil
}

func (be *filesystemBackend) Close() error {
	return nil
}

func (be *filesystemBackend) ReadEntry(ctx context.Context) (*vault.Entry, error) {
	key, err := be.walker.Next()
	if err != nil {
		return nil, err
	}
	if key == "" {
		return nil, io.EOF
	}
	return be.backend.Get(ctx, key)
}

type filesystemWalker struct {
	backend vault.Backend
	keys    chan string
	err     error
}

func newFilesystemWalker(ctx context.Context, backend vault.Backend, root string) *filesystemWalker {
	w := &filesystemWalker{
		backend: backend,
		keys:    make(chan string, 10),
	}
	go func() {
		if err := w.walk(ctx, root); err != nil {
			w.err = err
		}
		close(w.keys)
	}()
	return w
}

func (w *filesystemWalker) walk(ctx context.Context, prefix string) error {
	keys, err := w.backend.List(ctx, prefix)
	if err != nil {
		return err
	}
	sort.Strings(keys)

	for _, k := range keys {
		p := path.Join(prefix, k)

		if k[len(k)-1:] == "/" {
			if err := w.walk(ctx, p); err != nil {
				return err
			}
			continue
		}

		w.keys <- p
	}
	return nil
}

func (w *filesystemWalker) Next() (string, error) {
	var (
		key string
		err error
	)

	select {
	case k, ok := <-w.keys:
		key = k
		// Let the caller flush the channel before we propagate a pending error.
		if !ok {
			err = w.err
		}
	}
	return key, err
}

type consulBackend struct {
	file      io.WriteCloser
	buffer    *bytes.Buffer
	keyPrefix string
}

func openConsulBackend(backendPath, consulPath string) (*consulBackend, error) {
	keyPrefix := path.Clean(consulPath)
	if keyPrefix == "/" || keyPrefix == "." {
		return nil, fmt.Errorf("invalid Consul path: %v", consulPath)
	}

	f, err := os.OpenFile(backendPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return nil, err
	}

	be := &consulBackend{
		file:      f,
		buffer:    &bytes.Buffer{},
		keyPrefix: keyPrefix,
	}

	if err := be.writeHeader(); err != nil {
		f.Close() // nolint: errcheck
		return nil, err
	}
	return be, nil
}

func (be *consulBackend) Close() error {
	if err := be.writeTrailer(); err != nil {
		return err
	}
	if err := be.flush(); err != nil {
		return err
	}
	return be.file.Close()
}

func (be *consulBackend) WriteEntry(entry *consul.Entry) error {
	if err := be.flush(); err != nil {
		return err
	}

	compacted, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	be.buffer.WriteString("\t")
	if err := json.Indent(be.buffer, compacted, "\t", "\t"); err != nil {
		return err
	}
	be.buffer.WriteString(",\n")
	return nil
}

func (be *consulBackend) flush() error {
	if be.buffer.Len() > 0 {
		if _, err := be.buffer.WriteTo(be.file); err != nil {
			return err
		}
		be.buffer.Reset()
	}
	return nil
}

func (be *consulBackend) writeHeader() error {
	be.buffer.WriteString("[\n")
	return nil
}

func (be *consulBackend) writeTrailer() error {
	// Remove trailing JSON element sequence separator and newline.
	l := be.buffer.Len()
	if l >= 2 {
		be.buffer.Truncate(l - 2)
	}
	be.buffer.WriteString("\n]\n")
	return nil
}

func convertEntry(entry *vault.Entry, keyPrefix string) (*consul.Entry, error) {
	return &consul.Entry{
		Key:   keyAddPrefix(entry.Key, keyPrefix),
		Value: base64.StdEncoding.EncodeToString(entry.Value),
	}, nil
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
