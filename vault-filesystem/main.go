package main

import (
	"compress/gzip"
	"context"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/vault/helper/compressutil"
	"github.com/hashicorp/vault/physical/file"
	"github.com/hashicorp/vault/vault"
	kingpin "gopkg.in/alecthomas/kingpin.v2"

	"github.com/saj/vault-tools/internal/util"
)

const progname = "vault-filesystem"

func main() {
	var (
		app = kingpin.New(progname,
			"Read data from, and write data to, a Vault filesystem storage backend.").
			UsageTemplate(kingpin.CompactUsageTemplate)
		path = app.Flag("backend",
			"Local filesystem path to the Vault storage backend.  The backend must be secured with an AES-GCM barrier.  (At the time of writing, this was the only barrier type implemented in Vault 0.10.)").
			Short('p').PlaceHolder("PATH").Required().String()
		masterKeyPath = app.Flag("master-key",
			"Local filesystem path to the Vault master key file.  The program will interactively prompt for the Vault master key if this flag is not supplied.  The Vault master key must be supplied as a base64 encoded string; vault-construct-master-key outputs the Vault master key in this format.").
			PlaceHolder("PATH").ExistingFile()

		listCmd    = app.Command("list", "List keys.")
		listPrefix = listCmd.Arg("prefix", "").Default("/").String()

		readCmd        = app.Command("read", "Read and decrypt data from a Vault barrier.  Data is written to standard output as a formatted hexdump.")
		readKey        = readCmd.Arg("key", "").Required().String()
		readDecompress = readCmd.Flag("decompress", "Attempt to decompress data prior to output.  Enabled by default; use --no-decompress to disable.").Default("true").Bool()
		readVerbatim   = readCmd.Flag("verbatim", "Omit hexdump; write data byte-for-byte to standard output.").Bool()

		writeCmd      = app.Command("write", "Encrypt and write data to a Vault barrier.  Data is read from standard input by default; use --data to read from a file.")
		writeKey      = writeCmd.Arg("key", "").Required().String()
		writeData     = writeCmd.Flag("data", "Do not read data from standard input.  --data foo will write the string 'foo' to the Vault barrier.  --data @foo will write the contents of the file named 'foo' to the Vault barrier.").String()
		writeCompress = writeCmd.Flag("compress", "Compress data before writing to the Vault barrier.").Bool()
	)

	cmd := kingpin.MustParse(app.Parse(os.Args[1:]))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var masterKey []byte
	if *masterKeyPath != "" {
		var err error
		masterKey, err = readMasterKey(*masterKeyPath)
		if err != nil {
			app.Fatalf("%v", err)
		}
	} else {
		var err error
		masterKey, err = promptForMasterKey()
		if err != nil {
			app.Fatalf("%v", err)
		}
	}

	var barrier *vault.AESGCMBarrier
	{
		var err error
		barrier, err = openBarrier(*path)
		if err != nil {
			app.Fatalf("%v", err)
		}
		if err := barrier.Unseal(ctx, masterKey); err != nil {
			app.Fatalf("%v", err)
		}
	}

	switch cmd {
	case listCmd.FullCommand():
		if err := list(ctx, barrier, *listPrefix); err != nil {
			app.Fatalf("%v", err)
		}

	case readCmd.FullCommand():
		if err := read(ctx, barrier, *readKey, *readDecompress, *readVerbatim); err != nil {
			app.Fatalf("%v", err)
		}

	case writeCmd.FullCommand():
		var value []byte
		if *writeData != "" {
			value = []byte(*writeData)
		} else {
			var err error
			value, err = ioutil.ReadAll(os.Stdin)
			if err != nil {
				app.Fatalf("%v", err)
			}
		}

		if err := write(ctx, barrier, *writeKey, value, *writeCompress); err != nil {
			app.Fatalf("%v", err)
		}
	}
}

func list(ctx context.Context, barrier *vault.AESGCMBarrier, prefix string) error {
	keys, err := barrier.List(ctx, prefix)
	if err != nil {
		return err
	}
	for i := range keys {
		fmt.Println(keys[i])
	}
	return nil
}

func read(ctx context.Context, barrier *vault.AESGCMBarrier, key string, decompress, verbatim bool) error {
	entry, err := barrier.Get(ctx, key)
	if err != nil {
		return err
	}
	if entry == nil {
		return fmt.Errorf("no value at %s", key)
	}

	value := entry.Value
	if decompress {
		decompressed, notCompressed, cErr := compressutil.Decompress(value)
		if cErr != nil {
			return cErr
		}
		if !notCompressed {
			value = decompressed
		}
	}

	if verbatim {
		os.Stdout.Write(value)
	} else {
		d := hex.Dumper(os.Stdout)
		defer d.Close()
		d.Write(value)
	}
	return nil
}

func write(ctx context.Context, barrier *vault.AESGCMBarrier, key string, value []byte, compress bool) error {
	if compress {
		cfg := &compressutil.CompressionConfig{
			Type:                 compressutil.CompressionTypeGzip,
			GzipCompressionLevel: gzip.BestCompression,
		}
		compressed, err := compressutil.Compress(value, cfg)
		if err != nil {
			return err
		}
		value = compressed
	}

	return barrier.Put(ctx, &vault.Entry{
		Key:   key,
		Value: value,
	})
}

func openBarrier(backendPath string) (*vault.AESGCMBarrier, error) {
	logger := hclog.New(&hclog.LoggerOptions{
		Name:  progname,
		Level: hclog.LevelFromString("INFO"),
	})

	conf := map[string]string{"path": backendPath}

	backend, err := file.NewFileBackend(conf, logger)
	if err != nil {
		return nil, err
	}

	return vault.NewAESGCMBarrier(backend)
}

func promptForMasterKey() ([]byte, error) {
	t, err := util.NewTerminal()
	if err != nil {
		return nil, err
	}
	defer t.Restore()

	return t.ReadKeyBase64("Enter master key: ")
}

func readMasterKey(path string) ([]byte, error) {
	key, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	return util.DecodeKeyBase64Byte(key)
}
