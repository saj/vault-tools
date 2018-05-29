package main

import (
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/hashicorp/vault/helper/compressutil"
	"github.com/hashicorp/vault/physical/file"
	"github.com/hashicorp/vault/vault"
	logxi "github.com/mgutz/logxi/v1"
	kingpin "gopkg.in/alecthomas/kingpin.v2"

	"github.com/saj/vault-tools/internal/util"
)

func main() {
	var (
		app = kingpin.New("vault-filesystem",
			"Read data from a Vault filesystem storage backend.").
			UsageTemplate(kingpin.CompactUsageTemplate)
		path = app.Flag("backend-path",
			"Local filesystem path to the Vault storage backend.  The backend must be secured with an AES-GCM barrier.  (At the time of writing, this was the only barrier type implemented in Vault 0.10.)").
			Short('p').PlaceHolder("PATH").Required().String()
		masterKeyPath = app.Flag("master-key-path",
			"Local filesystem path to the Vault master key file.  The program will interactively prompt for the Vault master key if this flag is not supplied.  The Vault master key must be supplied as a base64 encoded string; vault-construct-master-key will output the Vault master key in this format.").
			PlaceHolder("PATH").ExistingFile()

		listCmd    = app.Command("list", "List keys.")
		listPrefix = listCmd.Arg("prefix", "").Required().String()

		getCmd        = app.Command("get", "Read key data.  Data will be written to standard output as a formatted hexdump.")
		getKey        = getCmd.Arg("key", "").Required().String()
		getDecompress = getCmd.Flag("decompress", "Attempt to decompress data prior to output.  Enabled by default; use --no-decompress to disable.").Default("true").Bool()
		getVerbatim   = getCmd.Flag("verbatim", "Omit hexdump; write data byte-for-byte to standard output.").Bool()
	)

	cmd := kingpin.MustParse(app.Parse(os.Args[1:]))

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
		if err := barrier.Unseal(masterKey); err != nil {
			app.Fatalf("%v", err)
		}
	}

	switch cmd {
	case listCmd.FullCommand():
		if err := list(barrier, *listPrefix); err != nil {
			app.Fatalf("%v", err)
		}

	case getCmd.FullCommand():
		if err := get(barrier, *getKey, *getDecompress, *getVerbatim); err != nil {
			app.Fatalf("%v", err)
		}
	}
}

func list(barrier *vault.AESGCMBarrier, prefix string) error {
	keys, err := barrier.List(prefix)
	if err != nil {
		return err
	}
	for i := range keys {
		fmt.Println(keys[i])
	}
	return nil
}

func get(barrier *vault.AESGCMBarrier, key string, decompress, verbatim bool) error {
	entry, err := barrier.Get(key)
	if err != nil {
		return err
	}
	if entry == nil {
		return fmt.Errorf("no value at %s", key)
	}

	value := entry.Value
	if decompress {
		dc, notC, cErr := compressutil.Decompress(value)
		if cErr != nil {
			return cErr
		}
		if !notC {
			value = dc
		}
	}

	if verbatim {
		os.Stdout.Write(value)
	} else {
		fmt.Print(hex.Dump(value))
	}
	return nil
}

func openBarrier(backendPath string) (*vault.AESGCMBarrier, error) {
	logger := logxi.New("vault")

	conf := make(map[string]string)
	conf["path"] = backendPath

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
