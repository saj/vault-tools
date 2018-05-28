package main

import (
	"encoding/hex"
	"fmt"
	"os"

	"github.com/hashicorp/vault/helper/compressutil"
	"github.com/hashicorp/vault/physical/file"
	"github.com/hashicorp/vault/vault"
	logxi "github.com/mgutz/logxi/v1"
	kingpin "gopkg.in/alecthomas/kingpin.v2"

	"github.com/saj/vault-tools/util"
)

func main() {
	var (
		app = kingpin.New("vault-backend-file",
			"Provides unfettered read access to a Vault file backend secured with an AES-GCM barrier.").
			UsageTemplate(kingpin.CompactUsageTemplate)
		path          = app.Flag("path", "Local filesystem path to the Vault file backend root directory.").Required().String()
		list          = app.Command("list", "List keys.")
		listPrefix    = list.Arg("prefix", "").Required().String()
		get           = app.Command("get", "Read key data.  Data will be written to standard output as a formatted hexdump.")
		getKey        = get.Arg("key", "").Required().String()
		getDecompress = get.Flag("decompress", "Attempt to decompress data prior to output.").Bool()
		getVerbatim   = get.Flag("verbatim", "Omit hexdump; write data byte-for-byte to standard output.").Bool()
	)

	cmd := kingpin.MustParse(app.Parse(os.Args[1:]))

	masterKey, err := promptForMasterKey()
	if err != nil {
		app.Fatalf("%v", err)
	}
	barrier, err := newBarrier(*path)
	if err != nil {
		app.Fatalf("%v", err)
	}
	if err := barrier.Unseal(masterKey); err != nil {
		app.Fatalf("%v", err)
	}

	switch cmd {
	case list.FullCommand():
		keys, err := barrier.List(*listPrefix)
		if err != nil {
			app.Fatalf("%v", err)
		}
		for i := range keys {
			fmt.Println(keys[i])
		}

	case get.FullCommand():
		entry, err := barrier.Get(*getKey)
		if err != nil {
			app.Fatalf("%v", err)
		}
		if entry == nil {
			app.Fatalf("no value at %s", *getKey)
		}
		d := entry.Value
		if *getDecompress {
			decompressed, notComp, compErr := compressutil.Decompress(d)
			if err != nil {
				app.Fatalf("%v", compErr)
			}
			if !notComp {
				d = decompressed
			}
		}
		if *getVerbatim {
			fmt.Println(string(d))
		} else {
			fmt.Print(hex.Dump(d))
		}
	}
}

func newBarrier(backendPath string) (*vault.AESGCMBarrier, error) {
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
