package main

import (
	"encoding/base64"
	"fmt"
	"os"

	"github.com/hashicorp/vault/shamir"
	kingpin "gopkg.in/alecthomas/kingpin.v2"

	"github.com/saj/vault-tools/internal/util"
)

func main() {
	app := kingpin.New("vault-construct-master-key",
		"Construct a Vault master key from a set of Shamir key shares.").
		UsageTemplate(kingpin.CompactUsageTemplate)
	numShares := app.Flag("num-shares",
		"Number of Shamir key shares required by the Vault seal configuration.").
		Default("3").Uint()

	die := func(err error) {
		app.Fatalf("%v", err)
	}

	kingpin.MustParse(app.Parse(os.Args[1:]))

	keyShares, err := promptForKeyShares(*numShares)
	if err != nil {
		die(err)
	}

	masterKey, err := shamir.Combine(keyShares)
	if err != nil {
		die(err)
	}
	fmt.Println(base64.StdEncoding.EncodeToString(masterKey))
}

func promptForKeyShares(numShares uint) ([][]byte, error) {
	t, err := util.NewTerminal()
	if err != nil {
		return nil, err
	}
	defer t.Restore() // nolint: errcheck

	keyShares := make([][]byte, numShares)
	for i := 0; i < int(numShares); i++ {
		k, err := t.ReadKeyBase64(fmt.Sprintf("Enter key share %d of %d: ", i+1, numShares))
		if err != nil {
			return nil, err
		}
		keyShares[i] = k
	}
	return keyShares, nil
}
