package util

import (
	"encoding/base64"
	"strings"
)

func DecodeKeyBase64Byte(key []byte) ([]byte, error) {
	return DecodeKeyBase64String(string(key))
}

func DecodeKeyBase64String(key string) ([]byte, error) {
	return decodeKeyBase64([]byte(strings.TrimSpace(key)))
}

func decodeKeyBase64(key []byte) ([]byte, error) {
	length := base64.StdEncoding.DecodedLen(len(key))
	decoded := make([]byte, length)
	n, err := base64.StdEncoding.Decode(decoded, key)
	if err != nil {
		return nil, err
	}
	return decoded[0:n], nil
}
