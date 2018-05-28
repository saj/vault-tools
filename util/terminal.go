package util

import (
	"errors"
	"os"

	"golang.org/x/crypto/ssh/terminal"
)

type Terminal struct {
	file      *os.File
	prevState *terminal.State
	term      *terminal.Terminal
}

func NewTerminal() (*Terminal, error) {
	f, err := findTerminal()
	if err != nil {
		return nil, err
	}
	prevState, err := terminal.MakeRaw(int(f.Fd()))
	if err != nil {
		return nil, err
	}
	return &Terminal{
		file:      f,
		prevState: prevState,
		term:      terminal.NewTerminal(f, ""),
	}, nil
}

func (t *Terminal) Restore() error {
	t.term = nil
	return terminal.Restore(int(t.file.Fd()), t.prevState)
}

func (t *Terminal) Write(buf []byte) (n int, err error) {
	if t.term == nil {
		return 0, errors.New("terminal not initialised")
	}

	return t.term.Write(buf)
}

func (t *Terminal) ReadKeyBase64(prompt string) ([]byte, error) {
	if t.term == nil {
		return nil, errors.New("terminal not initialised")
	}

	line, err := t.term.ReadPassword(prompt)
	if err != nil {
		return nil, err
	}

	return DecodeKeyBase64String(line)
}

func findTerminal() (*os.File, error) {
	for _, f := range []*os.File{os.Stdin, os.Stdout, os.Stderr} {
		if terminal.IsTerminal(int(f.Fd())) {
			return f, nil
		}
	}
	return nil, errors.New("no terminal attached to standard streams")
}
