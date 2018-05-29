package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// WalkResult is a result from a filesystem walk.  It is always a regular file.
type WalkResult struct {
	Path string
	Info os.FileInfo
}

// Walker is a stepped implementation of filepath.Walk; it returns one
// WalkResult for each call to Next.  Walker silently ignores all non-regular
// files while recursively descending into directories.
type Walker struct {
	results chan *WalkResult
	err     error

	stop       chan struct{}
	mtxStopped sync.Mutex
	stopped    bool
}

// NewWalker initialises a new Walker and starts a backgrounded filesystem walk.
// The backgrounded filesystem walk self-terminates after exhausting root.  Call
// Next to retrieve the results of the walk.
func NewWalker(root string) *Walker {
	w := &Walker{
		results: make(chan *WalkResult, 10),
		stop:    make(chan struct{}),
	}

	go func() {
		err := filepath.Walk(root, w.walkFn)
		if err != nil && err != walkStopped {
			w.err = err
		}
		close(w.results)
	}()

	return w
}

var walkStopped = fmt.Errorf("walk stopped")

func (w *Walker) walkFn(path string, info os.FileInfo, err error) error {
	if err != nil {
		return err
	}
	select {
	case <-w.stop:
		return walkStopped
	default:
	}

	if info.Mode()&os.ModeType != 0 {
		// Not a regular file.  Skip.
		return nil
	}

	select {
	case w.results <- &WalkResult{Path: path, Info: info}:
	case <-w.stop:
		return walkStopped
	}
	return nil
}

// Stop prematurely terminates the backgrounded filesystem walk.
func (w *Walker) Stop() {
	w.mtxStopped.Lock()
	defer w.mtxStopped.Unlock()
	if w.stopped {
		return
	}
	close(w.stop)
	w.stopped = true
}

// Next returns the next result from the backgrounded filesystem walk.  The
// returned WalkResult will be nil when the walk has terminated.  The walk is
// terminated when the search tree is exhausted, after Stop is called, or after
// the walk encounters an error.  The former two cases will not elicit an error.
func (w *Walker) Next() (*WalkResult, error) {
	var (
		result *WalkResult
		err    error
	)

	select {
	case r, ok := <-w.results:
		result = r
		// Let the caller flush the channel before we propagate a pending error.
		if !ok {
			err = w.err
		}
	}
	return result, err
}
