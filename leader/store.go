package leader

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
)

// A Store keeps a node's part in an election across restarts of its
// elector, and of the node: the term, its vote in it, and the singleton's
// state. An elector saves to it before anything that tells of them leaves
// it, as Raft does, so with a Store on every node a cluster that loses a
// majority of its nodes at once, or all of them, carries on from where it
// was. Each node needs its own, on its own disk: a Store that nodes share
// breaks the election, and so does one restored from a backup.
type Store interface {
	// Load returns what Save saved last, or nothing if it never did.
	Load() ([]byte, error)
	// Save replaces what is saved with b, and returns once that would
	// outlast a crash of the machine.
	Save(b []byte) error
}

// File is a Store in the file at path. Save writes path+".tmp", syncs it
// to disk and renames it over path, so a crash leaves the old state or the
// new one, and at most the one temporary file beside it, which the next
// Save overwrites.
func File(path string) Store { return file(path) }

type file string

func (f file) Load() ([]byte, error) {
	b, err := os.ReadFile(string(f))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	return b, err
}

func (f file) Save(b []byte) error {
	dir := filepath.Dir(string(f))
	tmp, err := os.OpenFile(string(f)+".tmp", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	_, err = tmp.Write(b)
	if err = errors.Join(err, tmp.Sync(), tmp.Close()); err == nil {
		err = os.Rename(tmp.Name(), string(f))
	}
	if err != nil {
		_ = os.Remove(tmp.Name())
		return err
	}
	// The rename lasts once the directory is synced too.
	d, err := os.Open(dir)
	if err == nil {
		err = errors.Join(d.Sync(), d.Close())
	}
	return err
}
