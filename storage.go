package wal

import (
	"io"

	"github.com/BeDreamCoder/wal/log"
	"github.com/BeDreamCoder/wal/snap"
	"github.com/BeDreamCoder/wal/snap/snappb"
	"go.uber.org/zap"
)

type Storage interface {
	log.WALAPI
	snap.SnapshotAPI

	SaveSnap(snap snappb.ShotData, s log.Snapshot) error
	Release(snap snappb.ShotData, s log.Snapshot) error
}

type storage struct {
	*log.WAL
	*snap.Snapshotter
}

func NewStorage(w *log.WAL, s *snap.Snapshotter) Storage {
	return &storage{w, s}
}

// SaveSnap saves the snapshot file to disk and writes the WAL snapshot entry.
func (st *storage) SaveSnap(snap snappb.ShotData, s log.Snapshot) error {
	// save the snapshot file before writing the snapshot to the wal.
	// This makes it possible for the snapshot file to become orphaned, but prevents
	// a WAL snapshot entry from having no corresponding snapshot file.
	err := st.SaveSnapData(snap)
	if err != nil {
		return err
	}

	return st.SaveSnapshot(s)
}

// Release releases resources older than the given snap and are no longer needed:
// - releases the locks to the wal files that are older than the provided wal for the given snap.
// - deletes any .snap.db files that are older than the given snap.
func (st *storage) Release(snap snappb.ShotData, s log.Snapshot) error {
	if err := st.ReleaseSnapDBs(snap); err != nil {
		return err
	}
	return st.ReleaseLockTo(s.GetIndex())
}

// ReadWAL reads the WAL at the given snap and returns the wal, its latest HardState and all entries that appear
// after the position of the given snap in the WAL.
// The snap must have been previously saved to the WAL, or this call will panic.
func ReadWAL(lg *zap.Logger, waldir string, snap log.Snapshot, unsafeNoFsync bool) (w *log.WAL,
	wmetadata []byte, st log.HardState, ents []log.LogEntry) {
	var err error

	st = log.NewEmptyState()
	repaired := false
	for {
		if w, err = log.Open(lg, waldir, snap); err != nil {
			lg.Fatal("failed to open WAL", zap.Error(err))
		}
		if unsafeNoFsync {
			w.SetUnsafeNoFsync()
		}
		if wmetadata, st, ents, err = w.ReadAll(); err != nil {
			w.Close()
			// we can only repair ErrUnexpectedEOF and we never repair twice.
			if repaired || err != io.ErrUnexpectedEOF {
				lg.Fatal("failed to read WAL, cannot be repaired", zap.Error(err))
			}
			if !log.Repair(lg, waldir) {
				lg.Fatal("failed to repair WAL", zap.Error(err))
			} else {
				lg.Info("repaired WAL", zap.Error(err))
				repaired = true
			}
			continue
		}
		break
	}
	return w, wmetadata, st, ents
}
