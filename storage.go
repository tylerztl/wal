package wal

import (
	"io"

	"github.com/BeDreamCoder/wal/log"
	"github.com/BeDreamCoder/wal/log/walpb"
	"go.uber.org/zap"
)

type Storage interface {
	// Save function saves ents and state to the underlying stable storage.
	// Save MUST block until st and ents are on stable storage.
	Save(st walpb.HardState, ents []walpb.Entry) error
	// SaveSnap function saves snapshot to the underlying stable storage.
	SaveSnap(snap walpb.Snapshot) error
	// SaveRecords function saves custom records to the underlying stable storage.
	SaveRecords(rt log.RecordType, crs []log.CustomRecord) error
	// Close closes the Storage and performs finalization.
	Close() error
	// Release releases the locked wal files older than the provided snapshot.
	Release(snap walpb.Snapshot) error
	// Sync WAL
	Sync() error
}

type storage struct {
	*log.WAL
}

func NewStorage(w *log.WAL) Storage {
	return &storage{w}
}

// SaveSnap saves the snapshot file to disk and writes the WAL snapshot entry.
func (st *storage) SaveSnap(snap walpb.Snapshot) error {
	return st.WAL.SaveSnapshot(snap)
}

// Release releases resources older than the given snap and are no longer needed:
// - releases the locks to the wal files that are older than the provided wal for the given snap.
// - deletes any .snap.db files that are older than the given snap.
func (st *storage) Release(snap walpb.Snapshot) error {
	return st.WAL.ReleaseLockTo(snap.Index)
}

// ReadWAL reads the WAL at the given snap and returns the wal, its latest HardState and all entries that appear
// after the position of the given snap in the WAL.
// The snap must have been previously saved to the WAL, or this call will panic.
func ReadWAL(lg *zap.Logger, waldir string, snap walpb.Snapshot, unsafeNoFsync bool) (w *log.WAL,
	wmetadata []byte, st walpb.HardState, ents []walpb.Entry, records []interface{}) {
	var err error

	repaired := false
	for {
		if w, err = log.Open(lg, waldir, snap); err != nil {
			lg.Fatal("failed to open WAL", zap.Error(err))
		}
		if unsafeNoFsync {
			w.SetUnsafeNoFsync()
		}
		if wmetadata, st, ents, records, err = w.ReadAll(); err != nil {
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
	return w, wmetadata, st, ents, records
}
