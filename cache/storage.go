// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"fmt"
	"sync"

	"github.com/BeDreamCoder/wal/log"
	"github.com/BeDreamCoder/wal/snap/snappb"
)

// ErrCompacted is returned by Storage.Entries/Compact when a requested
// index is unavailable because it predates the last snapshot.
var ErrCompacted = errors.New("requested index is unavailable due to compaction")

// ErrSnapOutOfDate is returned by Storage.CreateSnapshot when a requested
// index is older than the existing snapshot.
var ErrSnapOutOfDate = errors.New("requested index is older than the existing snapshot")

// ErrUnavailable is returned by Storage interface when the requested log entries
// are unavailable.
var ErrUnavailable = errors.New("requested entry at index is unavailable")

// ErrSnapshotTemporarilyUnavailable is returned by the Storage interface when the required
// snapshot is temporarily unavailable.
var ErrSnapshotTemporarilyUnavailable = errors.New("snapshot is temporarily unavailable")

// Storage is an interface that may be implemented by the application
// to retrieve log entries from storage.
//
// If any Storage method returns an error, the raft instance will
// become inoperable and refuse to participate in elections; the
// application is responsible for cleanup and recovery in this case.
type MemoryStorage interface {
	// InitialState returns the saved HardState and ConfState information.
	HardState() (log.HardState, error)
	// Entries returns a slice of log entries in the range [lo,hi).
	// MaxSize limits the total size of the log entries returned, but
	// Entries returns at least one entry if any.
	Entries(lo, hi, maxSize uint64) ([]log.LogEntry, error)
	// Snapshot returns the most recent snapshot.
	// If snapshot is temporarily unavailable, it should return ErrSnapshotTemporarilyUnavailable,
	// so raft state machine could know that Storage needs some time to prepare
	// snapshot and call Snapshot later.
	Snapshot() (snappb.ShotData, error)
	// LastIndex returns the index of the last entry in the log.
	LastIndex() (uint64, error)
	// FirstIndex returns the index of the first log entry that is
	// possibly available via Entries (older entries have been incorporated
	// into the latest Snapshot; if storage only contains the dummy entry the
	// first log entry is not available).
	FirstIndex() (uint64, error)
	// ApplySnapshot overwrites the contents of this Storage object with
	// those of the given snapshot.
	ApplySnapshot(snap snappb.ShotData, ents []log.LogEntry) error
	// CreateSnapshot makes a snapshot which can be retrieved with Snapshot() and
	// can be used to reconstruct the state at that point.
	// If any configuration changes have been made since the last compaction,
	// the result of the last ApplyConfChange must be passed in.
	CreateSnapshot(snap snappb.ShotData) (snappb.ShotData, error)
	// Compact discards all log entries prior to compactIndex.
	// It is the application's responsibility to not attempt to compact an index
	// greater than raftLog.applied.
	Compact(compactIndex uint64) error
	// Append the new entries to storage.
	Append(entries []log.LogEntry) error
}

// memoryStorage implements the Storage interface backed by an
// in-memory array.
type memoryStorage struct {
	// Protects access to all fields. Most methods of memoryStorage are
	// run on the raft goroutine, but Append() is run on an application
	// goroutine.
	sync.Mutex

	hardState log.HardState
	snapshot  snappb.ShotData
	// ents[i] has log position i+snapshot.Metadata.Index
	ents []log.LogEntry
}

// NewMemoryStorage creates an empty memoryStorage.
func NewMemoryStorage() MemoryStorage {
	return &memoryStorage{
		// When starting from scratch populate the list with a dummy entry at term zero.
		ents: make([]log.LogEntry, 1),
	}
}

// InitialState implements the Storage interface.
func (ms *memoryStorage) HardState() (log.HardState, error) {
	return ms.hardState, nil
}

// SetHardState saves the current HardState.
func (ms *memoryStorage) SetHardState(st log.HardState) error {
	ms.Lock()
	defer ms.Unlock()
	ms.hardState = st
	return nil
}

// Entries implements the Storage interface.
func (ms *memoryStorage) Entries(lo, hi, maxSize uint64) ([]log.LogEntry, error) {
	ms.Lock()
	defer ms.Unlock()
	offset := ms.ents[0].GetIndex()
	if lo <= offset {
		return nil, ErrCompacted
	}
	if hi > ms.lastIndex()+1 {
		panic(fmt.Sprintf("entries' hi(%d) is out of bound lastindex(%d)", hi, ms.lastIndex()))
	}
	// only contains dummy entries.
	if len(ms.ents) == 1 {
		return nil, ErrUnavailable
	}

	ents := ms.ents[lo-offset : hi-offset]
	return limitSize(ents, maxSize), nil
}

// LastIndex implements the Storage interface.
func (ms *memoryStorage) LastIndex() (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.lastIndex(), nil
}

func (ms *memoryStorage) lastIndex() uint64 {
	return ms.ents[0].GetIndex() + uint64(len(ms.ents)) - 1
}

// FirstIndex implements the Storage interface.
func (ms *memoryStorage) FirstIndex() (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.firstIndex(), nil
}

func (ms *memoryStorage) firstIndex() uint64 {
	return ms.ents[0].GetIndex() + 1
}

// Snapshot implements the Storage interface.
func (ms *memoryStorage) Snapshot() (snappb.ShotData, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.snapshot, nil
}

// ApplySnapshot overwrites the contents of this Storage object with
// those of the given snapshot.
func (ms *memoryStorage) ApplySnapshot(snap snappb.ShotData, ents []log.LogEntry) error {
	ms.Lock()
	defer ms.Unlock()

	//handle check for old snapshot being applied
	msIndex := ms.snapshot.Index
	snapIndex := snap.Index
	if msIndex >= snapIndex {
		return ErrSnapOutOfDate
	}

	ms.snapshot = snap
	ms.ents = ents
	return nil
}

// CreateSnapshot makes a snapshot which can be retrieved with Snapshot() and
// can be used to reconstruct the state at that point.
// If any configuration changes have been made since the last compaction,
// the result of the last ApplyConfChange must be passed in.
func (ms *memoryStorage) CreateSnapshot(snap snappb.ShotData) (snappb.ShotData, error) {
	ms.Lock()
	defer ms.Unlock()
	if snap.Index <= ms.snapshot.Index {
		return snappb.ShotData{}, ErrSnapOutOfDate
	}

	ms.snapshot = snap
	return ms.snapshot, nil
}

// Compact discards all log entries prior to compactIndex.
// It is the application's responsibility to not attempt to compact an index
// greater than raftLog.applied.
func (ms *memoryStorage) Compact(compactIndex uint64) error {
	ms.Lock()
	defer ms.Unlock()
	offset := ms.ents[0].GetIndex()
	if compactIndex <= offset {
		return ErrCompacted
	}
	if compactIndex > ms.lastIndex() {
		panic(fmt.Sprintf("compact %d is out of bound lastindex(%d)", compactIndex, ms.lastIndex()))
	}

	i := compactIndex - offset
	ents := make([]log.LogEntry, 1, 1+uint64(len(ms.ents))-i)
	ents[0] = ms.ents[i]
	ents = append(ents, ms.ents[i+1:]...)
	ms.ents = ents
	return nil
}

// Append the new entries to storage.
// TODO (xiangli): ensure the entries are continuous and
// entries[0].Index > ms.entries[0].Index
func (ms *memoryStorage) Append(entries []log.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	ms.Lock()
	defer ms.Unlock()

	first := ms.firstIndex()
	last := entries[0].GetIndex() + uint64(len(entries)) - 1

	// shortcut if there is no new entry.
	if last < first {
		return nil
	}
	// truncate compacted entries
	if first > entries[0].GetIndex() {
		entries = entries[first-entries[0].GetIndex():]
	}

	offset := entries[0].GetIndex() - ms.ents[0].GetIndex()
	switch {
	case uint64(len(ms.ents)) > offset:
		ms.ents = append([]log.LogEntry{}, ms.ents[:offset]...)
		ms.ents = append(ms.ents, entries...)
	case uint64(len(ms.ents)) == offset:
		ms.ents = append(ms.ents, entries...)
	default:
		panic(fmt.Sprintf("missing log entry [last: %d, append at: %d]",
			ms.lastIndex(), entries[0].GetIndex()))
	}
	return nil
}

func limitSize(ents []log.LogEntry, maxSize uint64) []log.LogEntry {
	if len(ents) == 0 {
		return ents
	}
	size := ents[0].Size()
	var limit int
	for limit = 1; limit < len(ents); limit++ {
		size += ents[limit].Size()
		if uint64(size) > maxSize {
			break
		}
	}
	return ents[:limit]
}
