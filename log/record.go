package log

import (
	"reflect"
	"sync"

	"github.com/BeDreamCoder/wal/log/walpb"
)

type RecordType int64

const (
	MetadataType RecordType = iota + 1
	EntryType
	StateType
	CrcType
	SnapshotType
)

type RecordData interface {
	Marshal() (data []byte, err error)
	Unmarshal(data []byte) error
}

// LogEntry implement custom entry data struct with entry index
type LogEntry interface {
	RecordData
	// index of the entry saved to the wal
	GetIndex() uint64
}

// HardState implement custom state data struct for save the system latest state to wal
type HardState interface {
	RecordData
	// The latest index that has been committed
	GetCommitted() uint64
	// Reset set state initial default value
	Reset()
}

// Snapshot implement custom snapshot struct with entry index
type Snapshot interface {
	RecordData
	// index of the entry saved to the wal
	GetIndex() uint64
}

var recordTypes sync.Map // map[RecordType]interface{}

func RegisterRecord(rt RecordType, ent interface{}) {
	switch ent.(type) {
	case LogEntry, HardState, Snapshot:
		recordTypes.Store(rt, ent)
	default:
		panic("invalid record struct")
	}
}

func NewEmptyEntry() (e LogEntry) {
	entry, ok := recordTypes.Load(EntryType)
	if !ok {
		panic("not register entry record type")
	}
	ent, ok := entry.(LogEntry)
	if !ok {
		panic("invalid entry record data")
	}
	if reflect.TypeOf(ent).Kind() == reflect.Ptr {
		// Pointer:
		e = reflect.New(reflect.ValueOf(ent).Elem().Type()).Interface().(LogEntry)
	} else {
		// Not pointer:
		e = reflect.New(reflect.TypeOf(ent)).Elem().Interface().(LogEntry)
	}
	return
}

func NewEmptyState() (s HardState) {
	state, ok := recordTypes.Load(StateType)
	if !ok {
		panic("not register hardstate record type")
	}
	hs, ok := state.(HardState)
	if !ok {
		panic("invalid hardstate record data")
	}
	if reflect.TypeOf(hs).Kind() == reflect.Ptr {
		// Pointer:
		s = reflect.New(reflect.ValueOf(hs).Elem().Type()).Interface().(HardState)
	} else {
		// Not pointer:
		s = reflect.New(reflect.TypeOf(hs)).Elem().Interface().(HardState)
	}
	return
}

func NewEmptySnapshot() (s Snapshot) {
	snapshot, ok := recordTypes.Load(SnapshotType)
	if !ok {
		panic("not register snapshot record type")
	}
	snap, ok := snapshot.(Snapshot)
	if !ok {
		panic("invalid snapshot record data")
	}
	if reflect.TypeOf(snap).Kind() == reflect.Ptr {
		// Pointer:
		s = reflect.New(reflect.ValueOf(snap).Elem().Type()).Interface().(Snapshot)
	} else {
		// Not pointer:
		s = reflect.New(reflect.TypeOf(snap)).Elem().Interface().(Snapshot)
	}
	return
}

func init() {
	RegisterRecord(EntryType, LogEntry(&walpb.Entry{}))
	RegisterRecord(StateType, HardState(&walpb.HardState{}))
	RegisterRecord(SnapshotType, Snapshot(&walpb.Snapshot{}))
}
