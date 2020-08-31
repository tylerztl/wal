package wal

import (
	"reflect"
	"sync"

	"github.com/pkg/errors"
)

var recordTypes sync.Map // map[RecordType]reflect.Type

func RegisterReocrd(rt RecordType, record CustomRecord) {
	if _, ok := recordTypes.Load(rt); ok {
		panic(errors.Errorf("[%d] record type is already registered", rt))
	}
	recordTypes.Store(rt, reflect.TypeOf(record))
}

type CustomRecord interface {
	Marshal() (data []byte, err error)
	Unmarshal(data []byte) (interface{}, error)
}

// LogStore is used to provide an interface for storing
// and retrieving logs.
type LogStore interface {
	Sync() error
	SetUnsafeNoFsync()
	ReadAll() (metadata []byte, state HardState, ents []Entry, records []interface{}, err error)
	Save(st HardState, ents []Entry) error
	SaveSnapshot(e Snapshot) error
	SaveRecords(rt RecordType, crs []CustomRecord) error
	// Close closes the current WAL file and directory.
	Close() error
}
