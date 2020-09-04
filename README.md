# WAL
Write-Ahead Logging based on etcd-wal

## Storage API
```go
type Storage interface {
	// Save function saves ents and state to the underlying stable storage.
	// Save MUST block until st and ents are on stable storage.
	Save(st log.HardState, ents []log.LogEntry) error
	// SaveState function saves state to the underlying stable storage.
	SaveState(st log.HardState) error
	// SaveState function saves ents to the underlying stable storage.
	SaveEntry(ents []log.LogEntry) error
	// SaveSnap function saves snapshot to the underlying stable storage.
	SaveSnap(snap log.Snapshot) error
	// Close closes the Storage and performs finalization.
	Close() error
	// Release releases the locked wal files older than the provided snapshot.
	Release(snap log.Snapshot) error
	// Sync WAL
	Sync() error
}
```

## Record Type
```go
const (
	MetadataType RecordType = iota + 1
	EntryType
	StateType
	CrcType
	SnapshotType
)
```

### Record API
```go
// LogEntry implement custom entry data struct with entry index
type LogEntry interface {
	RecordData
	// index of the entry saved to the wal
	GetIndex() uint64
	// size of entry alloc memory
	Size() (n int)
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

type RecordData interface {
	Marshal() (data []byte, err error)
	Unmarshal(data []byte) error
}
```

### Define Custom Record
```go
type CustomEntry struct {
	Index uint64
	Value string
}

func (m *CustomEntry) Marshal() (data []byte, err error) {
	return json.Marshal(m)
}

func (m *CustomEntry) Unmarshal(data []byte) error {
	return json.Unmarshal(data, m)
}
func (m *CustomEntry) GetIndex() uint64 {
	return m.Index
}

log.RegisterRecord(log.EntryType, log.LogEntry(&CustomEntry{}))
```
