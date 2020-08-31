# wal
Write ahead log based on etcd-wal

## Storage API
```go
type Storage interface {
	// Save function saves ents and state to the underlying stable storage.
	// Save MUST block until st and ents are on stable storage.
	Save(st HardState, ents []Entry) error
	// SaveSnap function saves snapshot to the underlying stable storage.
	SaveSnap(snap Snapshot) error
	// SaveRecords function saves custom records to the underlying stable storage.
	SaveRecords(rt RecordType, crs []CustomRecord) error
	// Close closes the Storage and performs finalization.
	Close() error
	// Release releases the locked wal files older than the provided snapshot.
	Release(snap Snapshot) error
	// Sync WAL
	Sync() error
}
```

## Define custom record
### record example
```go
type MockRecord struct {
	Index uint64
	Value string
}

func (m *MockRecord) Marshal() ([]byte, error) {
	return json.Marshal(m)
}

func (m *MockRecord) Unmarshal(data []byte) (interface{}, error) {
	var e MockRecord
	err := json.Unmarshal(data, &e)
	return e, err
}
func (m *MockRecord) RecordIndex() uint64 {
	return m.Index
}
```

### register custom record
```go
const mockType RecordType = 10

RegisterRecord(mockType, &MockRecord{})
```
