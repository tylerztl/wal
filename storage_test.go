/*
Copyright Zhigui.com. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package wal

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"testing"

	"go.uber.org/zap"
)

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

const mockType RecordType = 10

func init() {
	RegisterRecord(mockType, &MockRecord{})
}

func TestWAL_SaveRecords(t *testing.T) {
	p, err := ioutil.TempDir(os.TempDir(), "waltest")
	assert.NoError(t, err)
	//defer os.RemoveAll(p)

	w, err := Create(zap.NewExample(), p, []byte("metadata"))
	assert.NoError(t, err)

	storage := NewStorage(w)

	records := []CustomRecord{
		&MockRecord{0, "a"},
		&MockRecord{1, "b"},
		&MockRecord{2, "c"},
		&MockRecord{3, "d"},
	}

	for _, v := range records {
		err = storage.SaveRecords(mockType, []CustomRecord{v})
		assert.NoError(t, err)
		err = w.cut()
		assert.NoError(t, err)
	}

	storage.Close()

	w, err = Open(zap.NewExample(), p, Snapshot{})
	assert.NoError(t, err)

	storage2 := NewStorage(w)

	_, _, _, record, err := w.ReadAll()
	assert.NoError(t, err)

	defer storage2.Close()

	for _, v := range record {
		rec, _ := v.(MockRecord)
		t.Logf("read custom record: %s", rec.Value)
	}

	err = storage2.Release(Snapshot{Index: 3})
	assert.NoError(t, err)

	// expected remaining is 3
	if len(w.locks) != 3 {
		t.Errorf("len(w.locks) = %d, want %d", len(w.locks), 3)
	}
}
