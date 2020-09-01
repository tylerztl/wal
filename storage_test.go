/*
Copyright Zhigui.com. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package wal

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"testing"

	"github.com/BeDreamCoder/wal/log"
	"github.com/BeDreamCoder/wal/log/walpb"
	"github.com/stretchr/testify/assert"
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

const mockType log.RecordType = 10

func init() {
	log.RegisterRecord(mockType, &MockRecord{})
}

func TestWAL_SaveRecords(t *testing.T) {
	p, err := ioutil.TempDir(os.TempDir(), "waltest")
	assert.NoError(t, err)
	defer os.RemoveAll(p)

	w, err := log.Create(zap.NewExample(), p, []byte("metadata"))
	assert.NoError(t, err)

	storage := NewStorage(w)

	records := []log.CustomRecord{
		&MockRecord{0, "a"},
		&MockRecord{1, "b"},
		&MockRecord{2, "c"},
		&MockRecord{3, "d"},
	}

	for _, v := range records {
		err = storage.SaveRecords(mockType, []log.CustomRecord{v})
		assert.NoError(t, err)
	}

	storage.Close()

	w, err = log.Open(zap.NewExample(), p, walpb.Snapshot{})
	assert.NoError(t, err)

	storage2 := NewStorage(w)

	_, _, _, record, err := w.ReadAll()
	assert.NoError(t, err)

	defer storage2.Close()

	for _, v := range record {
		rec, _ := v.(MockRecord)
		t.Logf("read custom record: %s", rec.Value)
	}

	err = storage2.Release(walpb.Snapshot{Index: 3})
	assert.NoError(t, err)
}
