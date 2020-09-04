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
	"github.com/BeDreamCoder/wal/snap"
	"github.com/BeDreamCoder/wal/snap/snappb"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

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

func init() {
	log.RegisterRecord(log.EntryType, log.LogEntry(&CustomEntry{}))
}

func TestWAL_SaveEntry(t *testing.T) {
	p, err := ioutil.TempDir(os.TempDir(), "waltest")
	assert.NoError(t, err)
	s, err := ioutil.TempDir(os.TempDir(), "snaptest")
	assert.NoError(t, err)
	defer func() {
		os.RemoveAll(p)
		os.RemoveAll(s)
	}()

	lz := zap.NewExample()
	w, err := log.Create(lz, p, []byte("metadata"))
	assert.NoError(t, err)

	storage := NewStorage(w, snap.New(lz, s))

	ents := []log.LogEntry{
		&CustomEntry{1, "a"},
		&CustomEntry{2, "b"},
		&CustomEntry{3, "c"},
		&CustomEntry{4, "d"},
	}

	err = storage.SaveEntry(ents)
	assert.NoError(t, err)

	storage.Close()

	w, err = log.Open(zap.NewExample(), p, &walpb.Snapshot{})
	assert.NoError(t, err)

	storage2 := NewStorage(w, snap.New(lz, s))

	_, _, entrys, err := w.ReadAll()
	assert.NoError(t, err)

	defer storage2.Close()

	for _, v := range entrys {
		rec, _ := v.(*CustomEntry)
		t.Logf("read custom record: %s", rec.Value)
	}

	err = storage2.Release(snappb.ShotData{Index: 3}, &walpb.Snapshot{Index: 3})
	assert.NoError(t, err)
}
