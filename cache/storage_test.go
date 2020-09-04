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
	"math"
	"reflect"
	"testing"

	"github.com/BeDreamCoder/wal/log"
	"github.com/BeDreamCoder/wal/log/walpb"
)

func TestStorageEntries(t *testing.T) {
	ents := []log.LogEntry{&walpb.Entry{Index: 3}, &walpb.Entry{Index: 4}, &walpb.Entry{Index: 5}, &walpb.Entry{Index: 6}}
	tests := []struct {
		lo, hi, maxsize uint64

		werr     error
		wentries []log.LogEntry
	}{
		{2, 6, math.MaxUint64, ErrCompacted, nil},
		{3, 4, math.MaxUint64, ErrCompacted, nil},
		{4, 5, math.MaxUint64, nil, []log.LogEntry{&walpb.Entry{Index: 4}}},
		{4, 6, math.MaxUint64, nil, []log.LogEntry{&walpb.Entry{Index: 4}, &walpb.Entry{Index: 5}}},
		{4, 7, math.MaxUint64, nil, []log.LogEntry{&walpb.Entry{Index: 4}, &walpb.Entry{Index: 5}, &walpb.Entry{Index: 6}}},
		// even if maxsize is zero, the first entry should be returned
		{4, 7, 0, nil, []log.LogEntry{&walpb.Entry{Index: 4}}},
		// limit to 2
		{4, 7, uint64(ents[1].Size() + ents[2].Size()), nil, []log.LogEntry{&walpb.Entry{Index: 4}, &walpb.Entry{Index: 5}}},
		// limit to 2
		{4, 7, uint64(ents[1].Size() + ents[2].Size() + ents[3].Size()/2), nil, []log.LogEntry{&walpb.Entry{Index: 4}, &walpb.Entry{Index: 5}}},
		{4, 7, uint64(ents[1].Size() + ents[2].Size() + ents[3].Size() - 1), nil, []log.LogEntry{&walpb.Entry{Index: 4}, &walpb.Entry{Index: 5}}},
		// all
		{4, 7, uint64(ents[1].Size() + ents[2].Size() + ents[3].Size()), nil, []log.LogEntry{&walpb.Entry{Index: 4}, &walpb.Entry{Index: 5}, &walpb.Entry{Index: 6}}},
	}

	for i, tt := range tests {
		s := &memoryStorage{ents: ents}
		entries, err := s.Entries(tt.lo, tt.hi, tt.maxsize)
		if err != tt.werr {
			t.Errorf("#%d: err = %v, want %v", i, err, tt.werr)
		}
		if !reflect.DeepEqual(entries, tt.wentries) {
			t.Errorf("#%d: entries = %v, want %v", i, entries, tt.wentries)
		}
	}
}

func TestStorageLastIndex(t *testing.T) {
	ents := []log.LogEntry{&walpb.Entry{Index: 3}, &walpb.Entry{Index: 4}, &walpb.Entry{Index: 5}}
	s := &memoryStorage{ents: ents}

	last, err := s.LastIndex()
	if err != nil {
		t.Errorf("err = %v, want nil", err)
	}
	if last != 5 {
		t.Errorf("last = %d, want %d", last, 5)
	}

	s.Append([]log.LogEntry{&walpb.Entry{Index: 6}})
	last, err = s.LastIndex()
	if err != nil {
		t.Errorf("err = %v, want nil", err)
	}
	if last != 6 {
		t.Errorf("last = %d, want %d", last, 6)
	}
}

func TestStorageFirstIndex(t *testing.T) {
	ents := []log.LogEntry{&walpb.Entry{Index: 3}, &walpb.Entry{Index: 4}, &walpb.Entry{Index: 5}}
	s := &memoryStorage{ents: ents}

	first, err := s.FirstIndex()
	if err != nil {
		t.Errorf("err = %v, want nil", err)
	}
	if first != 4 {
		t.Errorf("first = %d, want %d", first, 4)
	}

	s.Compact(4)
	first, err = s.FirstIndex()
	if err != nil {
		t.Errorf("err = %v, want nil", err)
	}
	if first != 5 {
		t.Errorf("first = %d, want %d", first, 5)
	}
}

func TestStorageCompact(t *testing.T) {
	ents := []log.LogEntry{&walpb.Entry{Index: 3}, &walpb.Entry{Index: 4}, &walpb.Entry{Index: 5}}
	tests := []struct {
		i uint64

		werr   error
		windex uint64
		wterm  uint64
		wlen   int
	}{
		{2, ErrCompacted, 3, 3, 3},
		{3, ErrCompacted, 3, 3, 3},
		{4, nil, 4, 4, 2},
		{5, nil, 5, 5, 1},
	}

	for i, tt := range tests {
		s := &memoryStorage{ents: ents}
		err := s.Compact(tt.i)
		if err != tt.werr {
			t.Errorf("#%d: err = %v, want %v", i, err, tt.werr)
		}
		if s.ents[0].GetIndex() != tt.windex {
			t.Errorf("#%d: index = %d, want %d", i, s.ents[0].GetIndex(), tt.windex)
		}
		if s.ents[0].GetIndex() != tt.wterm {
			t.Errorf("#%d: term = %d, want %d", i, s.ents[0].GetIndex(), tt.wterm)
		}
		if len(s.ents) != tt.wlen {
			t.Errorf("#%d: len = %d, want %d", i, len(s.ents), tt.wlen)
		}
	}
}

func TestStorageAppend(t *testing.T) {
	ents := []log.LogEntry{&walpb.Entry{Index: 3}, &walpb.Entry{Index: 4}, &walpb.Entry{Index: 5}}
	tests := []struct {
		entries []log.LogEntry

		werr     error
		wentries []log.LogEntry
	}{
		{
			[]log.LogEntry{&walpb.Entry{Index: 1}, &walpb.Entry{Index: 2}},
			nil,
			[]log.LogEntry{&walpb.Entry{Index: 3}, &walpb.Entry{Index: 4}, &walpb.Entry{Index: 5}},
		},
		{
			[]log.LogEntry{&walpb.Entry{Index: 3}, &walpb.Entry{Index: 4}, &walpb.Entry{Index: 5}},
			nil,
			[]log.LogEntry{&walpb.Entry{Index: 3}, &walpb.Entry{Index: 4}, &walpb.Entry{Index: 5}},
		},
		{
			[]log.LogEntry{&walpb.Entry{Index: 3}, &walpb.Entry{Index: 4}, &walpb.Entry{Index: 5}},
			nil,
			[]log.LogEntry{&walpb.Entry{Index: 3}, &walpb.Entry{Index: 4}, &walpb.Entry{Index: 5}},
		},
		{
			[]log.LogEntry{&walpb.Entry{Index: 3}, &walpb.Entry{Index: 4}, &walpb.Entry{Index: 5}, &walpb.Entry{Index: 6}},
			nil,
			[]log.LogEntry{&walpb.Entry{Index: 3}, &walpb.Entry{Index: 4}, &walpb.Entry{Index: 5}, &walpb.Entry{Index: 6}},
		},
		// truncate incoming entries, truncate the existing entries and append
		{
			[]log.LogEntry{&walpb.Entry{Index: 2}, &walpb.Entry{Index: 3}, &walpb.Entry{Index: 4}},
			nil,
			[]log.LogEntry{&walpb.Entry{Index: 3}, &walpb.Entry{Index: 4}},
		},
		// truncate the existing entries and append
		{
			[]log.LogEntry{&walpb.Entry{Index: 4}},
			nil,
			[]log.LogEntry{&walpb.Entry{Index: 3}, &walpb.Entry{Index: 4}},
		},
		// direct append
		{
			[]log.LogEntry{&walpb.Entry{Index: 6}},
			nil,
			[]log.LogEntry{&walpb.Entry{Index: 3}, &walpb.Entry{Index: 4}, &walpb.Entry{Index: 5}, &walpb.Entry{Index: 6}},
		},
	}

	for i, tt := range tests {
		s := &memoryStorage{ents: ents}
		err := s.Append(tt.entries)
		if err != tt.werr {
			t.Errorf("#%d: err = %v, want %v", i, err, tt.werr)
		}
		if !reflect.DeepEqual(s.ents, tt.wentries) {
			t.Errorf("#%d: entries = %v, want %v", i, s.ents, tt.wentries)
		}
	}
}
