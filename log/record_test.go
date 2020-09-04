/*
Copyright Zhigui.com. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package log

import (
	"testing"

	"github.com/BeDreamCoder/wal/log/walpb"
	"github.com/stretchr/testify/assert"
)

func TestNewEmptyEntry(t *testing.T) {
	e := NewEmptyEntry()
	assert.EqualValues(t, &walpb.Entry{}, e)
}

func BenchmarkNewEmptyEntry(b *testing.B) {
	b.ReportAllocs()

	e := &walpb.Entry{}
	b.SetBytes(int64(e.Size()))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		NewEmptyEntry()
	}
}

func BenchmarkEmptyEntry(b *testing.B) {
	b.ReportAllocs()

	e := &walpb.Entry{}
	b.SetBytes(int64(e.Size()))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = &walpb.Entry{}
	}
}
