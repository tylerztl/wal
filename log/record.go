/*
Copyright Zhigui.com. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package log

import (
	"sync"

	"github.com/pkg/errors"
)

type CustomRecord interface {
	Marshal() (data []byte, err error)
	Unmarshal(data []byte) (interface{}, error)
	// index of the last record saved to the wal
	RecordIndex() uint64
}

var recordTypes sync.Map // map[RecordType]CustomRecord

func RegisterRecord(rt RecordType, record CustomRecord) {
	if _, ok := recordTypes.Load(rt); ok {
		panic(errors.Errorf("[%d] record type is already registered", rt))
	}
	recordTypes.Store(rt, record)
}
