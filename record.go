package wal

import (
	"go.etcd.io/etcd/pkg/pbutil"
)

func mustUnmarshalEntry(d []byte) Entry {
	var e Entry
	pbutil.MustUnmarshal(&e, d)
	return e
}

func mustUnmarshalState(d []byte) HardState {
	var s HardState
	pbutil.MustUnmarshal(&s, d)
	return s
}

var emptyState = HardState{}

func isHardStateEqual(a, b HardState) bool {
	return a.Term == b.Term && a.Vote == b.Vote && a.Commit == b.Commit
}

// IsEmptyHardState returns true if the given HardState is empty.
func IsEmptyHardState(st HardState) bool {
	return isHardStateEqual(st, emptyState)
}

// MustSync returns true if the hard state and count of Raft entries indicate
// that a synchronous write to persistent storage is required.
func MustSync(st, prevst HardState, entsnum int) bool {
	// Persistent state on all servers:
	// (Updated on stable storage before responding to RPCs)
	// currentTerm
	// votedFor
	// log entries[]
	return entsnum != 0 || st.Vote != prevst.Vote || st.Term != prevst.Term
}
