// Package main implements a distributed key-value store.
// It uses the Raft consensus algorithm for data replication and fault tolerance.
// This file contains the implementation of the finite state machine (FSM)
// that is used by the Raft library to manage the key-value store.
package main

import (
	"encoding/json"
	"io"

	"github.com/hashicorp/raft"
)

// fsm is a finite state machine that manages the key-value store.
// It implements the raft.FSM interface, which allows it to be used with the Raft library.
type fsm struct {
	store *Store
}

// command represents a command that can be applied to the key-value store.
type command struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Apply applies a Raft log entry to the key-value store.
// It is called by the Raft library when a log entry is committed.
func (f *fsm) Apply(log *raft.Log) interface{} {
	var c command
	if err := json.Unmarshal(log.Data, &c); err != nil {
		panic("failed to unmarshal command")
	}

	switch c.Op {
	case "put":
		f.store.Put(c.Key, c.Value)
		return nil
	default:
		panic("unrecognized command op")
	}
}

// Snapshot returns a snapshot of the key-value store.
// It is called by the Raft library when it needs to create a snapshot.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	return &fsmSnapshot{store: f.store}, nil
}

// Restore restores the key-value store from a snapshot.
// It is called by the Raft library when it needs to restore the FSM from a snapshot.
func (f *fsm) Restore(rc io.ReadCloser) error {
	s := make(map[string]string)
	if err := json.NewDecoder(rc).Decode(&s); err != nil {
		return err
	}

	f.store.data = s
	return nil
}

// fsmSnapshot is a snapshot of the key-value store.
// It implements the raft.FSMSnapshot interface, which allows it to be used with the Raft library.
type fsmSnapshot struct {
	store *Store
}

// Persist writes the snapshot to a sink.
// It is called by the Raft library when it needs to persist a snapshot.
func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		// Encode data.
		b, err := json.Marshal(f.store.data)
		if err != nil {
			return err
		}

		// Write data to sink.
		if _, err := sink.Write(b); err != nil {
			return err
		}

		// Close the sink.
		return sink.Close()
	}()

	if err != nil {
		sink.Cancel()
	}

	return err
}

// Release is called when the snapshot is no longer needed.
func (f *fsmSnapshot) Release() {}
