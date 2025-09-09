package main

import (
	"encoding/json"
	"io"

	"github.com/hashicorp/raft"
)

type fsm struct {
	store *Store
}

type command struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Apply applies a Raft log entry to the key-value store.
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
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	return &fsmSnapshot{store: f.store}, nil
}

// Restore restores the key-value store from a snapshot.
func (f *fsm) Restore(rc io.ReadCloser) error {
	s := make(map[string]string)
	if err := json.NewDecoder(rc).Decode(&s); err != nil {
		return err
	}

	f.store.data = s
	return nil
}

type fsmSnapshot struct {
	store *Store
}

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

func (f *fsmSnapshot) Release() {}
