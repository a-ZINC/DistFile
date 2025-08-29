package store

import (
	"strings"
	"testing"
)

func TestStore(t *testing.T) {
	store := NewStore()

	buff := strings.NewReader("test data")
	err := store.writeStream("test_key", buff)
	if err != nil {
		t.Errorf("Failed to write stream: %v", err)
	}
}

func TestDelete(t *testing.T) {
	store := NewStore()

	success := store.Delete("test_key")
	if !success {
		t.Errorf("Failed to delete key")
	}
}

func TestReadStream(t *testing.T) {
	store := NewStore()

	err := store.ReadStream("test_key")
	if err != nil {
		t.Errorf("Failed to read stream: %v", err)
	}
}
