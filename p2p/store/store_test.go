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
