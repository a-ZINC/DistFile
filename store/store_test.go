package store

import (
	"crypto/sha1"
	"encoding/hex"
	"strings"
	"testing"
)

func pathTransform(key string, root string) *Path {
	hash := sha1.Sum([]byte(key))
	hashStr := hex.EncodeToString(hash[:])

	pathLen := 5
	pathFolder := make([]string, pathLen)
	for i := range pathFolder {
		pathFolder[i] = hashStr[i*pathLen : (i+1)*pathLen]
	}
	return &Path{FileName: hashStr, DirPath: strings.Join(pathFolder, "/"), DefaultRoot: root}
}

func TestStore(t *testing.T) {
	StoreOpts := StoreOpts{
		DefaultRoot:       "/tmp/store",
		PathTransformFunc: pathTransform,
	}
	store := NewStore(StoreOpts)
	defer store.Clear()

	buff := strings.NewReader("test data")
	err := store.Write("test_key", buff)
	if err != nil {
		t.Errorf("Failed to write stream: %v", err)
	}
}

func TestDelete(t *testing.T) {
	StoreOpts := StoreOpts{
		DefaultRoot:       "/tmp/store",
		PathTransformFunc: pathTransform,
	}
	store := NewStore(StoreOpts)

	success := store.Delete("test_key")
	if !success {
		t.Errorf("Failed to delete key")
	}
}

func TestRead(t *testing.T) {
	StoreOpts := StoreOpts{
		DefaultRoot:       "/tmp/store",
		PathTransformFunc: pathTransform,
	}
	store := NewStore(StoreOpts)

	_, err := store.Read("test_key")
	if err != nil {
		t.Errorf("Failed to read: %v", err)
	}
}
