package store

import (
	"crypto/sha1"
	"encoding/hex"
	"io"
	"log"
	"os"
	"strings"
)

var defaultRoot = "dfs"

type Path struct {
	fileName string
	dirPath  string
}

type Store struct {
	pathTransformFunc func(string) *Path
}

func pathTransform(key string) *Path {
	hash := sha1.Sum([]byte(key))
	hashStr := hex.EncodeToString(hash[:])

	pathLen := 5
	pathFolder := make([]string, pathLen)
	for i := range pathFolder {
		pathFolder[i] = hashStr[i*pathLen : (i+1)*pathLen]
	}
	return &Path{fileName: hashStr, dirPath: strings.Join(pathFolder, "/")}
}

func NewStore() *Store {
	return &Store{
		pathTransformFunc: pathTransform,
	}
}

func (p *Path) getFullPath() string {
	return defaultRoot + "/" + p.dirPath
}

func (p *Path) getFirstDir() string {
	parts := strings.Split(p.dirPath, "/")
	if len(parts) > 0 {
		return defaultRoot + "/" + parts[0]
	}
	return ""
}

func (s *Store) Delete(key string) bool {
	transformPath := s.pathTransformFunc(key)
	err := os.RemoveAll(transformPath.getFirstDir())
	if err != nil {
		return false
	}
	return true
}

func (s *Store) Exists(key string) bool {
	transformPath := s.pathTransformFunc(key)
	_, err := os.Stat(transformPath.getFullPath() + "/" + transformPath.fileName)
	return !os.IsNotExist(err)
}

func (s *Store) ReadStream(key string) error {
	transformPath := s.pathTransformFunc(key)
	f, err := os.Open(transformPath.getFullPath() + "/" + transformPath.fileName)
	if err != nil {
		return err
	}
	defer f.Close()

	buff := new(strings.Builder)
	_, err = io.Copy(buff, f)
	if err != nil {
		return err
	}
	log.Printf("Read stream: %s", buff.String())

	return nil
}

func (s *Store) writeStream(key string, r io.Reader) error {
	transformPath := s.pathTransformFunc(key)
	err := os.MkdirAll(transformPath.getFullPath(), 0755)
	if err != nil {
		return err
	}
	f, err := os.Create(transformPath.getFullPath() + "/" + transformPath.fileName)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = io.Copy(f, r)
	if err != nil {
		return err
	}

	return nil
}
