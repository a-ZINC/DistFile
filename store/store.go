package store

import (
	"bytes"
	"io"
	"log"
	"os"
	"strings"
)

type Path struct {
	FileName   string
	DirPath    string
	DefaultRoot string
}

type StoreOpts struct {
	DefaultRoot      string
	PathTransformFunc func(string, string) *Path
}

type Store struct {
	StoreOpts
}

func NewStore(storeOpts StoreOpts) *Store {
	return &Store{
		StoreOpts: storeOpts,
	}
}

func (p *Path) getFullPath() string {
	return p.DefaultRoot + "/" + p.DirPath
}

func (p *Path) getFirstDir() string {
	parts := strings.Split(p.DirPath, "/")
	if len(parts) > 0 {
		return p.DefaultRoot + "/" + parts[0]
	}
	return ""
}

func (s *Store) Clear() {
	err := os.RemoveAll(s.DefaultRoot)
	if err != nil {
		log.Printf("Failed to clear store: %v", err)
	}
}

func (s *Store) Delete(key string) bool {
	transformPath := s.PathTransformFunc(key, s.DefaultRoot)
	err := os.RemoveAll(transformPath.getFirstDir())
	if err != nil {
		return false
	}
	return true
}

func (s *Store) Exists(key string) bool {
	transformPath := s.PathTransformFunc(key, s.DefaultRoot)
	_, err := os.Stat(transformPath.getFullPath() + "/" + transformPath.FileName)
	return !os.IsNotExist(err)
}

func (s *Store) Read(key string) (io.Reader, error) {
	return s.readStream(key)
}

func (s *Store) readStream(key string) (io.Reader, error) {
	transformPath := s.PathTransformFunc(key, s.DefaultRoot)
	f, err := os.Open(transformPath.getFullPath() + "/" + transformPath.FileName)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	buff := new(bytes.Buffer)
	_, err = io.Copy(buff, f)
	if err != nil {
		return nil, err
	}
	log.Printf("Read stream: %s", buff.String())

	return buff, nil
}

func (s *Store) Write(key string, data io.Reader) error {
	return s.writeStream(key, data)
}

func (s *Store) writeStream(key string, r io.Reader) error {
	transformPath := s.PathTransformFunc(key, s.DefaultRoot)
	err := os.MkdirAll(transformPath.getFullPath(), 0755)
	if err != nil {
		return err
	}
	f, err := os.Create(transformPath.getFullPath() + "/" + transformPath.FileName)
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
