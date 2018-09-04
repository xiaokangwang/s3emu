package lgpd

import "io"

type LGPD interface {
	Get(key string, nofetch bool) ([]byte, File, error)
	GetS(key string, nofetch bool) (io.ReadCloser, File, error)
	Put(key string, value []byte) error
	List(perfix string) []File
}

type File struct {
	Name   string
	Length int
	Mark   string
}
