package ftpd

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/goftp/server"
	"github.com/xiaokangwang/s3emu/lgpd"
)

type Ftpd struct {
	access map[string]lgpd.LGPD
}

type Fileinfo struct {
	name    string
	size    int64
	mode    os.FileMode
	modtime time.Time
	isDir   bool
}

func (fi Fileinfo) Name() string {
	return fi.name
}
func (fi Fileinfo) Size() int64 {
	return fi.size
}
func (fi Fileinfo) Mode() os.FileMode {
	amask := os.FileMode(0)
	if fi.isDir {
		amask |= os.ModeDir
	}
	return fi.mode | amask
}
func (fi Fileinfo) ModTime() time.Time {
	return fi.modtime
}
func (fi Fileinfo) IsDir() bool {
	return fi.isDir
}
func (fi Fileinfo) Sys() interface{} {
	return nil
}
func (fi Fileinfo) Owner() string { return "root" }
func (fi Fileinfo) Group() string { return "root" }

func (td Ftpd) Init(*server.Conn) {}
func (td Ftpd) Stat(s string) (server.FileInfo, error) {
	bucket := td.bucket(s)
	filename := td.filename(s)
	fmt.Printf("stat %v %v\n", bucket, filename)
	if bucket == "" {
		return &Fileinfo{isDir: true, name: "/"}, nil
	}
	access, ok := td.access[bucket]
	if !ok {
		return nil, errors.New("bucket not found")
	}
	if filename == "" {
		return &Fileinfo{isDir: true, name: bucket}, nil
	}
	_, file, err := access.Get(filename, true)

	if err != nil {
		return nil, err
	}
	return &Fileinfo{isDir: false, name: filename, size: int64(file.Length)}, nil
}
func (td Ftpd) ChangeDir(s string) error {
	return nil
}
func (td Ftpd) ListDir(s string, o func(server.FileInfo) error) error {
	if s == "/" {
		fmt.Printf("list /\n")
		for key := range td.access {
			println(o(Fileinfo{isDir: true, name: key}))
		}
		return nil
	}
	bucket := td.bucket(s)
	fmt.Printf("list %v", bucket)

	access, ok := td.access[bucket]
	if !ok {
		return errors.New("bucket not found")
	}

	list := access.List("")
	for _, listv := range list {
		o(Fileinfo{isDir: false, name: listv.Name, size: int64(listv.Length)})
	}
	return nil
}
func (td Ftpd) DeleteDir(s string) error {
	return nil
}
func (td Ftpd) DeleteFile(s string) error {
	return nil
}
func (td Ftpd) Rename(s string, s2 string) error {
	return nil
}
func (td Ftpd) MakeDir(s string) error {
	return nil
}
func (td Ftpd) GetFile(s string, s2 int64) (int64, io.ReadCloser, error) {
	if s2 != 0 {
		return 0, nil, errors.New("Not Supported")
	}
	bucket := td.bucket(s)
	filename := td.filename(s)
	fmt.Printf("get %v %v\n", bucket, filename)
	access, ok := td.access[bucket]
	if !ok {
		return 0, nil, errors.New("bucket not found")
	}
	body, file, err := access.GetS(filename, false)
	if err != nil {
		return 0, nil, err
	}

	return int64(file.Length), body, nil

}
func (td Ftpd) PutFile(s string, r io.Reader, o bool) (i int64, ret error) {
	defer func() {
		recover()
		ret = errors.New("Unexpected Error")
		i = 0
	}()
	if o {
		return 0, errors.New("Not Supported")
	}
	bucket := td.bucket(s)
	filename := td.filename(s)
	fmt.Printf("put %v %v\n", bucket, filename)
	access, ok := td.access[bucket]
	if !ok {
		return 0, errors.New("bucket not found")
	}
	by, err := ioutil.ReadAll(r)
	if err != nil {
		return 0, err
	}
	err = access.Put(filename, by)
	if err != nil {
		return 0, err
	}
	return int64(len(by)), nil
}
func (td *Ftpd) bucket(s string) string {
	defer func() {
		recover()
	}()
	return strings.Split(s, "/")[1]
}
func (td *Ftpd) filename(s string) string {
	defer func() {
		recover()
	}()
	return strings.Split(s, "/")[2]
}

func (td *Ftpd) SetSource(bk string, hd lgpd.LGPD) {
	if td.access == nil {
		td.access = make(map[string]lgpd.LGPD)
	}
	td.access[bk] = hd
}
