package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"sync"

	"github.com/ld9999999999/go-interfacetools"
	"github.com/nahanni/go-ucl"
	"github.com/xiaokangwang/s3emu/accessqueue"
	"github.com/xiaokangwang/s3emu/backend/gdrive"
	"github.com/xiaokangwang/s3emu/s3in"
)

type GDriveConfigure struct {
	Basedir string
	Bucket  string
}

type BackendConfigure struct {
	Gdrive []GDriveConfigure
}

type BackupConfigure struct {
	ListenAddress string
	UploadWorker  int
	UploadBacklog int
	Backend       BackendConfigure
}

func main() {
	var conffile BackupConfigure
	cfg, err := os.Open(os.Args[1])
	if err != nil {
		panic(err)
	}

	p := ucl.NewParser(cfg)
	result, err := p.Ucl()
	if err != nil {
		panic(err)
	}
	interfacetools.CopyOut(result, &conffile)
	var quitwaitgroup sync.WaitGroup
	quitctx := context.Background()
	emu := s3in.New()
	for _, conf := range conffile.Backend.Gdrive {
		gaccess := gdrive.NewGDriveBackend(conf.Basedir)
		accessQueue := accessqueue.NewAccessQueue(conffile.UploadWorker, conffile.UploadBacklog, gaccess, quitctx, &quitwaitgroup)
		emu.SetSource(conf.Bucket, accessQueue)
	}
	listenAndServe(conffile.ListenAddress, emu.Server())
}

func listenAndServe(addr string, handler http.Handler) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Println("failed to listen:", err)
		return
	}

	log.Println("using port:", listener.Addr().(*net.TCPAddr).Port)
	server := &http.Server{Addr: addr, Handler: handler}
	server.Serve(listener)
}
