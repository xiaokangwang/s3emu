package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"

	fserver "github.com/goftp/server"
	"github.com/ld9999999999/go-interfacetools"
	"github.com/nahanni/go-ucl"
	"github.com/xiaokangwang/s3emu/accessqueue"
	"github.com/xiaokangwang/s3emu/backend/gdrive"
	"github.com/xiaokangwang/s3emu/ftpd"
)

type GDriveConfigure struct {
	Basedir string `json:"Basedir"`
	Bucket  string `json:"Bucket"`
}

type BackendConfigure struct {
	Gdrive []GDriveConfigure `json:"Gdrive"`
}

type BackupConfigure struct {
	ListenAddress string           `json:"ListenAddress"`
	UploadWorker  int              `json:"UploadWorker"`
	UploadBacklog int              `json:"UploadBacklog"`
	Backend       BackendConfigure `json:"Backend"`
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
	fmt.Println(conffile)
	var quitwaitgroup sync.WaitGroup
	b := context.Background()
	quitctx, cancel := context.WithCancel(b)
	emu := ftpd.Ftpd{}
	for _, conf := range conffile.Backend.Gdrive {
		gaccess := gdrive.NewGDriveBackend(conf.Basedir)
		accessQueue := accessqueue.NewAccessQueue(conffile.UploadWorker, conffile.UploadBacklog, gaccess, quitctx, &quitwaitgroup, conf.Bucket)
		emu.SetSource(conf.Bucket, accessQueue)
	}
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		cancel()
		quitwaitgroup.Wait()
		os.Exit(0)
	}()
	listenAndServeFTP(conffile.ListenAddress, emu)
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

type Single struct {
	r fserver.Driver
}

func (s *Single) NewDriver() (fserver.Driver, error) {
	return s.r, nil
}

func (s *Single) CheckPasswd(string, string) (bool, error) {
	return true, nil
}

func listenAndServeFTP(addr string, handler ftpd.Ftpd) {

	i, _ := strconv.Atoi(addr)
	fac := &Single{r: handler}
	server := &fserver.ServerOpts{Hostname: "127.0.0.1", Port: i, Factory: fac, Auth: fac}
	sins := fserver.NewServer(server)
	sins.ListenAndServe()
}
