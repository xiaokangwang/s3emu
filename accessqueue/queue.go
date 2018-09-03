package accessqueue

import (
	"context"
	"sync"

	"github.com/xiaokangwang/s3emu/lgpd"
)

type AccessQueue struct {
	uploadSynclocker  sync.WaitGroup
	uploadWorker      *sync.WaitGroup
	uploadCloseStatus sync.Mutex
	uploadworkersum   int
	maxbacklog        int
	working           context.Context
	directLGPD        lgpd.LGPD
	uploadChan        chan NetworkUploadTask
}

type NetworkUploadTask struct {
	Filename string
	Content  []byte
}

func NewAccessQueue(uploadworkersum, maxbacklog int, directLGPD lgpd.LGPD, working context.Context, uploadWorker *sync.WaitGroup) *AccessQueue {
	ret := &AccessQueue{}
	ret.uploadworkersum = uploadworkersum
	ret.maxbacklog = maxbacklog
	ret.working = working
	ret.directLGPD = directLGPD
	ret.uploadWorker = uploadWorker

	ret.uploadChan = make(chan NetworkUploadTask, ret.maxbacklog)
	return ret
}

func (aq *AccessQueue) UploadWorker() {
	for {
		select {
		case Task := <-aq.uploadChan:
			aq.directLGPD.Put(Task.Filename, Task.Content)
			aq.uploadSynclocker.Done()
		case <-aq.working.Done():
			aq.uploadCloseStatus.Lock()
			aq.Finishup()
			aq.uploadCloseStatus.Unlock()
			aq.uploadWorker.Done()
			return
		}
	}
}

func (aq *AccessQueue) Finishup() {
	for {
		select {
		case Task := <-aq.uploadChan:
			aq.directLGPD.Put(Task.Filename, Task.Content)
		default:
			return
		}
	}
}

func (aq *AccessQueue) Put(key string, value []byte) error {
	aq.uploadCloseStatus.Lock()

	if aq.working.Err() != nil {
		return aq.working.Err()
	}

	task := NetworkUploadTask{Filename: key, Content: value}
	aq.uploadChan <- task
	aq.uploadCloseStatus.Unlock()
	return nil
}
func (aq *AccessQueue) Get(key string) ([]byte, lgpd.File, error) {
	return aq.directLGPD.Get(key)
}
func (aq *AccessQueue) List(perfix string) []lgpd.File {
	return aq.directLGPD.List(perfix)
}
