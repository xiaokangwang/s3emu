package accessqueue

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

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
	id                string
	backlogSum        int64
	totalSum          int64
}

type NetworkUploadTask struct {
	Filename string
	Content  []byte
}

func NewAccessQueue(uploadworkersum, maxbacklog int, directLGPD lgpd.LGPD, working context.Context, uploadWorker *sync.WaitGroup, id string) *AccessQueue {
	ret := &AccessQueue{}
	ret.uploadworkersum = uploadworkersum
	ret.maxbacklog = maxbacklog
	ret.working = working
	ret.directLGPD = directLGPD
	ret.uploadWorker = uploadWorker
	ret.uploadChan = make(chan NetworkUploadTask, ret.maxbacklog)
	ret.id = id

	for uploadworkersum >= 0 {
		uploadWorker.Add(1)
		go ret.UploadWorker()
		uploadworkersum--
	}
	return ret
}

func (aq *AccessQueue) UploadWorker() {
	for {
		select {
		case Task := <-aq.uploadChan:
			fmt.Printf("Uploading: %v->%v;\n", aq.id, Task.Filename)
			aq.directLGPD.Put(Task.Filename, Task.Content)
			aq.uploadSynclocker.Done()
			currentBacklog := atomic.AddInt64(&aq.backlogSum, -1)
			totalsum := atomic.LoadInt64(&aq.totalSum)
			fmt.Printf("Uploaded: %v->%v; Backlog %v, Total %v\n", aq.id, Task.Filename, currentBacklog, totalsum)
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
	aq.uploadSynclocker.Add(1)
	currentBacklog := atomic.AddInt64(&aq.totalSum, 1)
	totalsum := atomic.AddInt64(&aq.backlogSum, 1)
	fmt.Printf("Upload Queued: %v->%v; Backlog %v, Total %v\n", aq.id, key, currentBacklog, totalsum)
	aq.uploadChan <- task
	aq.uploadCloseStatus.Unlock()
	return nil
}
func (aq *AccessQueue) Get(key string, nofetch bool) ([]byte, lgpd.File, error) {
	aq.uploadSynclocker.Wait()
	return aq.directLGPD.Get(key, nofetch)
}
func (aq *AccessQueue) List(perfix string) []lgpd.File {
	return aq.directLGPD.List(perfix)
}
