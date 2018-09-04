package accessqueue

import (
	"context"
	"fmt"
	"io"
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
	oppulist          []lgpd.File
	listcache         []lgpd.File
}

type NetworkUploadTask struct {
	Filename string
	Content  []byte
}

func NewAccessQueue(uploadworkersum, maxbacklog int, directLGPD lgpd.LGPD, working context.Context, uploadWorker *sync.WaitGroup, id string) *AccessQueue {
	fmt.Printf("AQ Created")
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
	totalsum := atomic.AddInt64(&aq.totalSum, 1)
	currentBacklog := atomic.AddInt64(&aq.backlogSum, 1)
	fmt.Printf("Upload Queued: %v->%v; Backlog %v, Total %v\n", aq.id, key, currentBacklog, totalsum)
	aq.uploadChan <- task
	aq.oppulist = append(aq.oppulist, lgpd.File{Name: key, Length: len(value)})
	aq.uploadCloseStatus.Unlock()
	return nil
}
func (aq *AccessQueue) Get(key string, nofetch bool) ([]byte, lgpd.File, error) {
	aq.uploadSynclocker.Wait()
	return aq.directLGPD.Get(key, nofetch)
}
func (aq *AccessQueue) GetS(key string, nofetch bool) (io.ReadCloser, lgpd.File, error) {
	aq.uploadSynclocker.Wait()
	return aq.directLGPD.GetS(key, nofetch)
}
func (aq *AccessQueue) List(perfix string) []lgpd.File {
	if (len(aq.listcache)) == 0 {
		aq.listcache = aq.directLGPD.List(perfix)
	}
	result := append(aq.listcache, aq.oppulist...)
	var resultx []lgpd.File
	dedup := make(map[string]bool)
	for _, ctx := range result {
		_, dup := dedup[ctx.Name]
		dedup[ctx.Name] = true
		if !dup {
			resultx = append(resultx, ctx)
		}
	}
	return resultx
}
