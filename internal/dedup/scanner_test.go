package dedup

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDirWorkQueue_Basic(t *testing.T) {
	q := newDirWorkQueue()
	q.Push(scanDirQueueItem{Path: "/", Depth: 0})

	var wg sync.WaitGroup
	var count int32

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				item, ok := q.Pop()
				if !ok {
					return
				}
				atomic.AddInt32(&count, 1)
				if item.Depth < 4 {
					// 每个目录产生 2 个子目录
					q.Push(scanDirQueueItem{Path: item.Path + "a/", Depth: item.Depth + 1})
					q.Push(scanDirQueueItem{Path: item.Path + "b/", Depth: item.Depth + 1})
				}
				q.Done()
			}
		}()
	}

	wg.Wait()
	// 1 + 2 + 4 + 8 + 16 = 31 个节点
	if count != 31 {
		t.Fatalf("expected 31 items processed, got %d", count)
	}
}

func TestDirWorkQueue_Cancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	q := newDirWorkQueue()

	go func() {
		<-ctx.Done()
		q.Close()
	}()

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				_, ok := q.Pop()
				if !ok {
					return
				}
			}
		}()
	}

	time.Sleep(20 * time.Millisecond)
	cancel() // 唤醒所有阻塞在 Pop 上的 Worker
	wg.Wait()
}
