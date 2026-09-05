package dedup

import (
	"context"
	"path"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/fs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	hash_extend "github.com/OpenListTeam/OpenList/v4/pkg/utils/hash"
	log "github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
)

type scanDirQueueItem struct {
	Path  string
	Depth int
}

// 并发安全、自动扩容、无死锁/无关闭异常的目录工作队列
type dirWorkQueue struct {
	mu     sync.Mutex
	cond   *sync.Cond
	items  []scanDirQueueItem
	active int
	closed bool
}

func newDirWorkQueue() *dirWorkQueue {
	q := &dirWorkQueue{}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *dirWorkQueue) Push(item scanDirQueueItem) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	q.items = append(q.items, item)
	q.cond.Signal()
}

func (q *dirWorkQueue) Pop() (scanDirQueueItem, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for len(q.items) == 0 && !q.closed {
		if q.active == 0 {
			q.closed = true
			q.cond.Broadcast()
			return scanDirQueueItem{}, false
		}
		q.cond.Wait()
	}

	if q.closed || len(q.items) == 0 {
		return scanDirQueueItem{}, false
	}

	item := q.items[0]
	q.items = q.items[1:]
	q.active++
	return item, true
}

func (q *dirWorkQueue) Done() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.active--
	if len(q.items) == 0 && q.active == 0 {
		q.closed = true
		q.cond.Broadcast()
	}
}

func (q *dirWorkQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.cond.Broadcast()
}

// ScanWithWorkerPool 执行有界并发、限速与两阶段双重过滤的查重扫描
func ScanWithWorkerPool(ctx context.Context, cfg ScanConfig, task *TaskContext) ([]DupGroup, error) {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 3
	}
	if cfg.Concurrency > 8 {
		cfg.Concurrency = 8
	}
	if cfg.QPS <= 0 {
		cfg.QPS = 2.5
	}
	if cfg.MaxDepth <= 0 {
		cfg.MaxDepth = 30
	}

	limiter := rate.NewLimiter(rate.Limit(cfg.QPS), 1)
	queue := newDirWorkQueue()

	// 监听 context 取消，及时唤醒队列退出
	go func() {
		<-ctx.Done()
		queue.Close()
	}()

	var wg sync.WaitGroup
	var mu sync.Mutex
	sizeMap := make(map[int64][]FileItem)

	var (
		scannedDirs  int64
		scannedFiles int64
	)

	// 统计重复组数（实时估算）
	var dupGroupCount int64
	var dupFileCount int64

	for i := 0; i < cfg.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				// 检查 context 是否已取消
				select {
				case <-ctx.Done():
					return
				default:
				}

				item, ok := queue.Pop()
				if !ok {
					return
				}

				if err := limiter.Wait(ctx); err != nil {
					queue.Done()
					return
				}

				meta, _ := op.GetNearestMeta(item.Path)
				var (
					objs []model.Obj
					err  error
				)

				for retry := 0; retry < 2; retry++ {
					objs, err = fs.List(context.WithValue(ctx, conf.MetaKey, meta), item.Path, &fs.ListArgs{})
					if err != nil && (strings.Contains(err.Error(), "429") || strings.Contains(err.Error(), "rate")) {
						log.Warnf("[dedup] rate limited at %s, backing off for 3s", item.Path)
						select {
						case <-ctx.Done():
							queue.Done()
							return
						case <-time.After(3 * time.Second):
						}
						continue
					}
					break
				}

				atomic.AddInt64(&scannedDirs, 1)

				if err == nil {
					var batchFiles []FileItem
					for _, obj := range objs {
						fullPath := path.Join(item.Path, obj.GetName())
						if obj.IsDir() {
							if item.Depth < cfg.MaxDepth {
								queue.Push(scanDirQueueItem{
									Path:  fullPath,
									Depth: item.Depth + 1,
								})
							}
						} else if obj.GetSize() > 0 {
							h := obj.GetHash().GetHash(hash_extend.GCID)
							if h == "" {
								h = obj.GetHash().String()
							}
							if h != "" {
								batchFiles = append(batchFiles, FileItem{
									Path:     fullPath,
									Name:     obj.GetName(),
									Size:     obj.GetSize(),
									Hash:     strings.ToUpper(h),
									Modified: obj.ModTime(),
								})
							}
						}
					}

					if len(batchFiles) > 0 {
						atomic.AddInt64(&scannedFiles, int64(len(batchFiles)))
						mu.Lock()
						for _, f := range batchFiles {
							sizeMap[f.Size] = append(sizeMap[f.Size], f)
						}
						mu.Unlock()
					}
				} else {
					log.Warnf("[dedup] list dir failed: %s, err: %v", item.Path, err)
				}

				queue.Done()

				// 同步进度到内存
				task.mu.Lock()
				task.Task.ScannedDirs = atomic.LoadInt64(&scannedDirs)
				task.Task.ScannedFiles = atomic.LoadInt64(&scannedFiles)
				task.Task.DupGroups = int(atomic.LoadInt64(&dupGroupCount))
				task.Task.DupFiles = int(atomic.LoadInt64(&dupFileCount))
				task.mu.Unlock()
			}
		}()
	}

	// 初始根目录推入队列
	queue.Push(scanDirQueueItem{Path: cfg.RootPath, Depth: 0})

	// 启动定期持久化协程：每 5 秒写 DB
	tickerDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				task.mu.Lock()
				taskCopy := task.Task
				task.mu.Unlock()
				SaveTask(&taskCopy)
			case <-tickerDone:
				return
			}
		}
	}()

	wg.Wait()
	close(tickerDone)

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	// ========== 第二阶段：双重过滤，GCID 精确聚合 ==========
	var dupGroups []DupGroup
	var totalWasted int64
	var totalDupFiles int

	for size, files := range sizeMap {
		if len(files) < 2 {
			continue
		}

		hashMap := make(map[string][]FileItem)
		for _, f := range files {
			hashMap[f.Hash] = append(hashMap[f.Hash], f)
		}

		for h, fList := range hashMap {
			if len(fList) > 1 {
				sort.Slice(fList, func(i, j int) bool {
					return fList[i].Modified.Before(fList[j].Modified)
				})

				wasted := size * int64(len(fList)-1)
				totalWasted += wasted
				totalDupFiles += len(fList)

				dupGroups = append(dupGroups, DupGroup{
					Hash:        h,
					Size:        size,
					WastedBytes: wasted,
					Files:       fList,
				})
			}
		}
	}

	sort.Slice(dupGroups, func(i, j int) bool {
		return dupGroups[i].WastedBytes > dupGroups[j].WastedBytes
	})

	task.mu.Lock()
	task.Task.DupGroups = len(dupGroups)
	task.Task.DupFiles = totalDupFiles
	task.Task.WastedTotal = totalWasted
	task.mu.Unlock()

	return dupGroups, nil
}
