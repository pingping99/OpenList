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
		// 如果队列空且没有任何 worker 处于处理中状态，说明所有任务均已完结
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
	// 如果队列空且所有活跃任务完成，广播所有 worker 退出
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

	// 令牌桶限流器 (全局平滑控制 QPS，避免 PikPak 触发 429 冷却)
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

	// 启动 Worker 线程池
	for i := 0; i < cfg.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				item, ok := queue.Pop()
				if !ok {
					return
				}

				// 1. 等待限流令牌
				if err := limiter.Wait(ctx); err != nil {
					queue.Done()
					return
				}

				// 2. 发起目录列表请求，含 429 异常退避重试
				meta, _ := op.GetNearestMeta(item.Path)
				var (
					objs []model.Obj
					err  error
				)

				for retry := 0; retry < 2; retry++ {
					objs, err = fs.List(context.WithValue(ctx, conf.MetaKey, meta), item.Path, &fs.ListArgs{})
					if err != nil && (strings.Contains(err.Error(), "429") || strings.Contains(err.Error(), "rate")) {
						log.Warnf("[dedup] rate limited at %s, backing off for 3s", item.Path)
						time.Sleep(3 * time.Second)
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
							// 深度范围控制：未达最大深度则继续入队
							if item.Depth < cfg.MaxDepth {
								queue.Push(scanDirQueueItem{
									Path:  fullPath,
									Depth: item.Depth + 1,
								})
							}
						} else if obj.GetSize() > 0 { // 严格排除 0 字节空文件，避免误杀
							// 提取 PikPak 的 GCID Hash
							h := obj.GetHash().GetHash(hash_extend.GCID)
							if h == "" {
								// 若无 GCID 则尝试获取通用 Hash 字符串
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

				// 标记当前目录项已完成
				queue.Done()

				// 定期同步任务进度
				task.mu.Lock()
				task.Status.ScannedDirs = atomic.LoadInt64(&scannedDirs)
				task.Status.ScannedFiles = atomic.LoadInt64(&scannedFiles)
				task.mu.Unlock()
			}
		}()
	}

	// 初始根目录推入队列
	queue.Push(scanDirQueueItem{Path: cfg.RootPath, Depth: 0})
	wg.Wait()

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	// ========== 第二阶段：双重过滤，GCID 精确聚合 ==========
	var dupGroups []DupGroup
	var totalWasted int64

	for size, files := range sizeMap {
		// 第一关过滤：大小唯一的文件直接跳过（节省 90% 以上的数据比对）
		if len(files) < 2 {
			continue
		}

		// 第二关过滤：同大小文件按 Hash 分组
		hashMap := make(map[string][]FileItem)
		for _, f := range files {
			hashMap[f.Hash] = append(hashMap[f.Hash], f)
		}

		for h, fList := range hashMap {
			if len(fList) > 1 {
				// 组内排序：按修改时间升序排列（最早修改的文件排在第一位）
				sort.Slice(fList, func(i, j int) bool {
					return fList[i].Modified.Before(fList[j].Modified)
				})

				wasted := size * int64(len(fList)-1)
				totalWasted += wasted

				dupGroups = append(dupGroups, DupGroup{
					Hash:        h,
					Size:        size,
					WastedBytes: wasted,
					Files:       fList,
				})
			}
		}
	}

	// 结果排序：按冗余浪费的空间降序排列（可释放空间最大的排在最前）
	sort.Slice(dupGroups, func(i, j int) bool {
		return dupGroups[i].WastedBytes > dupGroups[j].WastedBytes
	})

	task.mu.Lock()
	task.Status.DupGroups = len(dupGroups)
	task.Status.WastedTotal = totalWasted
	task.mu.Unlock()

	return dupGroups, nil
}
