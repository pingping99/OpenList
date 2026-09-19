package dedup

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/fs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	hash_extend "github.com/OpenListTeam/OpenList/v4/pkg/utils/hash"
	log "github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
)

const (
	defaultConcurrency = 3
	maxConcurrency     = 8
	defaultQPS         = 2.5
	minQPS             = 0.1
	maxQPS             = 200
	defaultMaxDepth    = 10
	maxMaxDepth        = 64
)

func cleanExtList(exts []string) []string {
	if len(exts) == 0 {
		return nil
	}
	out := make([]string, 0, len(exts))
	seen := make(map[string]struct{}, len(exts))
	for _, e := range exts {
		for _, part := range strings.Split(e, ",") {
			cleaned := strings.ToLower(strings.TrimSpace(part))
			cleaned = strings.TrimPrefix(cleaned, ".")
			if cleaned == "" {
				continue
			}
			if _, ok := seen[cleaned]; !ok {
				seen[cleaned] = struct{}{}
				out = append(out, cleaned)
			}
		}
	}
	return out
}

// normalizeScanConfig 收敛用户传入的扫描参数，避免异常参数打爆存储或造成无限递归
func normalizeScanConfig(cfg ScanConfig) ScanConfig {
	if cfg.RootPath == "" {
		cfg.RootPath = "/"
	}
	cfg.RootPath = utils.FixAndCleanPath(cfg.RootPath)
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = defaultConcurrency
	}
	if cfg.Concurrency > maxConcurrency {
		cfg.Concurrency = maxConcurrency
	}
	if cfg.QPS <= 0 {
		cfg.QPS = defaultQPS
	}
	if cfg.QPS < minQPS {
		cfg.QPS = minQPS
	}
	if cfg.QPS > maxQPS {
		cfg.QPS = maxQPS
	}
	if cfg.MaxDepth <= 0 {
		cfg.MaxDepth = defaultMaxDepth
	}
	if cfg.MaxDepth > maxMaxDepth {
		cfg.MaxDepth = maxMaxDepth
	}
	if cfg.MinSize < 0 {
		cfg.MinSize = 0
	}
	cfg.IncludeExts = cleanExtList(cfg.IncludeExts)
	cfg.ExcludeExts = cleanExtList(cfg.ExcludeExts)
	return cfg
}

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

// ==================== 哈希解析（P0-2 修复核心） ====================

// preferredHashTypes 决定同一批文件使用哈希的口径：固定优先级（GCID 优先，其次 MD5），
// 保证一次扫描内所有文件使用同一判定标准，不会出现「同组内混用不同算法」的口径不一致。
var preferredHashTypes = buildHashPreference()

func buildHashPreference() []*utils.HashType {
	preferred := []*utils.HashType{hash_extend.GCID, utils.MD5, utils.SHA1, utils.SHA256}
	out := make([]*utils.HashType, 0, len(preferred)+len(utils.Supported))
	seen := make(map[*utils.HashType]struct{}, len(preferred))
	for _, ht := range preferred {
		if ht == nil {
			continue
		}
		if _, ok := seen[ht]; ok {
			continue
		}
		seen[ht] = struct{}{}
		out = append(out, ht)
	}
	for _, ht := range utils.Supported {
		if _, ok := seen[ht]; ok {
			continue
		}
		seen[ht] = struct{}{}
		out = append(out, ht)
	}
	return out
}

// resolveHash 返回驱动提供的可用哈希，形如 ("md5", "D41D8CD98F00B204E9800998ECF8427E")。
// 若驱动没有提供任何可用哈希，返回空值，调用方必须把该文件视为「未校验」。
//
// 注意：这里绝不能退化成 utils.HashInfo.String() 兜底。HashInfo 的零值（h == nil）经
// json.Marshal 会得到字符串 "null"，非空判断会成立，于是所有「无哈希」文件都会被塞进
// 同一个 hash 分组，最终把「同尺寸的不同文件」误判为重复文件（P0-2）。
func resolveHash(obj model.Obj) (hashType, hashValue string) {
	hi := obj.GetHash()
	for _, ht := range preferredHashTypes {
		if v := hi.GetHash(ht); v != "" {
			return ht.Name, strings.ToUpper(v)
		}
	}
	return "", ""
}

// candidateGroupKey 无哈希文件的候选分组键：同名（忽略大小写）+ 同尺寸。
// 名称做一次摘要，避免超长文件名撑爆键长度。
func candidateGroupKey(name string, size int64) string {
	digest := utils.HashData(utils.MD5, []byte(strings.ToLower(name)))
	return fmt.Sprintf("same:%d:%s", size, strings.ToLower(digest))
}

// verifiedGroupKey 已校验分组键，带算法前缀，保证不同算法之间不会互相比较
func verifiedGroupKey(hashType, hash string) string {
	return hashType + ":" + hash
}

// ==================== 扫描主体 ====================

// dirLister 列举一个目录。抽出为函数类型便于单元测试注入内存目录树。
type dirLister func(ctx context.Context, dir string) ([]model.Obj, error)

func defaultDirLister(ctx context.Context, dir string) ([]model.Obj, error) {
	meta, _ := op.GetNearestMeta(dir)
	return fs.List(context.WithValue(ctx, conf.MetaKey, meta), dir, &fs.ListArgs{})
}

// Scan 执行有界并发、限速、两阶段过滤的查重扫描。
//
// 阶段一：遍历目录，按「文件大小」粗筛，同时记录每个文件的哈希（可校验）或
// 「同名同尺寸」（不可校验候选）；
// 阶段二：同尺寸文件按哈希精确聚合，产出 verified 分组；无哈希文件按同名同尺寸
type fileCacheItem struct {
	Size     int64
	Modified time.Time
	HashType string
	Hash     string
}

func loadFileCache(rootPath, baseTaskID string) map[string]fileCacheItem {
	d := db.GetDb()
	targetTaskID := baseTaskID
	if targetTaskID == "" {
		var latestTask DedupTask
		if err := d.Where("root_path = ? AND state = ? AND (is_snapshot = ? OR snapshot_files > 0)", rootPath, "finished", true).
			Order("started_at DESC").First(&latestTask).Error; err == nil {
			targetTaskID = latestTask.ID
		}
	}
	if targetTaskID == "" {
		return nil
	}

	var items []DedupSnapshotItem
	if err := d.Where("task_id = ?", targetTaskID).Find(&items).Error; err != nil {
		log.Warnf("[dedup] failed to load snapshot items for base task %s: %v", targetTaskID, err)
		return nil
	}
	if len(items) == 0 {
		return nil
	}

	cache := make(map[string]fileCacheItem, len(items))
	for _, it := range items {
		cache[it.Path] = fileCacheItem{
			Size:     it.Size,
			Modified: it.Modified,
			HashType: it.HashType,
			Hash:     it.Hash,
		}
	}
	log.Infof("[dedup] loaded %d snapshot cache items from base task %s for %s", len(cache), targetTaskID, rootPath)
	return cache
}

// Scan 执行有界并发、限速、两阶段过滤的查重扫描。
//
// 阶段一：遍历目录，按「文件大小」粗筛，同时记录每个文件的哈希（可校验）或
// 「同名同尺寸」（不可校验候选）；
// 阶段二：同尺寸文件按哈希精确聚合，产出 verified 分组；无哈希文件按同名同尺寸
// 产出 candidate 分组。candidate 仅供人工确认，不参与批量清理。
func Scan(ctx context.Context, cfg ScanConfig, p *Progress) ([]DupGroup, map[string]*DirStat, []FileItem, error) {
	return scan(ctx, cfg, p, defaultDirLister)
}

func scan(ctx context.Context, cfg ScanConfig, p *Progress, lister dirLister) ([]DupGroup, map[string]*DirStat, []FileItem, error) {
	cfg = normalizeScanConfig(cfg)
	limiter := rate.NewLimiter(rate.Limit(cfg.QPS), 1)
	queue := newDirWorkQueue()

	var fileCache map[string]fileCacheItem
	if cfg.Incremental {
		fileCache = loadFileCache(cfg.RootPath, cfg.BaseTaskID)
	}

	stopWatch := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			queue.Close()
		case <-stopWatch:
		}
	}()
	defer close(stopWatch)

	var (
		mu          sync.Mutex
		sizeBuckets = make(map[int64]map[string][]FileItem) // size -> groupKey -> files（可校验）
		candidates  = make(map[string][]FileItem)           // groupKey -> files（不可校验候选）
		dirStats    = make(map[string]*DirStat)
		allFiles    []FileItem                              // 保存全量探测到的文件（用于快照持久化）
	)

	classify := func(dir string, objs []model.Obj, depth int) {
		var verified, unverified []FileItem
		var dirFileCount int
		var dirTotalSize int64
		var currentDirFiles []FileItem

		for _, obj := range objs {
			if obj.IsDir() {
				if depth < cfg.MaxDepth {
					queue.Push(scanDirQueueItem{Path: path.Join(dir, obj.GetName()), Depth: depth + 1})
				}
				continue
			}
			dirFileCount++
			dirTotalSize += obj.GetSize()

			filePath := path.Join(dir, obj.GetName())
			item := FileItem{
				Path:     filePath,
				Name:     obj.GetName(),
				Size:     obj.GetSize(),
				Modified: obj.ModTime(),
			}
			item.HashType, item.Hash = resolveHash(obj)

			// 增量哈希匹配：若大小与修改时间一致，复用历史哈希并记录增量命中
			if fileCache != nil {
				if cached, ok := fileCache[filePath]; ok && cached.Size == item.Size && cached.Modified.Equal(item.Modified) {
					if item.Hash == "" && cached.Hash != "" {
						item.HashType = cached.HashType
						item.Hash = cached.Hash
					}
					p.cachedFiles.Add(1)
					p.cachedBytes.Add(item.Size)
				}
			}

			currentDirFiles = append(currentDirFiles, item)

			if cfg.MinSize > 0 && obj.GetSize() < cfg.MinSize {
				continue
			}
			ext := strings.ToLower(strings.TrimPrefix(path.Ext(obj.GetName()), "."))
			if len(cfg.IncludeExts) > 0 {
				matched := false
				for _, ie := range cfg.IncludeExts {
					if ext == ie {
						matched = true
						break
					}
				}
				if !matched {
					continue
				}
			}
			if len(cfg.ExcludeExts) > 0 {
				excluded := false
				for _, ee := range cfg.ExcludeExts {
					if ext == ee {
						excluded = true
						break
					}
				}
				if excluded {
					continue
				}
			}

			if item.Hash != "" {
				verified = append(verified, item)
			} else if item.Size > 0 {
				// 无可用哈希：只能作为「同名同尺寸」候选，并明确标记为未校验
				unverified = append(unverified, item)
			}
		}

		mu.Lock()
		if dirStats[dir] == nil {
			dirStats[dir] = &DirStat{Path: dir}
		}
		dirStats[dir].FileCount += dirFileCount
		dirStats[dir].TotalSize += dirTotalSize
		allFiles = append(allFiles, currentDirFiles...)
		mu.Unlock()

		if len(verified) > 0 {
			mu.Lock()
			for _, f := range verified {
				byHash, ok := sizeBuckets[f.Size]
				if !ok {
					byHash = make(map[string][]FileItem)
					sizeBuckets[f.Size] = byHash
				}
				key := verifiedGroupKey(f.HashType, f.Hash)
				byHash[key] = append(byHash[key], f)
			}
			mu.Unlock()
		}
		if len(unverified) > 0 {
			mu.Lock()
			for _, f := range unverified {
				key := candidateGroupKey(f.Name, f.Size)
				candidates[key] = append(candidates[key], f)
			}
			mu.Unlock()
		}

		p.scannedFiles.Add(int64(len(verified) + len(unverified)))
		p.verifiedFiles.Add(int64(len(verified)))
		p.unverifiedFiles.Add(int64(len(unverified)))
	}

	// 根目录单独列举：根目录不可访问时直接判定任务失败，避免「路径写错却提示扫描完成、
	// 0 组重复」的假成功（旧实现只打一条 Warn 日志）。
	rootObjs, err := listWithRetry(ctx, limiter, lister, cfg.RootPath)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("无法列举根目录 %s: %w", cfg.RootPath, err)
	}
	p.scannedDirs.Add(1)
	classify(cfg.RootPath, rootObjs, 0)

	var wg sync.WaitGroup
	for i := 0; i < cfg.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				item, ok := queue.Pop()
				if !ok {
					return
				}
				if ctx.Err() != nil {
					queue.Done()
					return
				}
				objs, err := listWithRetry(ctx, limiter, lister, item.Path)
				queue.Done()
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					p.failedDirs.Add(1)
					log.Warnf("[dedup] list dir failed: %s, err: %v", item.Path, err)
					continue
				}
				p.scannedDirs.Add(1)
				classify(item.Path, objs, item.Depth)
			}
		}()
	}

	wg.Wait()

	if err := ctx.Err(); err != nil {
		return nil, nil, nil, err
	}

	// ========== 第二阶段：按哈希精确聚合 ==========
	var groups []DupGroup
	for size, byHash := range sizeBuckets {
		for key, files := range byHash {
			if len(files) < 2 {
				continue
			}
			sortByModified(files)
			groups = append(groups, DupGroup{
				GroupKey:    key,
				HashType:    files[0].HashType,
				Hash:        files[0].Hash,
				Size:        size,
				Verified:    true,
				WastedBytes: size * int64(len(files)-1),
				Files:       files,
			})
		}
	}
	// 未校验候选：同名同尺寸，内容未经验证，仅作为人工确认线索
	for key, files := range candidates {
		if len(files) < 2 {
			continue
		}
		sortByModified(files)
		groups = append(groups, DupGroup{
			GroupKey:    key,
			Size:        files[0].Size,
			Verified:    false,
			WastedBytes: files[0].Size * int64(len(files)-1),
			Files:       files,
		})
	}

	// 已校验分组优先，其次按可释放空间降序
	sort.Slice(groups, func(i, j int) bool {
		if groups[i].Verified != groups[j].Verified {
			return groups[i].Verified
		}
		if groups[i].WastedBytes != groups[j].WastedBytes {
			return groups[i].WastedBytes > groups[j].WastedBytes
		}
		return groups[i].GroupKey < groups[j].GroupKey
	})

	stats := p.Snapshot()
	stats.DupGroups, stats.DupFiles, stats.WastedBytes = 0, 0, 0
	stats.CandidateGroups, stats.CandidateFiles = 0, 0
	for _, g := range groups {
		if g.Verified {
			stats.DupGroups++
			stats.DupFiles += len(g.Files)
			stats.WastedBytes += g.WastedBytes
		} else {
			stats.CandidateGroups++
			stats.CandidateFiles += len(g.Files)
		}
	}
	p.Store(stats)

	return groups, dirStats, allFiles, nil
}

func sortByModified(files []FileItem) {
	sort.Slice(files, func(i, j int) bool {
		if files[i].Modified.Equal(files[j].Modified) {
			return files[i].Path < files[j].Path
		}
		return files[i].Modified.Before(files[j].Modified)
	})
}

// listWithRetry 列举目录，带限速与 429/限流退避重试
func listWithRetry(ctx context.Context, limiter *rate.Limiter, lister dirLister, dir string) ([]model.Obj, error) {
	if err := limiter.Wait(ctx); err != nil {
		return nil, err
	}
	objs, err := lister(ctx, dir)
	if err == nil {
		return objs, nil
	}
	if !isRateLimited(err) {
		return nil, err
	}
	log.Warnf("[dedup] rate limited at %s, backing off for 3s", dir)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(3 * time.Second):
	}
	if err := limiter.Wait(ctx); err != nil {
		return nil, err
	}
	return lister(ctx, dir)
}

func isRateLimited(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "429") || strings.Contains(msg, "rate limit") || strings.Contains(msg, "too many requests")
}
