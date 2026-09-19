package dedup

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/task"
	"github.com/OpenListTeam/tache"
	log "github.com/sirupsen/logrus"
)

// DedupScanTask 遵循 OpenList 官方 tache 规范的去重扫描任务。
//
// 所有可变状态（status/progress/stats）都由 mu 保护：后台任务在写、HTTP 接口在并发读，
// 旧实现让接口无锁直接读这些字段，存在数据竞争。
type DedupScanTask struct {
	task.TaskExtension

	Name   string     `json:"name"`
	Config ScanConfig `json:"config"`

	mu       sync.RWMutex
	status   string
	progress float64
	stats    ScanStats
}

var _ task.TaskExtensionInfo = (*DedupScanTask)(nil)

// TaskStatus 是任务状态对外的统一视图（HTTP 接口与内置任务列表共用同一份结构）
type TaskStatus struct {
	State    string    `json:"state"`
	Status   string    `json:"status"`
	Progress float64   `json:"progress"`
	Stats    ScanStats `json:"stats"`
}

func (t *DedupScanTask) GetName() string {
	if t.Name != "" {
		return t.Name
	}
	return fmt.Sprintf("去重扫描: %s", t.Config.RootPath)
}

func (t *DedupScanTask) GetStatus() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.status
}

func (t *DedupScanTask) SetStatus(s string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.status = s
}

func (t *DedupScanTask) GetProgress() float64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.progress
}

func (t *DedupScanTask) SetProgress(p float64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.progress = p
}

// Stats 返回统计快照（并发安全）
func (t *DedupScanTask) Stats() ScanStats {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.stats
}

// Snapshot 返回状态、进度与统计的一致快照
func (t *DedupScanTask) Snapshot() TaskStatus {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return TaskStatus{
		State:    tacheStateName(t.GetState()),
		Status:   t.status,
		Progress: t.progress,
		Stats:    t.stats,
	}
}

// tacheStateName 把 tache 内部状态统一映射为字符串。旧实现同一接口在不同分支
// 分别返回 int 与 string 两种 state 形态，前端无法统一处理。
func tacheStateName(s tache.State) string {
	switch s {
	case tache.StateRunning:
		return "running"
	case tache.StateSucceeded:
		return "finished"
	case tache.StateCanceling:
		return "canceling"
	case tache.StateCanceled:
		return "canceled"
	case tache.StateErrored:
		return "errored"
	case tache.StateFailing:
		return "failed"
	case tache.StateFailed:
		return "failed"
	case tache.StateWaitingRetry, tache.StatePending, tache.StateBeforeRetry:
		return "queued"
	default:
		return "unknown"
	}
}

// Run 执行具体的扫描逻辑 (tache 框架调度入口)
func (t *DedupScanTask) Run() error {
	ctx := t.Ctx()
	if ctx == nil {
		ctx = context.Background()
	}

	now := time.Now()
	t.SetStartTime(now)
	t.SetStatus("正在扫描...")
	t.SetProgress(0)

	progress := &Progress{}
	taskRow := &DedupTask{
		ID:          t.GetID(),
		RootPath:    t.Config.RootPath,
		State:       "running",
		MaxDepth:    t.Config.MaxDepth,
		Concurrency: t.Config.Concurrency,
		QPS:         t.Config.QPS,
		MinSize:     t.Config.MinSize,
		IncludeExts: strings.Join(t.Config.IncludeExts, ","),
		ExcludeExts: strings.Join(t.Config.ExcludeExts, ","),
		StartedAt:   now,
	}
	if creator := t.GetCreator(); creator != nil {
		taskRow.Creator = creator.Username
		taskRow.CreatorID = creator.ID
	}
	SaveTask(taskRow)

	// 进度上报：定时把原子计数快照同步到任务状态并落库
	reportDone := make(chan struct{})
	var reportWG sync.WaitGroup
	reportWG.Add(1)
	go func() {
		defer reportWG.Done()
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				snap := progress.Snapshot()
				t.mu.Lock()
				t.stats = snap
				t.status = fmt.Sprintf("已扫描 %d 目录 / %d 文件", snap.ScannedDirs, snap.ScannedFiles)
				t.mu.Unlock()
				t.Persist()

				// 定时同步更新 x_dedup_tasks 数据库记录，确保长时间扫描期间 DB 数据始终处于最新状态
				db.GetDb().Model(&DedupTask{}).Where("id = ?", t.GetID()).Updates(map[string]interface{}{
					"scanned_dirs":     snap.ScannedDirs,
					"scanned_files":    snap.ScannedFiles,
					"verified_files":   snap.VerifiedFiles,
					"unverified_files": snap.UnverifiedFiles,
					"failed_dirs":      snap.FailedDirs,
					"dup_groups":       snap.DupGroups,
					"dup_files":        snap.DupFiles,
					"wasted_total":     snap.WastedBytes,
					"candidate_groups": snap.CandidateGroups,
					"candidate_files":  snap.CandidateFiles,
				})
			case <-reportDone:
				return
			}
		}
	}()

	groups, dirStats, err := Scan(ctx, t.Config, progress)
	close(reportDone)
	reportWG.Wait() // 等上报协程退出，避免它在终态统计之后再次覆写状态

	// 扫描结束后重新取快照，保证终态统计包含最后一批目录/文件
	stats := progress.Snapshot()
	t.mu.Lock()
	t.stats = stats
	t.mu.Unlock()

	end := time.Now()
	t.SetEndTime(end)

	state := "finished"
	taskErr := ""
	if err != nil {
		if ctx.Err() != nil || errors.Is(err, context.Canceled) {
			state = "canceled"
			t.SetStatus("任务已取消")
		} else {
			state = "failed"
			taskErr = err.Error()
			t.SetStatus(fmt.Sprintf("扫描失败: %v", err))
		}
	} else {
		t.SetProgress(100)
		t.SetStatus(fmt.Sprintf(
			"发现 %d 组重复 (共 %d 文件)，可释放 %s；另有 %d 组同名同尺寸候选待人工确认",
			stats.DupGroups, stats.DupFiles, formatBytes(stats.WastedBytes), stats.CandidateGroups,
		))
		if stats.UnverifiedFiles > 0 {
			log.Warnf("[dedup] task %s: %d files without storage hash are listed as candidates only",
				t.GetID(), stats.UnverifiedFiles)
		}
		SaveDupFiles(t.GetID(), groups)
		SaveDirStats(t.GetID(), dirStats)
	}

	SaveTask(&DedupTask{
		ID:               t.GetID(),
		RootPath:         t.Config.RootPath,
		State:            state,
		Creator:          taskRow.Creator,
		CreatorID:        taskRow.CreatorID,
		ScannedDirs:      stats.ScannedDirs,
		ScannedFiles:     stats.ScannedFiles,
		VerifiedFiles:    stats.VerifiedFiles,
		UnverifiedFiles:  stats.UnverifiedFiles,
		FailedDirs:       stats.FailedDirs,
		DupGroups:        stats.DupGroups,
		DupFiles:         stats.DupFiles,
		WastedTotal:      stats.WastedBytes,
		InitialDupGroups: stats.DupGroups,
		InitialDupFiles:  stats.DupFiles,
		InitialWasted:    stats.WastedBytes,
		CleanedFiles:     0,
		CleanedBytes:     0,
		MaxDepth:         t.Config.MaxDepth,
		Concurrency:      t.Config.Concurrency,
		QPS:              t.Config.QPS,
		MinSize:          t.Config.MinSize,
		IncludeExts:      strings.Join(t.Config.IncludeExts, ","),
		ExcludeExts:      strings.Join(t.Config.ExcludeExts, ","),
		CandidateGroups:  stats.CandidateGroups,
		CandidateFiles:   stats.CandidateFiles,
		Error:            taskErr,
		StartedAt:        now,
		EndedAt:          &end,
	})

	return err
}

func formatBytes(bytes int64) string {
	if bytes <= 0 {
		return "0 B"
	}
	const k = 1024
	sizes := []string{"B", "KB", "MB", "GB", "TB"}
	i := 0
	val := float64(bytes)
	for val >= k && i < len(sizes)-1 {
		val /= k
		i++
	}
	return fmt.Sprintf("%.2f %s", val, sizes[i])
}

// DedupTaskManager 全局标准任务管理器
var DedupTaskManager task.Manager[*DedupScanTask]

// InitTaskManager 初始化去重任务管理器
func InitTaskManager() {
	DedupTaskManager = tache.NewManager[*DedupScanTask](
		tache.WithWorks(2),
		tache.WithPersistFunction(
			db.GetTaskDataFunc("dedup", true),
			db.UpdateTaskDataFunc("dedup", true),
		),
	)
	// 服务启动时，清理此前进程未正常结束的孤儿任务，避免在数据库中永久处于 running 状态
	db.GetDb().Model(&DedupTask{}).Where("state = ?", "running").Updates(map[string]interface{}{
		"state": "interrupted",
		"error": "服务重启，任务已中断",
	})
	log.Info("[dedup] native task manager initialized")
}

// AddScanTask 创建并提交扫描任务
func AddScanTask(cfg ScanConfig, creator *model.User) (*DedupScanTask, error) {
	if DedupTaskManager == nil {
		return nil, errors.New("去重任务管理器未初始化")
	}
	cfg = normalizeScanConfig(cfg)
	t := &DedupScanTask{
		Name:   fmt.Sprintf("去重: %s", cfg.RootPath),
		Config: cfg,
	}
	if creator != nil {
		t.SetCreator(creator)
	}
	t.SetStatus("等待调度...")
	DedupTaskManager.Add(t)
	return t, nil
}
