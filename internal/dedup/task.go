package dedup

import (
	"context"
	"fmt"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/task"
	"github.com/OpenListTeam/tache"
	log "github.com/sirupsen/logrus"
)

// DedupScanTask 遵循 OpenList 官方 tache 规范的标准去重扫描任务
type DedupScanTask struct {
	task.TaskExtension
	Name         string     `json:"name"`
	Status       string     `json:"-"`
	Progress     float64    `json:"-"`
	Config       ScanConfig `json:"config"`
	ScannedDirs  int64      `json:"scanned_dirs"`
	ScannedFiles int64      `json:"scanned_files"`
	DupGroups    int        `json:"dup_groups"`
	DupFiles     int        `json:"dup_files"`
	WastedBytes  int64      `json:"wasted_bytes"`
}

var _ task.TaskExtensionInfo = (*DedupScanTask)(nil)

func (t *DedupScanTask) GetName() string {
	if t.Name != "" {
		return t.Name
	}
	return fmt.Sprintf("去重扫描: %s", t.Config.RootPath)
}

func (t *DedupScanTask) GetStatus() string {
	return t.Status
}

func (t *DedupScanTask) SetStatus(s string) {
	t.Status = s
}

func (t *DedupScanTask) GetProgress() float64 {
	return t.Progress
}

func (t *DedupScanTask) SetProgress(p float64) {
	t.Progress = p
}

// Run 执行具体的扫描逻辑 (tache 框架调度入口)
func (t *DedupScanTask) Run() error {
	ctx := t.Ctx()
	if ctx == nil {
		ctx = context.Background()
	}

	now := time.Now()
	t.SetStartTime(now)

	SaveTask(&DedupTask{
		ID:        t.GetID(),
		RootPath:  t.Config.RootPath,
		State:     "running",
		StartedAt: now,
	})

	tCtx := &TaskContext{
		Task: DedupTask{
			ID:        t.GetID(),
			RootPath:  t.Config.RootPath,
			State:     "running",
			StartedAt: now,
		},
		Cancel: func() {
			t.Cancel()
		},
	}

	progressStop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				tCtx.mu.Lock()
				t.ScannedDirs = tCtx.Task.ScannedDirs
				t.ScannedFiles = tCtx.Task.ScannedFiles
				t.SetStatus(fmt.Sprintf("已扫描 %d 目录 / %d 文件", t.ScannedDirs, t.ScannedFiles))
				tCtx.mu.Unlock()
				t.Persist()
			case <-progressStop:
				return
			}
		}
	}()

	groups, err := ScanWithWorkerPool(ctx, t.Config, tCtx)
	close(progressStop)

	end := time.Now()
	t.SetEndTime(end)

	if err != nil {
		if ctx.Err() != nil {
			t.SetStatus("任务已取消")
			SaveTask(&DedupTask{
				ID:           t.GetID(),
				RootPath:     t.Config.RootPath,
				State:        "canceled",
				ScannedDirs:  t.ScannedDirs,
				ScannedFiles: t.ScannedFiles,
				StartedAt:    now,
				EndedAt:      &end,
			})
		} else {
			t.SetStatus(fmt.Sprintf("扫描失败: %v", err))
			SaveTask(&DedupTask{
				ID:           t.GetID(),
				RootPath:     t.Config.RootPath,
				State:        "failed",
				Error:        err.Error(),
				ScannedDirs:  t.ScannedDirs,
				ScannedFiles: t.ScannedFiles,
				StartedAt:    now,
				EndedAt:      &end,
			})
		}
		return err
	}

	t.DupGroups = len(groups)
	var dupFiles int
	var totalWasted int64
	for _, g := range groups {
		dupFiles += len(g.Files)
		totalWasted += g.WastedBytes
	}
	t.DupFiles = dupFiles
	t.WastedBytes = totalWasted
	t.SetTotalBytes(totalWasted)
	t.SetProgress(100)
	t.SetStatus(fmt.Sprintf("发现 %d 组重复 (共 %d 文件)，可释放 %s", t.DupGroups, t.DupFiles, formatBytes(totalWasted)))

	SaveDupFiles(t.GetID(), groups)

	SaveTask(&DedupTask{
		ID:           t.GetID(),
		RootPath:     t.Config.RootPath,
		State:        "finished",
		ScannedDirs:  t.ScannedDirs,
		ScannedFiles: t.ScannedFiles,
		DupGroups:    t.DupGroups,
		DupFiles:     t.DupFiles,
		WastedTotal:  totalWasted,
		StartedAt:    now,
		EndedAt:      &end,
	})

	return nil
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
	log.Info("[dedup] native task manager initialized")
}

// AddScanTask 创建并提交扫描任务
func AddScanTask(cfg ScanConfig, creator *model.User) (*DedupScanTask, error) {
	t := &DedupScanTask{
		Config: cfg,
		Name:   fmt.Sprintf("去重: %s", cfg.RootPath),
	}
	if creator != nil {
		t.SetCreator(creator)
	}
	t.SetStatus("等待调度...")
	DedupTaskManager.Add(t)
	return t, nil
}
