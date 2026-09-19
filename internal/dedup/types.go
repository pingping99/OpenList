package dedup

import (
	"sync/atomic"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

// ==================== 数据库模型 ====================

// DedupTask 任务元数据（持久化到 dedup_tasks 表）
type DedupTask struct {
	ID              string     `gorm:"primaryKey;size:36" json:"id"`
	RootPath        string     `gorm:"size:1024" json:"root_path"`
	State           string     `gorm:"index;size:20" json:"state"` // queued, running, finished, failed, canceled, interrupted
	Creator         string     `gorm:"size:255" json:"creator"`
	CreatorID       uint       `gorm:"index" json:"creator_id"`
	ScannedDirs     int64      `json:"scanned_dirs"`
	ScannedFiles    int64      `json:"scanned_files"`
	VerifiedFiles   int64      `json:"verified_files"`   // 有可用哈希、参与内容比对的文件数
	UnverifiedFiles int64      `json:"unverified_files"` // 无可用哈希、仅能按「同名同尺寸」列出的文件数
	FailedDirs      int64      `json:"failed_dirs"`      // 列举失败的目录数
	DupGroups        int        `json:"dup_groups"`       // 已校验的重复组数（可清理）
	DupFiles         int        `json:"dup_files"`        // 已校验的重复文件数
	WastedTotal      int64      `json:"wasted_total"`     // 已校验重复可释放空间
	InitialDupGroups int        `json:"initial_dup_groups"` // 扫描结束时的初始重复组数
	InitialDupFiles  int        `json:"initial_dup_files"`  // 扫描结束时的初始重复文件数
	InitialWasted    int64      `json:"initial_wasted"`     // 扫描结束时的初始可释放空间
	CleanedFiles     int        `json:"cleaned_files"`      // 已清理的副本文件数
	CleanedBytes     int64      `json:"cleaned_bytes"`      // 已清理释放的字节数
	MaxDepth         int        `json:"max_depth"`          // 扫描最大深度
	Concurrency      int        `json:"concurrency"`        // 扫描并发度
	QPS              float64    `json:"qps"`                // 扫描QPS
	MinSize          int64      `json:"min_size"`           // 最小文件大小过滤（字节）
	IncludeExts      string     `gorm:"size:255" json:"include_exts"` // 包含后缀
	ExcludeExts      string     `gorm:"size:255" json:"exclude_exts"` // 排除后缀
	CandidateGroups  int        `json:"candidate_groups"` // 未校验候选组数（不可批量清理）
	CandidateFiles   int        `json:"candidate_files"`  // 未校验候选文件数
	Error            string     `gorm:"type:text" json:"error,omitempty"`
	StartedAt        time.Time  `json:"started_at"`
	EndedAt          *time.Time `json:"ended_at,omitempty"`
}

// DedupFileItem 已校验重复文件 / 未校验候选文件的记录（持久化到 dedup_file_items 表）
type DedupFileItem struct {
	ID       uint      `gorm:"primaryKey;autoIncrement" json:"id"`
	TaskID   string    `gorm:"index:idx_task_group;size:36" json:"task_id"`
	GroupKey string    `gorm:"index:idx_task_group;size:160" json:"group_key"`
	Verified bool      `gorm:"index" json:"verified"`
	HashType string    `gorm:"size:16" json:"hash_type,omitempty"`
	Hash     string    `gorm:"size:128" json:"hash,omitempty"`
	Path     string    `gorm:"type:text" json:"path"`
	Name     string    `gorm:"size:512" json:"name"`
	Size     int64     `json:"size"`
	Modified time.Time `json:"modified"`
}

// ==================== 内存中的轻量结构 ====================

// FileItem 扫描结果中的单个文件
type FileItem struct {
	Path     string    `json:"path"`
	Name     string    `json:"name"`
	Size     int64     `json:"size"`
	HashType string    `json:"hash_type,omitempty"`
	Hash     string    `json:"hash,omitempty"`
	Modified time.Time `json:"modified"`
}

// DupGroup 一组重复文件（Verified=true）或一组待人工确认的候选文件（Verified=false）
type DupGroup struct {
	GroupKey    string     `json:"group_key"`
	HashType    string     `json:"hash_type,omitempty"`
	Hash        string     `json:"hash,omitempty"`
	Size        int64      `json:"size"`
	Verified    bool       `json:"verified"`
	WastedBytes int64      `json:"wasted_bytes"`
	Files       []FileItem `json:"files"`
}

// ScanConfig 扫描配置
type ScanConfig struct {
	RootPath    string   `json:"path"`
	MaxDepth    int      `json:"max_depth"`
	Concurrency int      `json:"concurrency"`
	QPS         float64  `json:"qps"`
	MinSize     int64    `json:"min_size"`     // 最小文件大小过滤（字节），<=0 表示不过滤
	IncludeExts []string `json:"include_exts"` // 仅包含的扩展名（小写，不带点，空表示不过滤）
	ExcludeExts []string `json:"exclude_exts"` // 排除的扩展名（小写，不带点）
}

// ==================== 任务进度 ====================

// ScanStats 进度快照，可安全跨 goroutine 传递
type ScanStats struct {
	ScannedDirs     int64 `json:"scanned_dirs"`
	ScannedFiles    int64 `json:"scanned_files"`
	VerifiedFiles   int64 `json:"verified_files"`
	UnverifiedFiles int64 `json:"unverified_files"`
	FailedDirs      int64 `json:"failed_dirs"`
	DupGroups       int   `json:"dup_groups"`
	DupFiles        int   `json:"dup_files"`
	WastedBytes     int64 `json:"wasted_bytes"`
	CandidateGroups int   `json:"candidate_groups"`
	CandidateFiles  int   `json:"candidate_files"`
}

// Progress 扫描进度计数器。全部使用原子操作，供后台任务与 HTTP 接口并发读取，
// 避免旧实现中「进度协程写结构体字段、接口无锁读同一字段」的数据竞争。
type Progress struct {
	scannedDirs     atomic.Int64
	scannedFiles    atomic.Int64
	verifiedFiles   atomic.Int64
	unverifiedFiles atomic.Int64
	failedDirs      atomic.Int64
	dupGroups       atomic.Int64
	dupFiles        atomic.Int64
	wastedBytes     atomic.Int64
	candidateGroups atomic.Int64
	candidateFiles  atomic.Int64
}

// Snapshot 返回当前进度的只读快照
func (p *Progress) Snapshot() ScanStats {
	return ScanStats{
		ScannedDirs:     p.scannedDirs.Load(),
		ScannedFiles:    p.scannedFiles.Load(),
		VerifiedFiles:   p.verifiedFiles.Load(),
		UnverifiedFiles: p.unverifiedFiles.Load(),
		FailedDirs:      p.failedDirs.Load(),
		DupGroups:       int(p.dupGroups.Load()),
		DupFiles:        int(p.dupFiles.Load()),
		WastedBytes:     p.wastedBytes.Load(),
		CandidateGroups: int(p.candidateGroups.Load()),
		CandidateFiles:  int(p.candidateFiles.Load()),
	}
}

// Store 用最终结果覆盖计数器（仅用于扫描结束阶段）
func (p *Progress) Store(stats ScanStats) {
	p.scannedDirs.Store(stats.ScannedDirs)
	p.scannedFiles.Store(stats.ScannedFiles)
	p.verifiedFiles.Store(stats.VerifiedFiles)
	p.unverifiedFiles.Store(stats.UnverifiedFiles)
	p.failedDirs.Store(stats.FailedDirs)
	p.dupGroups.Store(int64(stats.DupGroups))
	p.dupFiles.Store(int64(stats.DupFiles))
	p.wastedBytes.Store(stats.WastedBytes)
	p.candidateGroups.Store(int64(stats.CandidateGroups))
	p.candidateFiles.Store(int64(stats.CandidateFiles))
}

// ==================== 数据库操作 ====================

// Init 由 bootstrap 在数据库就绪后调用：建表 + 标记上次进程遗留的 running 任务 + 初始化任务管理器。
// 旧实现把前两步放在 RegisterRouter 里，路由注册与数据初始化耦合在一起。
func Init() {
	InitDB()
	MarkInterruptedTasks()
	InitTaskManager()
}

// InitDB 自动迁移去重模块的数据库表，并清理旧版本遗留的无效结果行
func InitDB() {
	d := db.GetDb()
	if err := d.AutoMigrate(&DedupTask{}, &DedupFileItem{}); err != nil {
		log.Errorf("[dedup] failed to migrate database: %v", err)
		return
	}
	// 旧版本没有 group_key / verified 语义，这些行无法再参与分组，直接清掉
	// （旧任务的结果会在重新扫描后重建，历史统计不受影响）
	result := d.Where("group_key = '' OR group_key IS NULL").Delete(&DedupFileItem{})
	if result.RowsAffected > 0 {
		log.Infof("[dedup] dropped %d legacy result rows without group key", result.RowsAffected)
	}
}

// SaveTask 保存或更新任务元数据到数据库
func SaveTask(task *DedupTask) {
	if err := db.GetDb().Save(task).Error; err != nil {
		log.Errorf("[dedup] failed to save task %s: %v", task.ID, err)
	}
}

// GetTaskByID 按 ID 读取任务元数据
func GetTaskByID(id string) (*DedupTask, error) {
	var task DedupTask
	if err := db.GetDb().Where("id = ?", id).First(&task).Error; err != nil {
		return nil, err
	}
	return &task, nil
}

// SaveDupFiles 将扫描结果写入数据库：同一事务内先清旧数据再分批写入，避免中途失败留下脏数据
func SaveDupFiles(taskID string, groups []DupGroup) {
	err := db.GetDb().Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("task_id = ?", taskID).Delete(&DedupFileItem{}).Error; err != nil {
			return err
		}
		batch := make([]DedupFileItem, 0, 500)
		flush := func() error {
			if len(batch) == 0 {
				return nil
			}
			if err := tx.CreateInBatches(batch, len(batch)).Error; err != nil {
				return err
			}
			batch = batch[:0]
			return nil
		}
		for _, g := range groups {
			for _, f := range g.Files {
				batch = append(batch, DedupFileItem{
					TaskID:   taskID,
					GroupKey: g.GroupKey,
					Verified: g.Verified,
					HashType: f.HashType,
					Hash:     f.Hash,
					Path:     f.Path,
					Name:     f.Name,
					Size:     f.Size,
					Modified: f.Modified,
				})
			}
			if len(batch) >= 500 {
				if err := flush(); err != nil {
					return err
				}
			}
		}
		return flush()
	})
	if err != nil {
		log.Errorf("[dedup] failed to save dup files of task %s: %v", taskID, err)
	}
}

// MarkInterruptedTasks 启动时将数据库中所有 state="running" 的任务标记为 "interrupted"
func MarkInterruptedTasks() {
	d := db.GetDb()
	now := time.Now()
	result := d.Model(&DedupTask{}).
		Where("state = ?", "running").
		Updates(map[string]interface{}{
			"state":    "interrupted",
			"error":    "process restarted, task was interrupted",
			"ended_at": &now,
		})
	if result.RowsAffected > 0 {
		log.Infof("[dedup] marked %d interrupted tasks", result.RowsAffected)
	}
}

// GroupMembers 读取指定分组在库内的全部成员路径，键为 group_key。
// 用于清理前判断「这一组还剩几份」，避免把一组重复文件删得一份不剩。
func GroupMembers(taskID string, groupKeys []string) (map[string][]string, error) {
	members := make(map[string][]string, len(groupKeys))
	if len(groupKeys) == 0 {
		return members, nil
	}
	var items []DedupFileItem
	if err := db.GetDb().
		Select("group_key, path").
		Where("task_id = ? AND group_key IN ?", taskID, groupKeys).
		Find(&items).Error; err != nil {
		return nil, err
	}
	for _, item := range items {
		members[item.GroupKey] = append(members[item.GroupKey], item.Path)
	}
	return members, nil
}

// DeleteFileItems 从结果明细中移除已成功删除的文件
func DeleteFileItems(taskID string, paths []string) error {
	if len(paths) == 0 {
		return nil
	}
	return db.GetDb().
		Where("task_id = ? AND path IN ?", taskID, paths).
		Delete(&DedupFileItem{}).Error
}

// DropDanglingGroups 清掉清理后只剩 1 个成员的分组（已不构成重复）
func DropDanglingGroups(taskID string) error {
	d := db.GetDb()
	var keys []string
	if err := d.Model(&DedupFileItem{}).
		Select("group_key").
		Where("task_id = ?", taskID).
		Group("group_key").
		Having("COUNT(*) < 2").
		Pluck("group_key", &keys).Error; err != nil {
		return err
	}
	if len(keys) == 0 {
		return nil
	}
	return d.Where("task_id = ? AND group_key IN ?", taskID, keys).
		Delete(&DedupFileItem{}).Error
}

// RecomputeTaskCounters 依据当前结果明细重算任务统计并落库。
// 清理文件后组数/文件数/可释放空间都会变化，若不重算，历史里的数字会一直停留在扫描时点。
func RecomputeTaskCounters(taskID string) {
	task, err := GetTaskByID(taskID)
	if err != nil {
		log.Errorf("[dedup] recompute counters failed, task %s: %v", taskID, err)
		return
	}
	stats := ScanStats{
		ScannedDirs:     task.ScannedDirs,
		ScannedFiles:    task.ScannedFiles,
		VerifiedFiles:   task.VerifiedFiles,
		UnverifiedFiles: task.UnverifiedFiles,
		FailedDirs:      task.FailedDirs,
	}
	for _, verified := range []bool{true, false} {
		var aggs []struct {
			GroupKey string
			Count    int64
			MaxSize  int64
		}
		if err := db.GetDb().Model(&DedupFileItem{}).
			Select("group_key, COUNT(*) AS count, MAX(size) AS max_size").
			Where("task_id = ? AND verified = ?", taskID, verified).
			Group("group_key").
			Find(&aggs).Error; err != nil {
			log.Errorf("[dedup] recompute counters failed, task %s: %v", taskID, err)
			return
		}
		var groups, files int
		var wasted int64
		for _, a := range aggs {
			if a.Count < 2 {
				continue
			}
			groups++
			files += int(a.Count)
			wasted += a.MaxSize * (a.Count - 1)
		}
		if verified {
			stats.DupGroups, stats.DupFiles, stats.WastedBytes = groups, files, wasted
		} else {
			stats.CandidateGroups, stats.CandidateFiles = groups, files
		}
	}
	task.DupGroups = stats.DupGroups
	task.DupFiles = stats.DupFiles
	task.WastedTotal = stats.WastedBytes
	task.CandidateGroups = stats.CandidateGroups
	task.CandidateFiles = stats.CandidateFiles
	if task.InitialDupFiles == 0 && task.DupFiles > 0 {
		task.InitialDupGroups = task.DupGroups
		task.InitialDupFiles = task.DupFiles
		task.InitialWasted = task.WastedTotal
	}
	if task.InitialDupFiles > 0 && task.InitialDupFiles >= task.DupFiles {
		task.CleanedFiles = task.InitialDupFiles - task.DupFiles
	}
	if task.InitialWasted > 0 && task.InitialWasted >= task.WastedTotal {
		task.CleanedBytes = task.InitialWasted - task.WastedTotal
	}
	SaveTask(task)
}
