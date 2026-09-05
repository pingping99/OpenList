package dedup

import (
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	log "github.com/sirupsen/logrus"
)

// ==================== 数据库模型 ====================

// DedupTask 任务元数据（持久化到 dedup_tasks 表）
type DedupTask struct {
	ID           string     `gorm:"primaryKey;size:36" json:"id"`
	RootPath     string     `gorm:"size:1024" json:"root_path"`
	State        string     `gorm:"index;size:20" json:"state"` // running, finished, failed, canceled, interrupted
	ScannedDirs  int64      `json:"scanned_dirs"`
	ScannedFiles int64      `json:"scanned_files"`
	DupGroups    int        `json:"dup_groups"`
	DupFiles     int        `json:"dup_files"`
	WastedTotal  int64      `json:"wasted_total"`
	Error        string     `gorm:"type:text" json:"error,omitempty"`
	StartedAt    time.Time  `json:"started_at"`
	EndedAt      *time.Time `json:"ended_at,omitempty"`
}

// DedupFileItem 重复文件记录（持久化到 dedup_file_items 表）
type DedupFileItem struct {
	ID       uint      `gorm:"primaryKey;autoIncrement" json:"id"`
	TaskID   string    `gorm:"index:idx_task_hash;size:36" json:"task_id"`
	Hash     string    `gorm:"index:idx_task_hash;size:64" json:"hash"`
	Path     string    `gorm:"type:text" json:"path"`
	Name     string    `gorm:"size:512" json:"name"`
	Size     int64     `json:"size"`
	Modified time.Time `json:"modified"`
}

// ==================== 内存中的轻量结构 ====================

// FileItem 扫描阶段的内存文件元数据
type FileItem struct {
	Path     string    `json:"path"`
	Name     string    `json:"name"`
	Size     int64     `json:"size"`
	Hash     string    `json:"hash"`
	Modified time.Time `json:"modified"`
}

// DupGroup 重复文件组（内存 + API 响应）
type DupGroup struct {
	Hash        string     `json:"hash"`
	Size        int64      `json:"size"`
	WastedBytes int64      `json:"wasted_bytes"`
	Files       []FileItem `json:"files"`
}

// ScanConfig 扫描配置
type ScanConfig struct {
	RootPath    string  `json:"path"`
	MaxDepth    int     `json:"max_depth"`
	Concurrency int     `json:"concurrency"`
	QPS         float64 `json:"qps"`
}

// ==================== 运行时任务管理 ====================

// TaskContext 运行中任务的内存上下文
type TaskContext struct {
	Task   DedupTask
	Cancel func()
	mu     sync.Mutex
}

type TaskManager struct {
	mu    sync.RWMutex
	tasks map[string]*TaskContext
}

var Tasks = &TaskManager{tasks: make(map[string]*TaskContext)}

func (m *TaskManager) Get(id string) (*TaskContext, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	t, ok := m.tasks[id]
	return t, ok
}

func (m *TaskManager) Set(id string, t *TaskContext) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tasks[id] = t
}

func (m *TaskManager) Delete(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.tasks, id)
}

// ==================== 数据库操作 ====================

// InitDB 自动迁移去重模块的数据库表
func InitDB() {
	if err := db.GetDb().AutoMigrate(&DedupTask{}, &DedupFileItem{}); err != nil {
		log.Errorf("[dedup] failed to migrate database: %v", err)
	}
}

// SaveTask 保存或更新任务元数据到数据库
func SaveTask(task *DedupTask) {
	d := db.GetDb()
	if err := d.Save(task).Error; err != nil {
		log.Errorf("[dedup] failed to save task %s: %v", task.ID, err)
	}
}

// SaveDupFiles 将重复文件扫描结果批量写入数据库
func SaveDupFiles(taskID string, groups []DupGroup) {
	d := db.GetDb()
	// 先清理旧数据
	d.Where("task_id = ?", taskID).Delete(&DedupFileItem{})

	var items []DedupFileItem
	for _, g := range groups {
		for _, f := range g.Files {
			items = append(items, DedupFileItem{
				TaskID:   taskID,
				Hash:     f.Hash,
				Path:     f.Path,
				Name:     f.Name,
				Size:     f.Size,
				Modified: f.Modified,
			})
		}
	}
	if len(items) == 0 {
		return
	}
	// 分批插入，每批 500 条，避免内存暴涨
	const batchSize = 500
	for i := 0; i < len(items); i += batchSize {
		end := i + batchSize
		if end > len(items) {
			end = len(items)
		}
		if err := d.CreateInBatches(items[i:end], batchSize).Error; err != nil {
			log.Errorf("[dedup] failed to save dup files batch: %v", err)
		}
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
