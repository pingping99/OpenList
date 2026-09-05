package dedup

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

// FileItem 轻量文件元数据
type FileItem struct {
	Path     string    `json:"path"`
	Name     string    `json:"name"`
	Size     int64     `json:"size"`
	Hash     string    `json:"hash"`
	Modified time.Time `json:"modified"`
}

// DupGroup 重复文件组
type DupGroup struct {
	Hash        string     `json:"hash"`
	Size        int64      `json:"size"`
	WastedBytes int64      `json:"wasted_bytes"` // 冗余空间 = Size * (Count - 1)
	Files       []FileItem `json:"files"`
}

// ScanConfig 扫描配置
type ScanConfig struct {
	RootPath    string  `json:"path"`
	MaxDepth    int     `json:"max_depth"`   // 最大向下递归深度
	Concurrency int     `json:"concurrency"` // 并发工作协程数 (建议 2-4)
	QPS         float64 `json:"qps"`         // 每秒请求频率限制 (令牌桶)
}

// TaskStatus 任务运行状态与统计
type TaskStatus struct {
	ID           string     `json:"id"`
	RootPath     string     `json:"root_path"`
	State        string     `json:"state"` // running, finished, failed, canceled
	ScannedDirs  int64      `json:"scanned_dirs"`
	ScannedFiles int64      `json:"scanned_files"`
	DupGroups    int        `json:"dup_groups"`
	WastedTotal  int64      `json:"wasted_total"`
	StartedAt    time.Time  `json:"started_at"`
	EndedAt      *time.Time `json:"ended_at,omitempty"`
	Error        string     `json:"error,omitempty"`
	Result       []DupGroup `json:"result,omitempty"`
}

type TaskContext struct {
	Status TaskStatus
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

func persistTask(t *TaskContext) {
	t.mu.Lock()
	data, err := json.Marshal(t.Status)
	t.mu.Unlock()
	if err != nil {
		return
	}
	item := &model.TaskItem{Key: "dedup:" + t.Status.ID, PersistData: string(data)}
	if err := db.CreateTaskData(item); err != nil {
		_ = db.UpdateTaskData(item)
	}
}
