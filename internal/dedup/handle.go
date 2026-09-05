package dedup

import (
	"context"
	"fmt"
	"net/http"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/fs"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type StartReq struct {
	Path        string  `json:"path"`
	MaxDepth    int     `json:"max_depth"`
	Concurrency int     `json:"concurrency"`
	QPS         float64 `json:"qps"`
}

type RemoveReq struct {
	Paths            []string `json:"paths"`
	DeleteCompanions bool     `json:"delete_companions"` // 是否连带删除同名附属文件 (.srt/.nfo/.jpg 等)
	RemoveEmptyDirs  bool     `json:"remove_empty_dirs"` // 清理后是否自动移除空文件夹
}

// HandleStartScan 启动异步查重任务
func HandleStartScan(c *gin.Context) {
	var req StartReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if req.Path == "" {
		req.Path = "/"
	}
	if req.Concurrency <= 0 {
		req.Concurrency = 3
	}
	if req.QPS <= 0 {
		req.QPS = 2.5
	}
	if req.MaxDepth <= 0 {
		req.MaxDepth = 30
	}

	taskID := uuid.NewString()
	ctx, cancel := context.WithCancel(context.Background())

	tCtx := &TaskContext{
		Status: TaskStatus{
			ID:        taskID,
			RootPath:  req.Path,
			State:     "running",
			StartedAt: time.Now(),
		},
		Cancel: cancel,
	}

	Tasks.Set(taskID, tCtx)

	cfg := ScanConfig{
		RootPath:    req.Path,
		MaxDepth:    req.MaxDepth,
		Concurrency: req.Concurrency,
		QPS:         req.QPS,
	}

	go func() {
		result, err := ScanWithWorkerPool(ctx, cfg, tCtx)
		now := time.Now()
		tCtx.mu.Lock()
		defer tCtx.mu.Unlock()

		tCtx.Status.EndedAt = &now
		if err != nil {
			if err == context.Canceled {
				tCtx.Status.State = "canceled"
			} else {
				tCtx.Status.State = "failed"
				tCtx.Status.Error = err.Error()
			}
		} else {
			tCtx.Status.State = "finished"
			tCtx.Status.Result = result
		}
		persistTask(tCtx)
	}()

	c.JSON(http.StatusOK, gin.H{
		"task_id": taskID,
		"status":  "started",
	})
}

// HandleGetStatus 查询任务状态及进度
func HandleGetStatus(c *gin.Context) {
	taskID := c.Query("task_id")
	tCtx, ok := Tasks.Get(taskID)
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "task not found"})
		return
	}

	tCtx.mu.Lock()
	statusCopy := tCtx.Status
	tCtx.mu.Unlock()

	c.JSON(http.StatusOK, statusCopy)
}

// HandleCancelScan 终止任务
func HandleCancelScan(c *gin.Context) {
	taskID := c.Query("task_id")
	tCtx, ok := Tasks.Get(taskID)
	if ok && tCtx.Cancel != nil {
		tCtx.Cancel()
	}
	c.JSON(http.StatusOK, gin.H{"status": "canceling"})
}

// 常见媒体附属文件后缀
var companionExts = map[string]bool{
	".srt": true, ".ass": true, ".ssa": true, ".sub": true, ".idx": true,
	".nfo": true, ".jpg": true, ".jpeg": true, ".png": true, ".webp": true,
	".xml": true,
}

// HandleBatchRemove 批量清理重复文件（可选连带删除附属文件）
func HandleBatchRemove(c *gin.Context) {
	var req RemoveReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	successCount := 0
	companionRemoved := 0
	var errMsgs []string

	// 用 map 记录已删除或待删除的文件，避免重复删
	toDeleteSet := make(map[string]bool)
	for _, p := range req.Paths {
		toDeleteSet[p] = true
	}

	// 若开启了联动删除附属文件
	if req.DeleteCompanions {
		// 按目录分组待删视频
		dirToFiles := make(map[string][]string)
		for _, p := range req.Paths {
			dir := path.Dir(p)
			dirToFiles[dir] = append(dirToFiles[dir], p)
		}

		for dir, files := range dirToFiles {
			// 列出同目录下的文件
			objs, err := fs.List(c.Request.Context(), dir, &fs.ListArgs{})
			if err != nil {
				continue
			}

			for _, videoPath := range files {
				videoName := path.Base(videoPath)
				ext := filepath.Ext(videoName)
				baseName := strings.TrimSuffix(videoName, ext)

				for _, obj := range objs {
					if obj.IsDir() {
						continue
					}
					objName := obj.GetName()
					objExt := strings.ToLower(filepath.Ext(objName))

					// 检查是否为同名附属文件 (如 movie.zh.srt 或 movie-poster.jpg)
					if companionExts[objExt] {
						if strings.HasPrefix(objName, baseName) {
							fullCompanionPath := path.Join(dir, objName)
							if !toDeleteSet[fullCompanionPath] {
								toDeleteSet[fullCompanionPath] = true
								companionRemoved++
							}
						}
					}
				}
			}
		}
	}

	// 统一执行删除
	affectedDirsMap := make(map[string]bool)
	for p := range toDeleteSet {
		err := fs.Remove(c.Request.Context(), p)
		if err != nil {
			log.Errorf("[dedup] failed to remove file %s: %v", p, err)
			errMsgs = append(errMsgs, fmt.Sprintf("%s: %v", p, err))
		} else {
			successCount++
			if req.RemoveEmptyDirs {
				// 收集受影响的目录
				d := path.Dir(p)
				for d != "" && d != "/" && d != "." {
					affectedDirsMap[d] = true
					d = path.Dir(d)
				}
			}
		}
	}

	emptyDirsRemoved := 0
	// 若启用了自动移除空文件夹
	if req.RemoveEmptyDirs && len(affectedDirsMap) > 0 {
		// 按路径深度（长度）从深到浅排序，确保优先检查并删除底层空目录
		var sortedDirs []string
		for d := range affectedDirsMap {
			sortedDirs = append(sortedDirs, d)
		}
		for i := 0; i < len(sortedDirs)-1; i++ {
			for j := i + 1; j < len(sortedDirs); j++ {
				if len(sortedDirs[i]) < len(sortedDirs[j]) {
					sortedDirs[i], sortedDirs[j] = sortedDirs[j], sortedDirs[i]
				}
			}
		}

		for _, dir := range sortedDirs {
			subObjs, err := fs.List(c.Request.Context(), dir, &fs.ListArgs{Refresh: true})
			if err == nil && len(subObjs) == 0 {
				if err := fs.Remove(c.Request.Context(), dir); err == nil {
					emptyDirsRemoved++
					log.Infof("[dedup] removed empty directory: %s", dir)
				}
			}
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"success_count":      successCount,
		"companion_removed":  companionRemoved,
		"empty_dirs_removed": emptyDirsRemoved,
		"failed_count":       len(errMsgs),
		"errors":             errMsgs,
	})
}

// HandleListDirs 获取指定目录下的子文件夹（用于 UI 快速选目录）
func HandleListDirs(c *gin.Context) {
	dirPath := c.DefaultQuery("path", "/")
	objs, err := fs.List(c.Request.Context(), dirPath, &fs.ListArgs{})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	var dirs []gin.H
	for _, obj := range objs {
		if obj.IsDir() {
			dirs = append(dirs, gin.H{
				"name": obj.GetName(),
				"path": path.Join(dirPath, obj.GetName()),
			})
		}
	}
	c.JSON(http.StatusOK, dirs)
}
