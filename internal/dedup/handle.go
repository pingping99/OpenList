package dedup

import (
	"fmt"
	"net/http"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/fs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
)

type StartReq struct {
	Path        string  `json:"path"`
	MaxDepth    int     `json:"max_depth"`
	Concurrency int     `json:"concurrency"`
	QPS         float64 `json:"qps"`
}

type RemoveReq struct {
	TaskID           string   `json:"task_id"`
	Paths            []string `json:"paths"`
	DeleteCompanions bool     `json:"delete_companions"`
	RemoveEmptyDirs  bool     `json:"remove_empty_dirs"`
}

// HandleStartScan 启动异步查重任务 (原生 tache 架构)
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

	cfg := ScanConfig{
		RootPath:    req.Path,
		MaxDepth:    req.MaxDepth,
		Concurrency: req.Concurrency,
		QPS:         req.QPS,
	}

	var creator *model.User
	if u, ok := c.Request.Context().Value(conf.UserKey).(*model.User); ok {
		creator = u
	}

	t, err := AddScanTask(cfg, creator)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"task_id": t.GetID(),
		"status":  "started",
	})
}

// HandleGetStatus 查询任务状态及进度
func HandleGetStatus(c *gin.Context) {
	taskID := c.Query("task_id")

	// 1. 优先从原生 tache 管理器读取
	if DedupTaskManager != nil {
		if t, ok := DedupTaskManager.GetByID(taskID); ok {
			c.JSON(http.StatusOK, gin.H{
				"id":            t.GetID(),
				"root_path":     t.Config.RootPath,
				"state":         int(t.GetState()),
				"status":        t.GetStatus(),
				"progress":      t.GetProgress(),
				"scanned_dirs":  t.ScannedDirs,
				"scanned_files": t.ScannedFiles,
				"dup_groups":    t.DupGroups,
				"dup_files":     t.DupFiles,
				"wasted_total":  t.WastedBytes,
				"start_time":    t.GetStartTime(),
				"end_time":      t.GetEndTime(),
			})
			return
		}
	}

	// 2. 从旧内存缓存读取
	tCtx, ok := Tasks.Get(taskID)
	if ok {
		tCtx.mu.Lock()
		task := tCtx.Task
		tCtx.mu.Unlock()
		c.JSON(http.StatusOK, task)
		return
	}

	// 3. 从数据库读取已结束的任务
	var task DedupTask
	if err := db.GetDb().Where("id = ?", taskID).First(&task).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "task not found"})
		return
	}
	c.JSON(http.StatusOK, task)
}

// HandleCancelScan 终止任务
func HandleCancelScan(c *gin.Context) {
	taskID := c.Query("task_id")
	if taskID == "" {
		var body struct {
			TaskID string `json:"task_id"`
		}
		if err := c.ShouldBindJSON(&body); err == nil && body.TaskID != "" {
			taskID = body.TaskID
		}
	}

	// 优先使用原生 tache Manager 取消
	if DedupTaskManager != nil {
		DedupTaskManager.Cancel(taskID)
	}

	// 兼容旧内存任务
	tCtx, ok := Tasks.Get(taskID)
	if ok && tCtx.Cancel != nil {
		tCtx.Cancel()
	}

	c.JSON(http.StatusOK, gin.H{"status": "canceling"})
}

// HandleGetResult 分页获取任务的重复文件结果
func HandleGetResult(c *gin.Context) {
	taskID := c.Query("task_id")
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	size, _ := strconv.Atoi(c.DefaultQuery("size", "20"))
	if page < 1 {
		page = 1
	}
	if size < 1 || size > 200 {
		size = 20
	}

	// 先查重复的 hash 列表（分页是在 hash 组级别）
	var hashes []string
	d := db.GetDb()
	d.Model(&DedupFileItem{}).
		Select("hash").
		Where("task_id = ?", taskID).
		Group("hash").
		Having("COUNT(*) > 1").
		Order("MAX(size) * (COUNT(*) - 1) DESC").
		Offset((page - 1) * size).
		Limit(size).
		Pluck("hash", &hashes)

	// 统计总组数
	var totalGroups int64
	d.Model(&DedupFileItem{}).
		Select("hash").
		Where("task_id = ?", taskID).
		Group("hash").
		Having("COUNT(*) > 1").
		Count(&totalGroups)

	if len(hashes) == 0 {
		c.JSON(http.StatusOK, gin.H{
			"total":  totalGroups,
			"page":   page,
			"size":   size,
			"groups": []DupGroup{},
		})
		return
	}

	// 查出这些 hash 下的所有文件
	var items []DedupFileItem
	d.Where("task_id = ? AND hash IN ?", taskID, hashes).
		Order("hash, modified ASC").
		Find(&items)

	// 组装成 DupGroup
	groupMap := make(map[string]*DupGroup)
	var groupOrder []string
	for _, item := range items {
		g, ok := groupMap[item.Hash]
		if !ok {
			g = &DupGroup{
				Hash: item.Hash,
				Size: item.Size,
			}
			groupMap[item.Hash] = g
			groupOrder = append(groupOrder, item.Hash)
		}
		g.Files = append(g.Files, FileItem{
			Path:     item.Path,
			Name:     item.Name,
			Size:     item.Size,
			Hash:     item.Hash,
			Modified: item.Modified,
		})
	}

	var groups []DupGroup
	for _, h := range groupOrder {
		g := groupMap[h]
		g.WastedBytes = g.Size * int64(len(g.Files)-1)
		groups = append(groups, *g)
	}

	c.JSON(http.StatusOK, gin.H{
		"total":  totalGroups,
		"page":   page,
		"size":   size,
		"groups": groups,
	})
}

// HandleListHistory 列出所有历史任务
func HandleListHistory(c *gin.Context) {
	var tasks []DedupTask
	if err := db.GetDb().Order("started_at DESC").Find(&tasks).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, tasks)
}

// HandleGetHistoryDetail 获取单个历史任务详情
func HandleGetHistoryDetail(c *gin.Context) {
	taskID := c.Param("id")
	var task DedupTask
	if err := db.GetDb().Where("id = ?", taskID).First(&task).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "task not found"})
		return
	}
	c.JSON(http.StatusOK, task)
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

	toDeleteSet := make(map[string]bool)
	for _, p := range req.Paths {
		toDeleteSet[p] = true
	}

	if req.DeleteCompanions {
		dirToFiles := make(map[string][]string)
		for _, p := range req.Paths {
			dir := path.Dir(p)
			dirToFiles[dir] = append(dirToFiles[dir], p)
		}

		for dir, files := range dirToFiles {
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

	successPaths := make([]string, 0, len(toDeleteSet))
	affectedDirsMap := make(map[string]bool)
	for p := range toDeleteSet {
		err := fs.Remove(c.Request.Context(), p)
		if err != nil {
			log.Errorf("[dedup] failed to remove file %s: %v", p, err)
			errMsgs = append(errMsgs, fmt.Sprintf("%s: %v", p, err))
		} else {
			successCount++
			successPaths = append(successPaths, p)
			if req.RemoveEmptyDirs {
				d := path.Dir(p)
				for d != "" && d != "/" && d != "." {
					affectedDirsMap[d] = true
					d = path.Dir(d)
				}
			}
		}
	}

	// 从本地数据库清理已成功删除的条目，保证重新查询时不会再次出现
	if len(successPaths) > 0 {
		dbQuery := db.GetDb().Where("path IN ?", successPaths)
		if req.TaskID != "" {
			dbQuery = dbQuery.Where("task_id = ?", req.TaskID)
		}
		if err := dbQuery.Delete(&DedupFileItem{}).Error; err != nil {
			log.Errorf("[dedup] failed to delete cleaned files from DB: %v", err)
		}
	}

	emptyDirsRemoved := 0
	if req.RemoveEmptyDirs && len(affectedDirsMap) > 0 {
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
