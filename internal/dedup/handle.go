package dedup

import (
	"context"
	"fmt"
	"net/http"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/fs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/OpenListTeam/OpenList/v4/server/common"
	"github.com/OpenListTeam/tache"
	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
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

// removeRejection 描述一条被拒绝的删除请求，便于前端解释原因
type removeRejection struct {
	Path   string `json:"path"`
	Reason string `json:"reason"`
}

// ==================== 公共辅助 ====================

func currentUser(c *gin.Context) *model.User {
	user, _ := c.Request.Context().Value(conf.UserKey).(*model.User)
	return user
}

// canManageTask 判定当前用户是否有权操作该任务：管理员可以管理所有任务，
// 普通用户只能管理自己发起的任务（避免跨用户看到/删除他人路径）
func canManageTask(user *model.User, task *DedupTask) bool {
	if task == nil {
		return false
	}
	if user == nil {
		return false
	}
	if user.IsAdmin() {
		return true
	}
	return task.CreatorID != 0 && task.CreatorID == user.ID
}

// StatusView 是任务状态的统一响应结构，running（tache 内存）与历史（数据库）两种来源
// 都映射到同一个结构，避免前端需要兼容多种字段形态
type StatusView struct {
	ID        string     `json:"id"`
	RootPath  string     `json:"root_path"`
	State     string     `json:"state"`
	Status    string     `json:"status"`
	Progress  float64    `json:"progress"`
	Stats     ScanStats  `json:"stats"`
	StartTime *time.Time `json:"start_time,omitempty"`
	EndTime   *time.Time `json:"end_time,omitempty"`
	Error     string     `json:"error,omitempty"`
}

// ==================== 扫描任务 ====================

// HandleStartScan 启动异步查重任务
func HandleStartScan(c *gin.Context) {
	user := currentUser(c)
	var req StartReq
	if err := c.ShouldBindJSON(&req); err != nil {
		common.ErrorResp(c, err, http.StatusBadRequest)
		return
	}
	if user == nil {
		common.ErrorStrResp(c, "未登录", http.StatusUnauthorized)
		return
	}

	// 路径收敛到用户可访问范围（与 middlewares.FsUp 一致），避免越权扫描
	reqPath, err := user.JoinPath(req.Path)
	if err != nil {
		common.ErrorStrResp(c, "路径不合法", http.StatusForbidden)
		return
	}
	reqPath = utils.FixAndCleanPath(reqPath)

	// 目标必须真实存在且是目录，否则直接报错，而不是「扫描完成、0 组重复」的假成功
	obj, err := fs.Get(c.Request.Context(), reqPath, &fs.GetArgs{NoLog: true})
	if err != nil {
		common.ErrorResp(c, fmt.Errorf("目录不可访问: %w", err), http.StatusBadRequest)
		return
	}
	if !obj.IsDir() {
		common.ErrorStrResp(c, "只能对目录发起查重", http.StatusBadRequest)
		return
	}

	cfg := normalizeScanConfig(ScanConfig{
		RootPath:    reqPath,
		MaxDepth:    req.MaxDepth,
		Concurrency: req.Concurrency,
		QPS:         req.QPS,
	})

	// 同一个目录不允许并发重复扫描
	if DedupTaskManager != nil {
		for _, running := range DedupTaskManager.GetByCondition(func(t *DedupScanTask) bool {
			return utils.PathEqual(t.Config.RootPath, cfg.RootPath)
		}) {
			if tacheStateOf(running) {
				common.ErrorStrResp(c, fmt.Sprintf("%s 已有扫描任务正在进行", cfg.RootPath), http.StatusConflict)
				return
			}
		}
	}

	t, err := AddScanTask(cfg, user)
	if err != nil {
		common.ErrorResp(c, err, http.StatusInternalServerError)
		return
	}

	common.SuccessResp(c, gin.H{
		"task_id": t.GetID(),
		"config":  cfg,
	})
}

// tacheStateOf 判断任务是否仍在进行中（排队/运行/取消中）
func tacheStateOf(t *DedupScanTask) bool {
	switch t.GetState() {
	case tache.StatePending, tache.StateRunning, tache.StateCanceling,
		tache.StateWaitingRetry, tache.StateBeforeRetry:
		return true
	default:
		return false
	}
}

// HandleGetStatus 查询任务状态与进度
func HandleGetStatus(c *gin.Context) {
	user := currentUser(c)
	taskID := c.Query("task_id")
	if taskID == "" {
		common.ErrorStrResp(c, "缺少 task_id", http.StatusBadRequest)
		return
	}

	// 1. 正在运行的任务：从 tache 管理器读取（状态、进度、实时统计）
	if DedupTaskManager != nil {
		if t, ok := DedupTaskManager.GetByID(taskID); ok {
			if !canManageTask(user, taskRecord(t)) {
				common.ErrorStrResp(c, "无权查看该任务", http.StatusForbidden)
				return
			}
			snap := t.Snapshot()
			common.SuccessResp(c, StatusView{
				ID:        t.GetID(),
				RootPath:  t.Config.RootPath,
				State:     snap.State,
				Status:    snap.Status,
				Progress:  snap.Progress,
				Stats:     snap.Stats,
				StartTime: t.GetStartTime(),
				EndTime:   t.GetEndTime(),
			})
			return
		}
	}

	// 2. 已结束的任务：从数据库读取
	task, err := GetTaskByID(taskID)
	if err != nil {
		common.ErrorStrResp(c, "任务不存在", http.StatusNotFound)
		return
	}
	if !canManageTask(user, task) {
		common.ErrorStrResp(c, "无权查看该任务", http.StatusForbidden)
		return
	}
	common.SuccessResp(c, statusViewFromTask(task))
}

// taskRecord 用运行中的任务信息构造一个「仅用于权限判断」的任务记录
func taskRecord(t *DedupScanTask) *DedupTask {
	rec := &DedupTask{RootPath: t.Config.RootPath}
	if creator := t.GetCreator(); creator != nil {
		rec.Creator = creator.Username
		rec.CreatorID = creator.ID
	}
	return rec
}

func statusViewFromTask(task *DedupTask) StatusView {
	view := StatusView{
		ID:       task.ID,
		RootPath: task.RootPath,
		State:    task.State,
		Stats: ScanStats{
			ScannedDirs:     task.ScannedDirs,
			ScannedFiles:    task.ScannedFiles,
			VerifiedFiles:   task.VerifiedFiles,
			UnverifiedFiles: task.UnverifiedFiles,
			FailedDirs:      task.FailedDirs,
			DupGroups:       task.DupGroups,
			DupFiles:        task.DupFiles,
			WastedBytes:     task.WastedTotal,
			CandidateGroups: task.CandidateGroups,
			CandidateFiles:  task.CandidateFiles,
		},
		StartTime: &task.StartedAt,
		EndTime:   task.EndedAt,
		Error:     task.Error,
	}
	switch task.State {
	case "finished":
		view.Status = fmt.Sprintf("发现 %d 组重复 (共 %d 文件)，可释放 %s",
			task.DupGroups, task.DupFiles, formatBytes(task.WastedTotal))
		view.Progress = 100
	default:
		view.Status = task.Error
	}
	return view
}

// HandleCancelScan 终止任务
func HandleCancelScan(c *gin.Context) {
	user := currentUser(c)
	taskID := c.Query("task_id")
	if taskID == "" {
		var body struct {
			TaskID string `json:"task_id"`
		}
		if err := c.ShouldBindJSON(&body); err == nil {
			taskID = body.TaskID
		}
	}
	if taskID == "" {
		common.ErrorStrResp(c, "缺少 task_id", http.StatusBadRequest)
		return
	}

	if DedupTaskManager == nil {
		common.ErrorStrResp(c, "任务管理器未初始化", http.StatusInternalServerError)
		return
	}
	t, ok := DedupTaskManager.GetByID(taskID)
	if !ok {
		common.ErrorStrResp(c, "任务不存在或已结束", http.StatusNotFound)
		return
	}
	if !canManageTask(user, taskRecord(t)) {
		common.ErrorStrResp(c, "无权取消该任务", http.StatusForbidden)
		return
	}
	DedupTaskManager.Cancel(taskID)
	common.SuccessResp(c, gin.H{"task_id": taskID, "state": "canceling"})
}

// HandleGetResult 分页获取任务的重复文件结果
func HandleGetResult(c *gin.Context) {
	user := currentUser(c)
	taskID := c.Query("task_id")
	task, err := GetTaskByID(taskID)
	if err != nil {
		common.ErrorStrResp(c, "任务不存在", http.StatusNotFound)
		return
	}
	if !canManageTask(user, task) {
		common.ErrorStrResp(c, "无权查看该任务", http.StatusForbidden)
		return
	}

	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	size, _ := strconv.Atoi(c.DefaultQuery("size", "20"))
	if page < 1 {
		page = 1
	}
	if size < 1 || size > 200 {
		size = 20
	}
	verifiedFilter := c.DefaultQuery("verified", "1") // 1=已校验重复，0=同名同尺寸候选，all=全部
	var verified *bool
	switch verifiedFilter {
	case "1":
		v := true
		verified = &v
	case "0":
		v := false
		verified = &v
	}

	// 分组键按「可释放空间」降序分页
	var keys []string
	if err := resultGroupsQuery(taskID, verified).
		Select("group_key").
		Group("group_key").
		Having("COUNT(*) > 1").
		Order("MAX(size) * (COUNT(*) - 1) DESC").
		Offset((page-1)*size).
		Limit(size).
		Pluck("group_key", &keys).Error; err != nil {
		common.ErrorResp(c, err, http.StatusInternalServerError, true)
		return
	}

	var total int64
	if err := resultGroupsQuery(taskID, verified).
		Select("group_key").
		Group("group_key").
		Having("COUNT(*) > 1").
		Count(&total).Error; err != nil {
		common.ErrorResp(c, err, http.StatusInternalServerError, true)
		return
	}

	groups := make([]DupGroup, 0, len(keys))
	if len(keys) > 0 {
		var items []DedupFileItem
		if err := db.GetDb().
			Where("task_id = ? AND group_key IN ?", taskID, keys).
			Order("group_key, modified ASC, path ASC").
			Find(&items).Error; err != nil {
			common.ErrorResp(c, err, http.StatusInternalServerError, true)
			return
		}
		// 按分页查询得到的顺序（可释放空间降序）组装，保证页内顺序与排序口径一致
		index := make(map[string]int, len(keys))
		for i, k := range keys {
			index[k] = i
		}
		buckets := make([][]FileItem, len(keys))
		meta := make([]DedupFileItem, len(keys))
		for _, item := range items {
			i, ok := index[item.GroupKey]
			if !ok {
				continue
			}
			meta[i] = item
			buckets[i] = append(buckets[i], FileItem{
				Path:     item.Path,
				Name:     item.Name,
				Size:     item.Size,
				HashType: item.HashType,
				Hash:     item.Hash,
				Modified: item.Modified,
			})
		}
		for i, files := range buckets {
			if len(files) == 0 {
				continue
			}
			groups = append(groups, DupGroup{
				GroupKey:    keys[i],
				HashType:    meta[i].HashType,
				Hash:        meta[i].Hash,
				Size:        meta[i].Size,
				Verified:    meta[i].Verified,
				WastedBytes: meta[i].Size * int64(len(files)-1),
				Files:       files,
			})
		}
	}

	common.SuccessResp(c, gin.H{
		"total":  total,
		"page":   page,
		"size":   size,
		"groups": groups,
	})
}

func resultGroupsQuery(taskID string, verified *bool) *gorm.DB {
	q := db.GetDb().Model(&DedupFileItem{}).Where("task_id = ?", taskID)
	if verified != nil {
		q = q.Where("verified = ?", *verified)
	}
	return q
}

// HandleListHistory 列出历史任务（普通用户仅能看到自己发起的任务）
func HandleListHistory(c *gin.Context) {
	user := currentUser(c)
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	size, _ := strconv.Atoi(c.DefaultQuery("size", "20"))
	if page < 1 {
		page = 1
	}
	if size < 1 || size > 200 {
		size = 20
	}

	q := db.GetDb().Model(&DedupTask{})
	if user == nil {
		common.ErrorStrResp(c, "未登录", http.StatusUnauthorized)
		return
	}
	if !user.IsAdmin() {
		q = q.Where("creator_id = ?", user.ID)
	}

	var total int64
	if err := q.Count(&total).Error; err != nil {
		common.ErrorResp(c, err, http.StatusInternalServerError, true)
		return
	}
	var tasks []DedupTask
	if err := q.Order("started_at DESC").Offset((page - 1) * size).Limit(size).Find(&tasks).Error; err != nil {
		common.ErrorResp(c, err, http.StatusInternalServerError, true)
		return
	}
	common.SuccessResp(c, gin.H{"content": tasks, "total": total})
}

// HandleGetHistoryDetail 获取单个历史任务详情
func HandleGetHistoryDetail(c *gin.Context) {
	user := currentUser(c)
	task, err := GetTaskByID(c.Param("id"))
	if err != nil {
		common.ErrorStrResp(c, "任务不存在", http.StatusNotFound)
		return
	}
	if !canManageTask(user, task) {
		common.ErrorStrResp(c, "无权查看该任务", http.StatusForbidden)
		return
	}
	common.SuccessResp(c, task)
}

// HandleDeleteHistory 删除历史任务记录及其结果数据（运行中的任务需先取消）
func HandleDeleteHistory(c *gin.Context) {
	user := currentUser(c)
	task, err := GetTaskByID(c.Param("id"))
	if err != nil {
		common.ErrorStrResp(c, "任务不存在", http.StatusNotFound)
		return
	}
	if !canManageTask(user, task) {
		common.ErrorStrResp(c, "无权删除该任务", http.StatusForbidden)
		return
	}
	if task.State == "running" {
		common.ErrorStrResp(c, "任务正在运行，请先取消", http.StatusConflict)
		return
	}
	if err := db.GetDb().Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("task_id = ?", task.ID).Delete(&DedupFileItem{}).Error; err != nil {
			return err
		}
		return tx.Where("id = ?", task.ID).Delete(&DedupTask{}).Error
	}); err != nil {
		common.ErrorResp(c, err, http.StatusInternalServerError, true)
		return
	}
	common.SuccessResp(c)
}

// ==================== 清理 ====================

// 常见媒体附属文件后缀
var companionExts = map[string]bool{
	".srt": true, ".ass": true, ".ssa": true, ".sub": true, ".idx": true,
	".nfo": true, ".jpg": true, ".jpeg": true, ".png": true, ".webp": true,
	".xml": true,
}

// isCompanionOf 判断 objName 是否是 videoName 的附属文件。
// 仅允许「同名不同扩展名」或「主名 + .语言/标签 + 附属扩展名」两种形式，
// 避免旧实现用 HasPrefix 造成 EP01 误删 EP01-其他版本.srt 这类过宽匹配。
func isCompanionOf(objName, videoName string) bool {
	objExt := strings.ToLower(filepath.Ext(objName))
	if !companionExts[objExt] {
		return false
	}
	base := strings.TrimSuffix(videoName, filepath.Ext(videoName))
	obj := strings.ToLower(objName)
	baseLower := strings.ToLower(base)
	if baseLower == "" || !strings.HasPrefix(obj, baseLower) {
		return false
	}
	rest := obj[len(baseLower):]
	return rest == "" || strings.HasPrefix(rest, ".")
}

// authorizeRemovePaths 对待删除路径做三重校验（P0-3）：
//  1. 收敛到当前用户可访问的路径范围（user.JoinPath），并拒绝空路径与根路径；
//  2. 必须位于该任务的扫描根目录内；
//  3. 当前用户对该路径拥有写权限（内容写权限或元信息授权）。
//
// 通过校验的路径仍需与本任务「已校验重复文件」做交集，见 HandleBatchRemove。
func authorizeRemovePaths(ctx context.Context, user *model.User, task *DedupTask, paths []string) (allowed []string, rejected []removeRejection) {
	seen := make(map[string]struct{}, len(paths))
	for _, raw := range paths {
		if strings.TrimSpace(raw) == "" {
			continue
		}
		reqPath, err := user.JoinPath(raw)
		if err != nil {
			rejected = append(rejected, removeRejection{Path: raw, Reason: "路径超出可访问范围"})
			continue
		}
		clean := utils.FixAndCleanPath(reqPath)
		if clean == "/" || clean == "." {
			rejected = append(rejected, removeRejection{Path: raw, Reason: "不允许删除根路径"})
			continue
		}
		if _, ok := seen[clean]; ok {
			continue
		}
		if !utils.IsSubPath(task.RootPath, clean) {
			rejected = append(rejected, removeRejection{Path: raw, Reason: "不在本次扫描的目录范围内"})
			continue
		}
		parent := path.Dir(clean)
		meta, mErr := op.GetNearestMeta(parent)
		if mErr != nil && !errors.Is(errors.Cause(mErr), errs.MetaNotFound) {
			rejected = append(rejected, removeRejection{Path: raw, Reason: "无法读取路径元信息"})
			continue
		}
		if !user.CanWriteContent() && !common.CanWriteContentBypassUserPerms(meta, clean) {
			rejected = append(rejected, removeRejection{Path: raw, Reason: "没有写权限"})
			continue
		}
		if !common.CanWrite(user, meta, clean) {
			rejected = append(rejected, removeRejection{Path: raw, Reason: "没有写权限"})
			continue
		}
		seen[clean] = struct{}{}
		allowed = append(allowed, clean)
	}
	return allowed, rejected
}

// maxRemovePaths 单次清理请求允许的最大路径数，避免一次请求触发超大批量删除
const maxRemovePaths = 5000

// guardKeepOneCopy 保证「同一组重复文件不会被全部删光」。
//
// 前端可以「整组选中」，用户很容易把一份内容的所有副本都勾上——这在查重工具里
// 等价于彻底删除该内容。这里在服务端兜底：如果某组待删路径数 >= 该组在库内的成员数，
// 则该组的这批路径全部拒绝并给出明确原因（前端会展示在清理结果里）。
//
// members: group_key -> 该组在库内的全部成员路径
// deletable: 已通过权限校验、待删除的路径
func guardKeepOneCopy(members map[string][]string, deletable []string) (allowed []string, rejected []removeRejection) {
	if len(deletable) == 0 {
		return nil, nil
	}
	pathToGroup := make(map[string]string, len(deletable))
	for key, paths := range members {
		for _, p := range paths {
			pathToGroup[p] = key
		}
	}
	deleteCount := make(map[string]int, len(members))
	for _, p := range deletable {
		if key, ok := pathToGroup[p]; ok {
			deleteCount[key]++
		}
	}
	for _, p := range deletable {
		key, ok := pathToGroup[p]
		if ok && deleteCount[key] >= len(members[key]) {
			rejected = append(rejected, removeRejection{
				Path:   p,
				Reason: "会删除该组全部副本，至少需要保留 1 份",
			})
			continue
		}
		allowed = append(allowed, p)
	}
	return allowed, rejected
}

// HandleBatchRemove 批量清理重复文件（仅限本任务「已校验」的重复文件）
func HandleBatchRemove(c *gin.Context) {
	user := currentUser(c)
	var req RemoveReq
	if err := c.ShouldBindJSON(&req); err != nil {
		common.ErrorResp(c, err, http.StatusBadRequest)
		return
	}
	if user == nil {
		common.ErrorStrResp(c, "未登录", http.StatusUnauthorized)
		return
	}
	if req.TaskID == "" {
		common.ErrorStrResp(c, "缺少 task_id", http.StatusBadRequest)
		return
	}
	task, err := GetTaskByID(req.TaskID)
	if err != nil {
		common.ErrorStrResp(c, "任务不存在", http.StatusNotFound)
		return
	}
	if !canManageTask(user, task) {
		common.ErrorStrResp(c, "无权操作该任务", http.StatusForbidden)
		return
	}
	if len(req.Paths) == 0 {
		common.ErrorStrResp(c, "未选择任何文件", http.StatusBadRequest)
		return
	}
	// 上限校验：避免一次请求触发不可控的大批量删除
	// （前端单页最多 20 组，正常情况下远达不到这个数量）
	if len(req.Paths) > maxRemovePaths {
		common.ErrorStrResp(c, fmt.Sprintf("单次最多删除 %d 个文件，请分批清理", maxRemovePaths), http.StatusBadRequest)
		return
	}

	allowed, rejected := authorizeRemovePaths(c.Request.Context(), user, task, req.Paths)

	// 只允许删除本任务中「已校验」的重复文件：未校验候选、已清理、非本任务路径一律拒绝
	deletable := make([]string, 0, len(allowed))
	groupKeys := make([]string, 0, len(allowed))
	if len(allowed) > 0 {
		var items []DedupFileItem
		if err := db.GetDb().
			Where("task_id = ? AND verified = ? AND path IN ?", req.TaskID, true, allowed).
			Find(&items).Error; err != nil {
			common.ErrorResp(c, err, http.StatusInternalServerError, true)
			return
		}
		verifiedSet := make(map[string]struct{}, len(items))
		for _, item := range items {
			verifiedSet[item.Path] = struct{}{}
			groupKeys = append(groupKeys, item.GroupKey)
		}
		for _, p := range allowed {
			if _, ok := verifiedSet[p]; ok {
				deletable = append(deletable, p)
			} else {
				rejected = append(rejected, removeRejection{Path: p, Reason: "不属于该任务的已校验重复文件"})
			}
		}
	}

	// 组内兜底：同一组重复文件至少要保留 1 份，避免「整组选中」把内容删光
	if len(deletable) > 0 {
		members, err := GroupMembers(req.TaskID, groupKeys)
		if err != nil {
			common.ErrorResp(c, err, http.StatusInternalServerError, true)
			return
		}
		guarded, guardRejected := guardKeepOneCopy(members, deletable)
		deletable, rejected = guarded, append(rejected, guardRejected...)
	}

	requireWrite := func(target string) bool {
		parent := path.Dir(target)
		meta, mErr := op.GetNearestMeta(parent)
		if mErr != nil && !errors.Is(errors.Cause(mErr), errs.MetaNotFound) {
			return false
		}
		if !user.CanWriteContent() && !common.CanWriteContentBypassUserPerms(meta, target) {
			return false
		}
		return common.CanWrite(user, meta, target)
	}

	// 连带删除附属文件（限同目录、且同样通过写权限校验）
	targets := make(map[string]struct{}, len(deletable))
	for _, p := range deletable {
		targets[p] = struct{}{}
	}
	companions := make(map[string]struct{})
	if req.DeleteCompanions {
		dirToFiles := make(map[string][]string)
		for _, p := range deletable {
			dir := path.Dir(p)
			dirToFiles[dir] = append(dirToFiles[dir], p)
		}
		for dir, files := range dirToFiles {
			objs, err := fs.List(c.Request.Context(), dir, &fs.ListArgs{NoLog: true})
			if err != nil {
				log.Warnf("[dedup] list dir %s failed when collecting companions: %v", dir, err)
				continue
			}
			for _, videoPath := range files {
				videoName := path.Base(videoPath)
				for _, obj := range objs {
					if obj.IsDir() || !isCompanionOf(obj.GetName(), videoName) {
						continue
					}
					companionPath := path.Join(dir, obj.GetName())
					if _, ok := targets[companionPath]; ok {
						continue
					}
					if !utils.IsSubPath(task.RootPath, companionPath) || !requireWrite(companionPath) {
						continue
					}
					targets[companionPath] = struct{}{}
					companions[companionPath] = struct{}{}
				}
			}
		}
	}

	successPaths := make([]string, 0, len(targets))
	companionRemoved := 0
	var errMsgs []string
	for p := range targets {
		if err := fs.Remove(c.Request.Context(), p); err != nil {
			log.Errorf("[dedup] failed to remove file %s: %v", p, err)
			errMsgs = append(errMsgs, fmt.Sprintf("%s: %v", p, err))
			continue
		}
		successPaths = append(successPaths, p)
		if _, ok := companions[p]; ok {
			companionRemoved++
		}
	}

	// 从本地数据库清理已成功删除的条目，保证重新查询时不会再次出现；
	// 随后丢弃只剩 1 个成员的分组并重算统计，让界面上的数字与库内真实结果一致
	if len(successPaths) > 0 {
		if err := DeleteFileItems(req.TaskID, successPaths); err != nil {
			log.Errorf("[dedup] failed to delete cleaned files from DB: %v", err)
		} else if err := DropDanglingGroups(req.TaskID); err != nil {
			log.Errorf("[dedup] failed to drop dangling groups: %v", err)
		} else {
			RecomputeTaskCounters(req.TaskID)
		}
	}

	// 空目录清理：仅限扫描根目录之内，且绝不会删除根目录本身
	emptyDirsRemoved := 0
	if req.RemoveEmptyDirs && len(successPaths) > 0 {
		affected := make(map[string]struct{})
		for _, p := range successPaths {
			d := path.Dir(p)
			for d != "" && d != "/" && d != "." {
				if utils.PathEqual(d, task.RootPath) || !utils.IsSubPath(task.RootPath, d) {
					break
				}
				affected[d] = struct{}{}
				d = path.Dir(d)
			}
		}
		dirs := make([]string, 0, len(affected))
		for d := range affected {
			dirs = append(dirs, d)
		}
		// 由深到浅，便于父目录在子目录清空后也能被清理
		sort.Slice(dirs, func(i, j int) bool {
			if len(dirs[i]) != len(dirs[j]) {
				return len(dirs[i]) > len(dirs[j])
			}
			return dirs[i] < dirs[j]
		})
		for _, d := range dirs {
			objs, err := fs.List(c.Request.Context(), d, &fs.ListArgs{Refresh: true, NoLog: true})
			if err != nil || len(objs) != 0 {
				continue
			}
			if err := fs.Remove(c.Request.Context(), d); err != nil {
				continue
			}
			emptyDirsRemoved++
			log.Infof("[dedup] removed empty directory: %s", d)
		}
	}

	common.SuccessResp(c, gin.H{
		"success_count":      len(successPaths),
		"companion_removed":  companionRemoved,
		"empty_dirs_removed": emptyDirsRemoved,
		"failed_count":       len(errMsgs),
		"errors":             errMsgs,
		"rejected":           rejected,
	})
}

// HandleListDirs 获取指定目录下的子文件夹（保留给第三方调用）。
// 前端已改用通用的 FolderChooseInput（走 /api/fs/list），这里同样收敛到用户可访问范围。
func HandleListDirs(c *gin.Context) {
	user := currentUser(c)
	if user == nil {
		common.ErrorStrResp(c, "未登录", http.StatusUnauthorized)
		return
	}
	reqPath, err := user.JoinPath(c.DefaultQuery("path", "/"))
	if err != nil {
		common.ErrorStrResp(c, "路径不合法", http.StatusForbidden)
		return
	}
	reqPath = utils.FixAndCleanPath(reqPath)

	objs, err := fs.List(c.Request.Context(), reqPath, &fs.ListArgs{NoLog: true})
	if err != nil {
		common.ErrorResp(c, err, http.StatusInternalServerError)
		return
	}

	dirs := make([]gin.H, 0, len(objs))
	for _, obj := range objs {
		if !obj.IsDir() {
			continue
		}
		dirs = append(dirs, gin.H{
			"name": obj.GetName(),
			"path": path.Join(reqPath, obj.GetName()),
		})
	}
	common.SuccessResp(c, gin.H{"path": reqPath, "dirs": dirs})
}
