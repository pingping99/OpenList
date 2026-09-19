package dedup

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

func setupTestDB(t *testing.T) {
	t.Helper()
	d, err := gorm.Open(sqlite.Open("file:mem_reanalyze?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open in-memory sqlite: %v", err)
	}
	conf.Conf = conf.DefaultConfig("data")
	db.Init(d)
	InitDB()
}

func TestSnapshot_SaveAndGet(t *testing.T) {
	setupTestDB(t)

	taskID := uuid.NewString()
	files := []FileItem{
		{
			Path:     "/data/a.mp4",
			Name:     "a.mp4",
			Size:     1024,
			HashType: "md5",
			Hash:     "MD5_A",
			Modified: time.Unix(1700000000, 0),
		},
		{
			Path:     "/data/b.mp4",
			Name:     "b.mp4",
			Size:     2048,
			HashType: "md5",
			Hash:     "MD5_B",
			Modified: time.Unix(1700000100, 0),
		},
	}

	if err := SaveSnapshotFiles(taskID, files); err != nil {
		t.Fatalf("SaveSnapshotFiles failed: %v", err)
	}

	items, err := GetSnapshotFiles(taskID)
	if err != nil {
		t.Fatalf("GetSnapshotFiles failed: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(items))
	}
	if items[0].Path != "/data/a.mp4" || items[0].Hash != "MD5_A" {
		t.Fatalf("item 0 mismatch: %+v", items[0])
	}

	// 测试复制快照
	newTaskID := uuid.NewString()
	if err := CopySnapshotFiles(taskID, newTaskID); err != nil {
		t.Fatalf("CopySnapshotFiles failed: %v", err)
	}
	newItems, err := GetSnapshotFiles(newTaskID)
	if err != nil {
		t.Fatalf("GetSnapshotFiles for new task failed: %v", err)
	}
	if len(newItems) != 2 {
		t.Fatalf("expected 2 copied items, got %d", len(newItems))
	}

	// 测试清理文件时同步清理快照
	if err := DeleteFileItems(taskID, []string{"/data/a.mp4"}); err != nil {
		t.Fatalf("DeleteFileItems failed: %v", err)
	}
	remItems, err := GetSnapshotFiles(taskID)
	if err != nil {
		t.Fatalf("GetSnapshotFiles after delete failed: %v", err)
	}
	if len(remItems) != 1 || remItems[0].Path != "/data/b.mp4" {
		t.Fatalf("expected 1 remaining item /data/b.mp4, got %+v", remItems)
	}
}

func TestReanalyze_FilteringAndGrouping(t *testing.T) {
	setupTestDB(t)

	gin.SetMode(gin.TestMode)
	router := gin.New()
	adminUser := &model.User{ID: 1, Role: model.ADMIN, Username: "admin"}

	router.Use(func(c *gin.Context) {
		c.Request = c.Request.WithContext(context.WithValue(c.Request.Context(), conf.UserKey, adminUser))
		c.Next()
	})
	router.POST("/reanalyze", HandleReanalyze)

	sourceTaskID := uuid.NewString()
	baseTask := &DedupTask{
		ID:            sourceTaskID,
		RootPath:      "/videos",
		State:         "finished",
		Creator:       adminUser.Username,
		CreatorID:     adminUser.ID,
		IsSnapshot:    true,
		SnapshotFiles: 6,
	}
	SaveTask(baseTask)

	files := []FileItem{
		// 100MB 视频 2 份 (重复)
		{Path: "/videos/v1.mp4", Name: "v1.mp4", Size: 100 * 1024 * 1024, HashType: "md5", Hash: "HASH_V", Modified: time.Unix(100, 0)},
		{Path: "/videos/v2.mp4", Name: "v2.mp4", Size: 100 * 1024 * 1024, HashType: "md5", Hash: "HASH_V", Modified: time.Unix(200, 0)},
		// 50MB 文档 2 份 (重复)
		{Path: "/videos/d1.pdf", Name: "d1.pdf", Size: 50 * 1024 * 1024, HashType: "md5", Hash: "HASH_D", Modified: time.Unix(100, 0)},
		{Path: "/videos/d2.pdf", Name: "d2.pdf", Size: 50 * 1024 * 1024, HashType: "md5", Hash: "HASH_D", Modified: time.Unix(200, 0)},
		// 5MB 小文件 2 份 (重复)
		{Path: "/videos/s1.txt", Name: "s1.txt", Size: 5 * 1024 * 1024, HashType: "md5", Hash: "HASH_S", Modified: time.Unix(100, 0)},
		{Path: "/videos/s2.txt", Name: "s2.txt", Size: 5 * 1024 * 1024, HashType: "md5", Hash: "HASH_S", Modified: time.Unix(200, 0)},
	}
	if err := SaveSnapshotFiles(sourceTaskID, files); err != nil {
		t.Fatalf("SaveSnapshotFiles failed: %v", err)
	}

	// 1. 重析：最小大小 20MB -> 应当过滤掉 5MB 的 txt，保留 mp4 和 pdf (2组重复)
	body1, _ := json.Marshal(ReanalyzeReq{
		TaskID:  sourceTaskID,
		MinSize: 20 * 1024 * 1024,
	})
	w1 := httptest.NewRecorder()
	req1, _ := http.NewRequest("POST", "/reanalyze", bytes.NewBuffer(body1))
	req1.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w1, req1)

	if w1.Code != http.StatusOK {
		t.Fatalf("reanalyze 1 failed with code %d: %s", w1.Code, w1.Body.String())
	}
	var res1 struct {
		Code int `json:"code"`
		Data struct {
			TaskID    string `json:"task_id"`
			DupGroups int    `json:"dup_groups"`
			DupFiles  int    `json:"dup_files"`
		} `json:"data"`
	}
	if err := json.Unmarshal(w1.Body.Bytes(), &res1); err != nil {
		t.Fatalf("unmarshal resp 1 failed: %v", err)
	}
	if res1.Data.DupGroups != 2 || res1.Data.DupFiles != 4 {
		t.Fatalf("expected 2 groups, 4 files with min_size 20MB, got groups=%d, files=%d", res1.Data.DupGroups, res1.Data.DupFiles)
	}

	// 2. 重析：最小大小 20MB + 仅包含 mp4 -> 仅留下 1 组视频
	body2, _ := json.Marshal(ReanalyzeReq{
		TaskID:      sourceTaskID,
		MinSize:     20 * 1024 * 1024,
		IncludeExts: []string{"mp4"},
	})
	w2 := httptest.NewRecorder()
	req2, _ := http.NewRequest("POST", "/reanalyze", bytes.NewBuffer(body2))
	req2.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w2, req2)

	if w2.Code != http.StatusOK {
		t.Fatalf("reanalyze 2 failed with code %d: %s", w2.Code, w2.Body.String())
	}
	var res2 struct {
		Code int `json:"code"`
		Data struct {
			TaskID    string `json:"task_id"`
			DupGroups int    `json:"dup_groups"`
			DupFiles  int    `json:"dup_files"`
		} `json:"data"`
	}
	if err := json.Unmarshal(w2.Body.Bytes(), &res2); err != nil {
		t.Fatalf("unmarshal resp 2 failed: %v", err)
	}
	if res2.Data.DupGroups != 1 || res2.Data.DupFiles != 2 {
		t.Fatalf("expected 1 group, 2 files with mp4 filter, got groups=%d, files=%d", res2.Data.DupGroups, res2.Data.DupFiles)
	}
}

func TestIncremental_CacheHit(t *testing.T) {
	setupTestDB(t)

	baseTaskID := uuid.NewString()
	baseTask := &DedupTask{
		ID:            baseTaskID,
		RootPath:      "/remote",
		State:         "finished",
		IsSnapshot:    true,
		SnapshotFiles: 2,
		StartedAt:     time.Now().Add(-1 * time.Hour),
	}
	SaveTask(baseTask)

	mTime := time.Unix(1700000000, 0)
	// 在基准任务的快照中记录文件哈希
	baseFiles := []FileItem{
		{Path: "/remote/file1.bin", Name: "file1.bin", Size: 200, HashType: "md5", Hash: "HASH_FILE_1", Modified: mTime},
		{Path: "/remote/file2.bin", Name: "file2.bin", Size: 200, HashType: "md5", Hash: "HASH_FILE_1", Modified: mTime},
	}
	if err := SaveSnapshotFiles(baseTaskID, baseFiles); err != nil {
		t.Fatalf("SaveSnapshotFiles failed: %v", err)
	}

	// 模拟驱动不提供哈希的无哈希文件树（如 WebDAV 或本地存储）
	tree := map[string][]model.Obj{
		"/remote": {
			fileObj("file1.bin", 200, nil, ""), // 尺寸与修改时间与快照完全一致
			fileObj("file2.bin", 200, nil, ""), // 尺寸与修改时间与快照完全一致
			fileObj("file3.bin", 300, nil, ""), // 新增文件
		},
	}

	progress := &Progress{}
	cfg := ScanConfig{
		RootPath:    "/remote",
		Incremental: true,
		BaseTaskID:  baseTaskID,
		Concurrency: 1,
		QPS:         1000,
	}

	groups, _, allFiles, err := scan(context.Background(), cfg, progress, fakeLister(tree))
	if err != nil {
		t.Fatalf("incremental scan failed: %v", err)
	}

	// file1 和 file2 应当从快照缓存中命中哈希，因此直接聚合为 1 个已校验重复组！
	if len(groups) != 1 {
		t.Fatalf("expected 1 verified group from cache hit, got %d: %+v", len(groups), groups)
	}
	if !groups[0].Verified || groups[0].Hash != "HASH_FILE_1" {
		t.Fatalf("expected verified group with HASH_FILE_1, got %+v", groups[0])
	}

	// 验证命中计数
	stats := progress.Snapshot()
	if stats.CachedFiles != 2 {
		t.Fatalf("expected 2 cached files hit, got %d", stats.CachedFiles)
	}
	if stats.CachedBytes != 400 {
		t.Fatalf("expected 400 cached bytes hit, got %d", stats.CachedBytes)
	}
	if len(allFiles) != 3 {
		t.Fatalf("expected 3 total scanned files in snapshot, got %d", len(allFiles))
	}
}
