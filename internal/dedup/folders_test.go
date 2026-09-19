package dedup

import (
	"context"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestComputeFolderPairs_Threshold30Percent(t *testing.T) {
	now := time.Now()

	// 构造：
	// DirA: 10 个文件，其中 4 个与 DirB 相同（40% > 30%）
	// DirB: 10 个文件，其中 4 个与 DirA 相同（40% > 30%）
	// DirC: 10 个文件，其中 1 个与 DirA 相同（10% < 30%）
	items := []DedupFileItem{
		// A 与 B 重复的 4 个文件
		{TaskID: "t1", GroupKey: "g1", Verified: true, Path: "/media/A/f1.mp4", Name: "f1.mp4", Size: 100, Modified: now},
		{TaskID: "t1", GroupKey: "g1", Verified: true, Path: "/media/B/f1.mp4", Name: "f1.mp4", Size: 100, Modified: now},
		{TaskID: "t1", GroupKey: "g2", Verified: true, Path: "/media/A/f2.mp4", Name: "f2.mp4", Size: 100, Modified: now},
		{TaskID: "t1", GroupKey: "g2", Verified: true, Path: "/media/B/f2.mp4", Name: "f2.mp4", Size: 100, Modified: now},
		{TaskID: "t1", GroupKey: "g3", Verified: true, Path: "/media/A/f3.mp4", Name: "f3.mp4", Size: 100, Modified: now},
		{TaskID: "t1", GroupKey: "g3", Verified: true, Path: "/media/B/f3.mp4", Name: "f3.mp4", Size: 100, Modified: now},
		{TaskID: "t1", GroupKey: "g4", Verified: true, Path: "/media/A/f4.mp4", Name: "f4.mp4", Size: 100, Modified: now},
		{TaskID: "t1", GroupKey: "g4", Verified: true, Path: "/media/B/f4.mp4", Name: "f4.mp4", Size: 100, Modified: now},

		// A 与 C 重复的 1 个文件
		{TaskID: "t1", GroupKey: "g5", Verified: true, Path: "/media/A/f5.mp4", Name: "f5.mp4", Size: 100, Modified: now},
		{TaskID: "t1", GroupKey: "g5", Verified: true, Path: "/media/C/f5.mp4", Name: "f5.mp4", Size: 100, Modified: now},
	}

	dirStats := map[string]*DirStat{
		"/media/A": {Path: "/media/A", FileCount: 10, TotalSize: 1000},
		"/media/B": {Path: "/media/B", FileCount: 10, TotalSize: 1000},
		"/media/C": {Path: "/media/C", FileCount: 10, TotalSize: 1000},
	}

	t.Run("默认 30% 阈值应当命中 A 与 B，过滤 A 与 C", func(t *testing.T) {
		pairs := computeFolderPairs(items, dirStats, 0.30)
		if len(pairs) != 1 {
			t.Fatalf("expected 1 pair (A and B), got %d: %+v", len(pairs), pairs)
		}
		p := pairs[0]
		if (p.DirA != "/media/A" && p.DirA != "/media/B") || (p.DirB != "/media/A" && p.DirB != "/media/B") {
			t.Fatalf("unexpected pair dirs: %s, %s", p.DirA, p.DirB)
		}
		if p.DupFilesCount != 4 {
			t.Fatalf("expected 4 duplicate files, got %d", p.DupFilesCount)
		}
		if p.RatioA != 0.40 && p.RatioB != 0.40 {
			t.Fatalf("expected ratio 0.40, got ratioA: %f, ratioB: %f", p.RatioA, p.RatioB)
		}
		if p.Similarity != 0.40 {
			t.Fatalf("expected similarity 0.40, got %f", p.Similarity)
		}
	})

	t.Run("阈值提升至 50% 时 A 与 B 也应当被过滤", func(t *testing.T) {
		pairs := computeFolderPairs(items, dirStats, 0.50)
		if len(pairs) != 0 {
			t.Fatalf("expected 0 pairs with 50%% threshold, got %d", len(pairs))
		}
	})

	t.Run("阈值降低至 10% 时 A 与 C 也应当命中", func(t *testing.T) {
		pairs := computeFolderPairs(items, dirStats, 0.10)
		if len(pairs) != 2 {
			t.Fatalf("expected 2 pairs with 10%% threshold, got %d", len(pairs))
		}
	})
}

func TestComputeFolderPairs_ExcludeAncestors(t *testing.T) {
	now := time.Now()

	// /movies 与 /movies/action 虽然共享重复文件，但因为是父子目录关系，必须排除
	items := []DedupFileItem{
		{TaskID: "t1", GroupKey: "g1", Verified: true, Path: "/movies/f1.mp4", Name: "f1.mp4", Size: 100, Modified: now},
		{TaskID: "t1", GroupKey: "g1", Verified: true, Path: "/movies/action/f1.mp4", Name: "f1.mp4", Size: 100, Modified: now},
	}
	dirStats := map[string]*DirStat{
		"/movies":        {Path: "/movies", FileCount: 2, TotalSize: 200},
		"/movies/action": {Path: "/movies/action", FileCount: 1, TotalSize: 100},
	}

	pairs := computeFolderPairs(items, dirStats, 0.30)
	if len(pairs) != 0 {
		t.Fatalf("父子目录层级应当被过滤，实际匹配到了: %+v", pairs)
	}
}

func TestMergeFolders_Validation(t *testing.T) {
	ctx := context.Background()
	user := &model.User{Role: 2} // admin

	t.Run("空路径拒绝", func(t *testing.T) {
		_, err := MergeFolders(ctx, user, MergeFoldersReq{
			TaskID:    "task-1",
			SourceDir: "",
			TargetDir: "/valid",
		})
		if err == nil {
			t.Fatal("expected error for empty source dir")
		}
	})

	t.Run("根目录拒绝", func(t *testing.T) {
		_, err := MergeFolders(ctx, user, MergeFoldersReq{
			TaskID:    "task-1",
			SourceDir: "/",
			TargetDir: "/valid",
		})
		if err == nil {
			t.Fatal("expected error for root source dir")
		}
	})

	t.Run("相同目录拒绝", func(t *testing.T) {
		_, err := MergeFolders(ctx, user, MergeFoldersReq{
			TaskID:    "task-1",
			SourceDir: "/same",
			TargetDir: "/same",
		})
		if err == nil {
			t.Fatal("expected error for same dirs")
		}
	})

	t.Run("嵌套目录拒绝", func(t *testing.T) {
		_, err := MergeFolders(ctx, user, MergeFoldersReq{
			TaskID:    "task-1",
			SourceDir: "/parent",
			TargetDir: "/parent/child",
		})
		if err == nil {
			t.Fatal("expected error for nested dirs")
		}
	})
}

func TestQueryDuplicateFolders_PaginationAndKeyword(t *testing.T) {
	taskID := "test-cache-task"
	InvalidateFolderPairsCache(taskID)

	// Pre-populate cache directly to test QueryDuplicateFolders and GetFolderMatchedFiles
	folderPairsCacheMu.Lock()
	folderPairsCache[taskID] = &cachedFolderPairs{
		computedAt: time.Now(),
		pairs: []DupFolderPair{
			{DirA: "/media/movies/action", DirB: "/backup/action", Similarity: 0.90, DupFilesCount: 9, MatchedFiles: []DupFolderMatchedFile{{NameA: "act1.mp4", NameB: "act1.mp4", Size: 100}}},
			{DirA: "/media/movies/comedy", DirB: "/backup/comedy", Similarity: 0.80, DupFilesCount: 8, MatchedFiles: []DupFolderMatchedFile{{NameA: "com1.mp4", NameB: "com1.mp4", Size: 200}}},
			{DirA: "/media/music/rock", DirB: "/backup/rock", Similarity: 0.70, DupFilesCount: 7},
			{DirA: "/media/music/jazz", DirB: "/backup/jazz", Similarity: 0.60, DupFilesCount: 6},
			{DirA: "/media/photos/2023", DirB: "/backup/2023", Similarity: 0.50, DupFilesCount: 5},
		},
	}
	folderPairsCacheMu.Unlock()

	defer InvalidateFolderPairsCache(taskID)

	t.Run("分页第一页 (perPage=2)", func(t *testing.T) {
		total, pairs, err := QueryDuplicateFolders(taskID, 0.30, "", 1, 2)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if total != 5 {
			t.Fatalf("expected total 5, got %d", total)
		}
		if len(pairs) != 2 {
			t.Fatalf("expected 2 pairs on page 1, got %d", len(pairs))
		}
		if pairs[0].DirA != "/media/movies/action" || pairs[1].DirA != "/media/movies/comedy" {
			t.Fatalf("unexpected pairs on page 1: %+v", pairs)
		}
	})

	t.Run("分页第二页 (perPage=2)", func(t *testing.T) {
		total, pairs, err := QueryDuplicateFolders(taskID, 0.30, "", 2, 2)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if total != 5 {
			t.Fatalf("expected total 5, got %d", total)
		}
		if len(pairs) != 2 {
			t.Fatalf("expected 2 pairs on page 2, got %d", len(pairs))
		}
		if pairs[0].DirA != "/media/music/rock" || pairs[1].DirA != "/media/music/jazz" {
			t.Fatalf("unexpected pairs on page 2: %+v", pairs)
		}
	})

	t.Run("关键词过滤 (kw=music)", func(t *testing.T) {
		total, pairs, err := QueryDuplicateFolders(taskID, 0.30, "music", 1, 10)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if total != 2 {
			t.Fatalf("expected total 2 for kw=music, got %d", total)
		}
		if len(pairs) != 2 {
			t.Fatalf("expected 2 pairs, got %d", len(pairs))
		}
	})

	t.Run("阈值过滤 (threshold=0.75)", func(t *testing.T) {
		total, pairs, err := QueryDuplicateFolders(taskID, 0.75, "", 1, 10)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if total != 2 {
			t.Fatalf("expected 2 pairs with similarity >= 0.75, got %d", total)
		}
		if len(pairs) != 2 {
			t.Fatalf("expected 2 pairs, got %d", len(pairs))
		}
	})

	t.Run("GetFolderMatchedFiles 获取单对匹配文件", func(t *testing.T) {
		files, err := GetFolderMatchedFiles(taskID, "/media/movies/action", "/backup/action")
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if len(files) != 1 || files[0].NameA != "act1.mp4" {
			t.Fatalf("expected 1 matched file, got: %+v", files)
		}
	})

	t.Run("InvalidateFolderPairsCache 生效测试", func(t *testing.T) {
		InvalidateFolderPairsCache(taskID)
		folderPairsCacheMu.RLock()
		_, ok := folderPairsCache[taskID]
		folderPairsCacheMu.RUnlock()
		if ok {
			t.Fatal("expected cache entry to be deleted after InvalidateFolderPairsCache")
		}
	})
}
