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
