package dedup

import (
	"context"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
)

func TestScan_MinSizeFilter(t *testing.T) {
	tree := map[string][]model.Obj{
		"/": {
			fileObj("small1.txt", 50, utils.MD5, "hash1"),
			fileObj("small2.txt", 50, utils.MD5, "hash1"),
			fileObj("large1.bin", 200, utils.MD5, "hash2"),
			fileObj("large2.bin", 200, utils.MD5, "hash2"),
		},
	}

	// 1. 无过滤：应该扫出 2 组
	p1 := &Progress{}
	groups1, _, _, err := scan(context.Background(), ScanConfig{RootPath: "/", MinSize: 0, Concurrency: 1, QPS: 1000}, p1, fakeLister(tree))
	if err != nil {
		t.Fatalf("scan failed: %v", err)
	}
	if len(groups1) != 2 {
		t.Fatalf("expected 2 groups without filter, got %d", len(groups1))
	}

	// 2. MinSize = 100：small1/small2 (50字节) 应当被过滤，仅留下 large1/large2
	p2 := &Progress{}
	groups2, _, _, err := scan(context.Background(), ScanConfig{RootPath: "/", MinSize: 100, Concurrency: 1, QPS: 1000}, p2, fakeLister(tree))
	if err != nil {
		t.Fatalf("scan with MinSize failed: %v", err)
	}
	if len(groups2) != 1 {
		t.Fatalf("expected 1 group with MinSize=100, got %d", len(groups2))
	}
	if groups2[0].Size != 200 {
		t.Fatalf("expected size 200 group, got %d", groups2[0].Size)
	}
}

func TestScan_ExtFilters(t *testing.T) {
	tree := map[string][]model.Obj{
		"/": {
			fileObj("video1.mp4", 100, utils.MD5, "h_video"),
			fileObj("video2.mp4", 100, utils.MD5, "h_video"),
			fileObj("image1.png", 50, utils.MD5, "h_image"),
			fileObj("image2.png", 50, utils.MD5, "h_image"),
			fileObj("doc1.pdf", 70, utils.MD5, "h_doc"),
			fileObj("doc2.pdf", 70, utils.MD5, "h_doc"),
		},
	}

	// 1. IncludeExts = ["mp4"]
	p1 := &Progress{}
	groups1, _, _, err := scan(context.Background(), ScanConfig{
		RootPath:    "/",
		IncludeExts: []string{".MP4"}, // 测试大小写与前导点清理
		Concurrency: 1,
		QPS:         1000,
	}, p1, fakeLister(tree))
	if err != nil {
		t.Fatalf("scan failed: %v", err)
	}
	if len(groups1) != 1 {
		t.Fatalf("expected 1 group, got %d", len(groups1))
	}
	if groups1[0].Files[0].Name != "video1.mp4" {
		t.Fatalf("expected mp4 group, got %s", groups1[0].Files[0].Name)
	}

	// 2. ExcludeExts = ["pdf", "png"]
	p2 := &Progress{}
	groups2, _, _, err := scan(context.Background(), ScanConfig{
		RootPath:    "/",
		ExcludeExts: []string{"pdf", "png"},
		Concurrency: 1,
		QPS:         1000,
	}, p2, fakeLister(tree))
	if err != nil {
		t.Fatalf("scan failed: %v", err)
	}
	if len(groups2) != 1 {
		t.Fatalf("expected 1 group, got %d", len(groups2))
	}
	if groups2[0].Files[0].Name != "video1.mp4" {
		t.Fatalf("expected video1.mp4, got %s", groups2[0].Files[0].Name)
	}
}
