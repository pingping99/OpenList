package dedup

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	hash_extend "github.com/OpenListTeam/OpenList/v4/pkg/utils/hash"
)

func TestDirWorkQueue_Basic(t *testing.T) {
	q := newDirWorkQueue()
	q.Push(scanDirQueueItem{Path: "/", Depth: 0})

	var wg sync.WaitGroup
	var count int32

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				item, ok := q.Pop()
				if !ok {
					return
				}
				atomic.AddInt32(&count, 1)
				if item.Depth < 4 {
					// 每个目录产生 2 个子目录
					q.Push(scanDirQueueItem{Path: item.Path + "a/", Depth: item.Depth + 1})
					q.Push(scanDirQueueItem{Path: item.Path + "b/", Depth: item.Depth + 1})
				}
				q.Done()
			}
		}()
	}

	wg.Wait()
	// 1 + 2 + 4 + 8 + 16 = 31 个节点
	if count != 31 {
		t.Fatalf("expected 31 items processed, got %d", count)
	}
}

func TestDirWorkQueue_Cancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	q := newDirWorkQueue()

	go func() {
		<-ctx.Done()
		q.Close()
	}()

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				_, ok := q.Pop()
				if !ok {
					return
				}
			}
		}()
	}

	time.Sleep(20 * time.Millisecond)
	cancel() // 唤醒所有阻塞在 Pop 上的 Worker
	wg.Wait()
}

// TestResolveHash_NoHashMustBeEmpty 覆盖 P0-2：驱动不提供哈希时，
// 绝不能退化成 HashInfo.String()（零值序列化为字符串 "null"）而把文件当成「有哈希」。
func TestResolveHash_NoHashMustBeEmpty(t *testing.T) {
	plain := &model.Object{Name: "a.mkv", Size: 123}
	hashType, hashValue := resolveHash(plain)
	if hashType != "" || hashValue != "" {
		t.Fatalf("无哈希文件必须返回空值，实际得到 (%q, %q)", hashType, hashValue)
	}
	// 显式锁定旧实现在这里得到的错误结果，防止回归
	if plain.GetHash().String() != "null" {
		t.Fatalf("前置假设变化：零值 HashInfo.String() = %q", plain.GetHash().String())
	}
}

func TestResolveHash_WithHashes(t *testing.T) {
	md5Only := &model.Object{
		Name:     "b.mkv",
		HashInfo: utils.NewHashInfo(utils.MD5, "d41d8cd98f00b204e9800998ecf8427e"),
	}
	hashType, hashValue := resolveHash(md5Only)
	if hashType != utils.MD5.Name || hashValue != "D41D8CD98F00B204E9800998ECF8427E" {
		t.Fatalf("md5 解析错误: (%q, %q)", hashType, hashValue)
	}

	// 同时提供 GCID 与 MD5 时，固定优先使用 GCID，保证同一批文件口径一致
	both := &model.Object{
		Name: "c.mkv",
		HashInfo: utils.NewHashInfoByMap(map[*utils.HashType]string{
			utils.MD5:        "d41d8cd98f00b204e9800998ecf8427e",
			hash_extend.GCID: "0123456789abcdef0123456789abcdef01234567",
			utils.SHA256:     "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		}),
	}
	hashType, hashValue = resolveHash(both)
	if hashType != hash_extend.GCID.Name {
		t.Fatalf("应优先使用 GCID，实际 %q", hashType)
	}
	if hashValue != "0123456789ABCDEF0123456789ABCDEF01234567" {
		t.Fatalf("哈希值应转为大写，实际 %q", hashValue)
	}
}

// TestGroupKeyAlgorithmsDontMix 保证不同算法的哈希不会落进同一个分组键
func TestGroupKeyAlgorithmsDontMix(t *testing.T) {
	same := "d41d8cd98f00b204e9800998ecf8427e"
	if verifiedGroupKey("md5", same) == verifiedGroupKey("sha1", same) {
		t.Fatal("不同算法的同值哈希不应产生相同的分组键")
	}
}

func TestCandidateGroupKey(t *testing.T) {
	a := candidateGroupKey("Movie.mkv", 1024)
	b := candidateGroupKey("movie.MKV", 1024) // 大小写不敏感
	if a != b {
		t.Fatalf("同名（忽略大小写）同尺寸应得到相同候选键: %q vs %q", a, b)
	}
	if a == candidateGroupKey("Movie.mkv", 1025) {
		t.Fatal("不同尺寸不应得到相同候选键")
	}
	if a == candidateGroupKey("Other.mkv", 1024) {
		t.Fatal("不同文件名不应得到相同候选键")
	}
}

func TestNormalizeScanConfig(t *testing.T) {
	cfg := normalizeScanConfig(ScanConfig{RootPath: "movies//sub/../", Concurrency: 99, QPS: 9999, MaxDepth: 999})
	if cfg.RootPath != "/movies" {
		t.Fatalf("路径应被清理，实际 %q", cfg.RootPath)
	}
	if cfg.Concurrency != maxConcurrency {
		t.Fatalf("并发应被收敛到 %d，实际 %d", maxConcurrency, cfg.Concurrency)
	}
	if cfg.QPS != maxQPS {
		t.Fatalf("QPS 应被收敛到 %v，实际 %v", maxQPS, cfg.QPS)
	}
	if cfg.MaxDepth != maxMaxDepth {
		t.Fatalf("深度应被收敛到 %d，实际 %d", maxMaxDepth, cfg.MaxDepth)
	}

	defaults := normalizeScanConfig(ScanConfig{})
	if defaults.Concurrency != defaultConcurrency || defaults.QPS != defaultQPS || defaults.MaxDepth != defaultMaxDepth {
		t.Fatalf("默认值不正确: %+v", defaults)
	}
	if defaults.RootPath != "/" {
		t.Fatalf("空路径应默认根目录，实际 %q", defaults.RootPath)
	}
}

// TestIsCompanionOf 覆盖旧的 HasPrefix 过宽匹配问题
func TestIsCompanionOf(t *testing.T) {
	cases := []struct {
		obj   string
		video string
		want  bool
	}{
		{"EP01.srt", "EP01.mkv", true},
		{"EP01.zh.srt", "EP01.mkv", true},
		{"EP01.ass", "EP01.mkv", true},
		{"EP01.jpg", "EP01.mkv", true},
		{"EP01-其他版本.srt", "EP01.mkv", false},
		{"EP01extra.srt", "EP01.mkv", false},
		{"EP010.srt", "EP01.mkv", false},
		{"EP02.srt", "EP01.mkv", false},
		{"EP01.mkv", "EP01.mkv", false},   // 本体不是附属文件
		{"EP01.mp4", "EP01.mkv", false},   // 视频扩展名不在附属列表内
		{"ep01.zh.srt", "EP01.mkv", true}, // 附属文件大小写不敏感
	}
	for _, c := range cases {
		if got := isCompanionOf(c.obj, c.video); got != c.want {
			t.Errorf("isCompanionOf(%q, %q) = %v, want %v", c.obj, c.video, got, c.want)
		}
	}
}

// TestProgressSnapshot 保证进度结构在并发读写下的快照单调递增（不回退、不出现撕裂值）
func TestProgressSnapshot(t *testing.T) {
	p := &Progress{}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				p.scannedDirs.Add(1)
				p.scannedFiles.Add(2)
				p.verifiedFiles.Add(1)
				p.unverifiedFiles.Add(1)
			}
		}()
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		var last ScanStats
		for i := 0; i < 200; i++ {
			snap := p.Snapshot()
			if snap.ScannedFiles < last.ScannedFiles || snap.ScannedDirs < last.ScannedDirs {
				t.Errorf("进度出现回退: %+v -> %+v", last, snap)
				return
			}
			if snap.VerifiedFiles > snap.ScannedFiles {
				t.Errorf("校验文件数不应超过扫描文件数: %+v", snap)
				return
			}
			last = snap
		}
	}()
	wg.Wait()
	<-done

	snap := p.Snapshot()
	if snap.ScannedDirs != 8000 || snap.ScannedFiles != 16000 {
		t.Fatalf("计数错误: %+v", snap)
	}
	if snap.ScannedFiles != snap.VerifiedFiles+snap.UnverifiedFiles {
		t.Fatalf("扫描结束后统计应自洽: %+v", snap)
	}
}
