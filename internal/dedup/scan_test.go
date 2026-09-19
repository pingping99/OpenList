package dedup

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
)

func dirObj(name string) *model.Object {
	return &model.Object{Name: name, IsFolder: true}
}

// fileObj 构造测试文件；hash 为空表示该存储不提供哈希（如本地/WebDAV 驱动）
func fileObj(name string, size int64, hashType *utils.HashType, hash string) *model.Object {
	obj := &model.Object{
		Name:     name,
		Size:     size,
		Modified: time.Unix(1700000000, 0),
	}
	if hashType != nil && hash != "" {
		obj.HashInfo = utils.NewHashInfo(hashType, hash)
	}
	return obj
}

func fakeLister(tree map[string][]model.Obj) dirLister {
	return func(_ context.Context, dir string) ([]model.Obj, error) {
		objs, ok := tree[dir]
		if !ok {
			return nil, fmt.Errorf("no such dir: %s", dir)
		}
		return objs, nil
	}
}

func findGroup(groups []DupGroup, key string) (DupGroup, bool) {
	for _, g := range groups {
		if g.GroupKey == key {
			return g, true
		}
	}
	return DupGroup{}, false
}

// TestScan_VerifiedGroupingAndP0_2 覆盖 P0-2：
//   - 同尺寸但哈希不同的文件，绝不能被判为重复；
//   - 存储不提供哈希的文件，只能作为「同名同尺寸」候选（Verified=false），
//     且不同名字的同尺寸文件不会被聚成一组（旧实现会因为 HashInfo.String() == "null"
//     把它们全部归到同一个 hash 下，误报为重复文件）。
func TestScan_VerifiedGroupingAndP0_2(t *testing.T) {
	tree := map[string][]model.Obj{
		"/": {dirObj("a"), dirObj("b")},
		"/a": {
			fileObj("movie.mkv", 100, utils.MD5, "aaaa"),
			fileObj("other.mkv", 100, utils.MD5, "bbbb"), // 同尺寸不同哈希
			fileObj("local1.mkv", 500, nil, ""),          // 无哈希
			fileObj("unique.mkv", 700, nil, ""),          // 无哈希且无同名候选
		},
		"/b": {
			fileObj("movie.mkv", 100, utils.MD5, "aaaa"), // 与 /a/movie.mkv 内容相同
			fileObj("third.mkv", 100, utils.MD5, "cccc"),
			fileObj("local1.mkv", 500, nil, ""), // 与 /a/local1.mkv 同名同尺寸
		},
	}

	progress := &Progress{}
	groups, _, err := scan(context.Background(), ScanConfig{RootPath: "/", Concurrency: 4, QPS: 1000}, progress, fakeLister(tree))
	if err != nil {
		t.Fatalf("scan failed: %v", err)
	}

	// 1) 唯一一个已校验分组：/a/movie.mkv 与 /b/movie.mkv
	var verified []DupGroup
	for _, g := range groups {
		if g.Verified {
			verified = append(verified, g)
		}
	}
	if len(verified) != 1 {
		t.Fatalf("应只有 1 个已校验重复组，实际 %d：%+v", len(verified), verified)
	}
	if verified[0].GroupKey != "md5:AAAA" || len(verified[0].Files) != 2 {
		t.Fatalf("已校验分组内容不符: %+v", verified[0])
	}
	if verified[0].WastedBytes != 100 {
		t.Fatalf("可释放空间应为 100，实际 %d", verified[0].WastedBytes)
	}

	// 2) P0-2 核心断言：同尺寸但哈希不同的文件不能被判为重复
	for _, g := range verified {
		for _, f := range g.Files {
			if f.Name == "other.mkv" || f.Name == "third.mkv" {
				t.Fatalf("同尺寸但哈希不同的文件被判为重复: %+v", g)
			}
		}
	}

	// 3) 无哈希文件只能出现在未校验候选组里，且必须同名同尺寸
	var candidates []DupGroup
	for _, g := range groups {
		if !g.Verified {
			candidates = append(candidates, g)
		}
	}
	if len(candidates) != 1 {
		t.Fatalf("应只有 1 个候选组（local1.mkv），实际 %d: %+v", len(candidates), candidates)
	}
	if len(candidates[0].Files) != 2 || candidates[0].Files[0].Name != "local1.mkv" {
		t.Fatalf("候选组内容不符: %+v", candidates[0])
	}
	if candidates[0].WastedBytes == 0 {
		t.Fatal("候选组应给出可释放空间的估算")
	}

	// 4) 统计口径
	stats := progress.Snapshot()
	if stats.VerifiedFiles != 4 || stats.UnverifiedFiles != 3 {
		t.Fatalf("统计不符: %+v", stats)
	}
	if stats.DupGroups != 1 || stats.DupFiles != 2 || stats.WastedBytes != 100 {
		t.Fatalf("重复统计不符: %+v", stats)
	}
	if stats.CandidateGroups != 1 || stats.CandidateFiles != 2 {
		t.Fatalf("候选统计不符: %+v", stats)
	}
}

// TestScan_NoHashStorageNeverReportsDuplicates 锁定最坏情况：
// 整个存储都不提供哈希时（本地/WebDAV 等），同名同尺寸以外的文件都不应被判为重复。
func TestScan_NoHashStorageNeverReportsDuplicates(t *testing.T) {
	tree := map[string][]model.Obj{
		"/":  {dirObj("a"), dirObj("b")},
		"/a": {fileObj("x.mkv", 100, nil, ""), fileObj("y.mkv", 100, nil, "")},
		"/b": {fileObj("z.mkv", 100, nil, "")},
	}
	progress := &Progress{}
	groups, _, err := scan(context.Background(), ScanConfig{RootPath: "/", Concurrency: 2}, progress, fakeLister(tree))
	if err != nil {
		t.Fatalf("scan failed: %v", err)
	}
	if len(groups) != 0 {
		t.Fatalf("无哈希且不同名的同尺寸文件不应产生任何分组，实际: %+v", groups)
	}
	stats := progress.Snapshot()
	if stats.UnverifiedFiles != 3 || stats.DupGroups != 0 || stats.CandidateGroups != 0 {
		t.Fatalf("统计不符: %+v", stats)
	}
}

// TestScan_GCIDPreferred 同一批文件固定使用同一优先级哈希口径
func TestScan_GCIDPreferred(t *testing.T) {
	gcid := testGCID("1")
	md5v := testGCID("2")
	tree := map[string][]model.Obj{
		"/":  {dirObj("a"), dirObj("b")},
		"/a": {fileObj("m.mkv", 10, utils.MD5, md5v)},
		"/b": {fileObj("m.mkv", 10, utils.MD5, md5v)},
	}
	progress := &Progress{}
	groups, _, err := scan(context.Background(), ScanConfig{RootPath: "/"}, progress, fakeLister(tree))
	if err != nil {
		t.Fatalf("scan failed: %v", err)
	}
	if len(groups) != 1 || groups[0].GroupKey != "md5:"+uppercase(md5v) {
		t.Fatalf("仅提供 md5 时应按 md5 分组，实际: %+v", groups)
	}
	_ = gcid
}

// TestScan_RootFailureIsFatal 根目录不可访问时必须返回错误（旧的假成功问题）
func TestScan_RootFailureIsFatal(t *testing.T) {
	lister := func(_ context.Context, dir string) ([]model.Obj, error) {
		return nil, errors.New("boom")
	}
	progress := &Progress{}
	if _, _, err := scan(context.Background(), ScanConfig{RootPath: "/missing"}, progress, lister); err == nil {
		t.Fatal("根目录列举失败时应返回错误")
	}
}

// TestScan_SubDirFailureCounted 子目录失败计入 FailedDirs 且不影响整体结果
func TestScan_SubDirFailureCounted(t *testing.T) {
	tree := map[string][]model.Obj{
		"/":  {dirObj("a"), dirObj("denied")},
		"/a": {fileObj("m.mkv", 10, utils.MD5, "aaaa"), fileObj("m2.mkv", 10, utils.MD5, "aaaa")},
	}
	progress := &Progress{}
	groups, _, err := scan(context.Background(), ScanConfig{RootPath: "/", Concurrency: 2}, progress, fakeLister(tree))
	if err != nil {
		t.Fatalf("子目录失败不应使任务失败: %v", err)
	}
	if len(groups) != 1 {
		t.Fatalf("应得到 1 个重复组，实际 %+v", groups)
	}
	if progress.Snapshot().FailedDirs != 1 {
		t.Fatalf("失败目录数应为 1，实际 %+v", progress.Snapshot())
	}
}

// TestScan_MaxDepth 超出最大深度的子目录不再展开
func TestScan_MaxDepth(t *testing.T) {
	tree := map[string][]model.Obj{
		"/":    {dirObj("a")},
		"/a":   {dirObj("b"), fileObj("t.mkv", 1, utils.MD5, "aaaa")},
		"/a/b": {fileObj("x.mkv", 1, utils.MD5, "aaaa"), fileObj("y.mkv", 1, utils.MD5, "aaaa")},
	}
	progress := &Progress{}
	groups, _, err := scan(context.Background(), ScanConfig{RootPath: "/", MaxDepth: 1, Concurrency: 2}, progress, fakeLister(tree))
	if err != nil {
		t.Fatalf("scan failed: %v", err)
	}
	if len(groups) != 0 {
		t.Fatalf("深度限制应阻止进入 /a/b，实际: %+v", groups)
	}
	if progress.Snapshot().ScannedDirs != 2 {
		t.Fatalf("应只扫描根目录与 /a，实际 %+v", progress.Snapshot())
	}
}

func testGCID(seed string) string {
	return utils.HashData(utils.MD5, []byte("seed-"+seed))
}

func uppercase(s string) string {
	out := make([]rune, 0, len(s))
	for _, r := range s {
		if r >= 'a' && r <= 'z' {
			r = r - 'a' + 'A'
		}
		out = append(out, r)
	}
	return string(out)
}
