package dedup

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

// TestGuardKeepOneCopy 覆盖「同一组重复文件不能被删光」的服务端兜底逻辑。
// 前端的「整组选中」会把组内所有副本都勾上，这是查重工具里最容易造成内容丢失的操作。
func TestGuardKeepOneCopy(t *testing.T) {
	members := map[string][]string{
		"md5:aaa": {"/a/1.mkv", "/a/1-copy.mkv", "/b/1.mkv"}, // 3 份
		"md5:bbb": {"/a/2.mkv", "/a/2-copy.mkv"},             // 2 份
	}

	t.Run("删除部分副本应当被允许", func(t *testing.T) {
		allowed, rejected := guardKeepOneCopy(members, []string{"/a/1.mkv", "/a/1-copy.mkv"})
		if len(allowed) != 2 || len(rejected) != 0 {
			t.Fatalf("expected 2 allowed / 0 rejected, got %d / %d", len(allowed), len(rejected))
		}
	})

	t.Run("删除某组全部副本必须被拒绝", func(t *testing.T) {
		allowed, rejected := guardKeepOneCopy(members, []string{"/a/2.mkv", "/a/2-copy.mkv"})
		if len(allowed) != 0 {
			t.Fatalf("expected nothing allowed, got %v", allowed)
		}
		if len(rejected) != 2 {
			t.Fatalf("expected 2 rejected, got %d", len(rejected))
		}
		for _, r := range rejected {
			if !strings.Contains(r.Reason, "至少需要保留 1 份") {
				t.Fatalf("unexpected reason: %s", r.Reason)
			}
		}
	})

	t.Run("混合同意与拒绝时按组分别判定", func(t *testing.T) {
		allowed, rejected := guardKeepOneCopy(members, []string{
			"/b/1.mkv",      // 3 份组里的 1 份，允许
			"/a/2.mkv",      // 2 份组的第 1 份，暂时允许
			"/a/2-copy.mkv", // 加上这一份就等于删光 2 份组，两份都要拒绝
		})
		if len(allowed) != 1 || allowed[0] != "/b/1.mkv" {
			t.Fatalf("expected only /b/1.mkv allowed, got %v", allowed)
		}
		if len(rejected) != 2 {
			t.Fatalf("expected 2 rejected, got %d (%v)", len(rejected), rejected)
		}
	})

	t.Run("空输入不产生副作用", func(t *testing.T) {
		allowed, rejected := guardKeepOneCopy(members, nil)
		if len(allowed) != 0 || len(rejected) != 0 {
			t.Fatalf("expected empty result, got %v / %v", allowed, rejected)
		}
	})

	t.Run("不属于任何分组的路径不受影响", func(t *testing.T) {
		allowed, rejected := guardKeepOneCopy(members, []string{"/other/x.srt"})
		if len(allowed) != 1 || len(rejected) != 0 {
			t.Fatalf("expected the path to pass through, got %v / %v", allowed, rejected)
		}
	})
}

// TestIsCompanionOf 的边界用例补充：旧实现使用 HasPrefix，会把同前缀的其它剧集附属文件一起删掉。
func TestGuardCompanionPrefixIsStrict(t *testing.T) {
	cases := []struct {
		obj   string
		video string
		want  bool
	}{
		{"EP01.srt", "EP01.mkv", true},
		{"EP01.zh-CN.srt", "EP01.mkv", true},
		{"EP01.ass", "EP01.mkv", true},
		{"EP01-poster.jpg", "EP01.mkv", false}, // 不同「剧集」的附属文件，不应连带删除
		{"EP011.srt", "EP01.mkv", false},
		{"EP01.mkv", "EP01.mkv", false}, // 主文件本身不是自己的附属文件
		{"EP01.srt", "EP01-S01E01.mkv", false},
	}
	for _, c := range cases {
		if got := isCompanionOf(c.obj, c.video); got != c.want {
			t.Errorf("isCompanionOf(%q, %q) = %v, want %v", c.obj, c.video, got, c.want)
		}
	}
}

func TestRemoveResponseJSONEmptySlices(t *testing.T) {
	var errMsgs []string
	var rejected []removeRejection

	if errMsgs == nil {
		errMsgs = make([]string, 0)
	}
	if rejected == nil {
		rejected = make([]removeRejection, 0)
	}

	payload := map[string]any{
		"errors":   errMsgs,
		"rejected": rejected,
	}

	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	jsonStr := string(data)
	if strings.Contains(jsonStr, "null") {
		t.Fatalf("expected JSON to not contain null for arrays, got: %s", jsonStr)
	}
	if !strings.Contains(jsonStr, `"errors":[]`) || !strings.Contains(jsonStr, `"rejected":[]`) {
		t.Fatalf("expected empty arrays in JSON, got: %s", jsonStr)
	}
}

func TestDedupTask_InitialAndCleanedCounters(t *testing.T) {
	task := DedupTask{
		ID:               "task-test",
		InitialDupGroups: 3,
		InitialDupFiles:  6,
		InitialWasted:    600,
		DupGroups:        1,
		DupFiles:         2,
		WastedTotal:      200,
	}
	if task.InitialDupFiles >= task.DupFiles {
		task.CleanedFiles = task.InitialDupFiles - task.DupFiles
	}
	if task.InitialWasted >= task.WastedTotal {
		task.CleanedBytes = task.InitialWasted - task.WastedTotal
	}
	if task.CleanedFiles != 4 {
		t.Errorf("expected 4 cleaned files, got %d", task.CleanedFiles)
	}
	if task.CleanedBytes != 400 {
		t.Errorf("expected 400 cleaned bytes, got %d", task.CleanedBytes)
	}
}

func TestResultGroupsQuery_KeywordSearch(t *testing.T) {
	dB, err := gorm.Open(sqlite.Open("file:mem_result_groups?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open memory db: %v", err)
	}
	conf.Conf = conf.DefaultConfig("data")
	db.Init(dB)
	if err := db.AutoMigrate(&DedupFileItem{}); err != nil {
		t.Fatalf("failed to migrate DedupFileItem: %v", err)
	}

	taskID := "task-kw-test"
	now := time.Now()

	// 准备测试数据
	items := []DedupFileItem{
		// 组 1: sha1:111, Verified: true, 大小 1000
		{TaskID: taskID, GroupKey: "sha1:111", Verified: true, Path: "/movies/IronMan.mp4", Name: "IronMan.mp4", Size: 1000, Modified: now},
		{TaskID: taskID, GroupKey: "sha1:111", Verified: true, Path: "/backup/IronMan.mp4", Name: "IronMan.mp4", Size: 1000, Modified: now},

		// 组 2: sha1:222, Verified: true, 大小 2000
		{TaskID: taskID, GroupKey: "sha1:222", Verified: true, Path: "/movies/SpiderMan.mkv", Name: "SpiderMan.mkv", Size: 2000, Modified: now},
		{TaskID: taskID, GroupKey: "sha1:222", Verified: true, Path: "/archive/SpiderMan.mkv", Name: "SpiderMan.mkv", Size: 2000, Modified: now},

		// 组 3: name_size:333, Verified: false, 大小 3000
		{TaskID: taskID, GroupKey: "name_size:333", Verified: false, Path: "/downloads/Batman.avi", Name: "Batman.avi", Size: 3000, Modified: now},
		{TaskID: taskID, GroupKey: "name_size:333", Verified: false, Path: "/videos/Batman.avi", Name: "Batman.avi", Size: 3000, Modified: now},

		// 组 4: sha1:444, Verified: true, 大小 500
		{TaskID: taskID, GroupKey: "sha1:444", Verified: true, Path: "/docs/invoice_2026.pdf", Name: "invoice_2026.pdf", Size: 500, Modified: now},
		{TaskID: taskID, GroupKey: "sha1:444", Verified: true, Path: "/backup/invoice_2026.pdf", Name: "invoice_2026.pdf", Size: 500, Modified: now},

		// 孤立文件（不是重复组）
		{TaskID: taskID, GroupKey: "sha1:999", Verified: true, Path: "/single/unique.txt", Name: "unique.txt", Size: 100, Modified: now},
	}
	if err := db.GetDb().Create(&items).Error; err != nil {
		t.Fatalf("failed to insert test items: %v", err)
	}

	trueVal := true
	falseVal := false

	t.Run("无关键词时返回所有已验证重复分组并按可释放空间降序排序", func(t *testing.T) {
		var keys []string
		err := resultGroupsQuery(taskID, &trueVal, "").
			Select("group_key").
			Group("group_key").
			Having("COUNT(*) > 1").
			Order("MAX(size) * (COUNT(*) - 1) DESC").
			Pluck("group_key", &keys).Error
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if len(keys) != 3 {
			t.Fatalf("expected 3 verified groups, got %d: %v", len(keys), keys)
		}
		// 排序验证: sha1:222 (2000) > sha1:111 (1000) > sha1:444 (500)
		if keys[0] != "sha1:222" || keys[1] != "sha1:111" || keys[2] != "sha1:444" {
			t.Fatalf("unexpected order: %v", keys)
		}

		var total int64
		err = resultGroupsQuery(taskID, &trueVal, "").
			Select("group_key").
			Group("group_key").
			Having("COUNT(*) > 1").
			Count(&total).Error
		if err != nil {
			t.Fatalf("count failed: %v", err)
		}
		if total != 3 {
			t.Fatalf("expected total 3, got %d", total)
		}
	})

	t.Run("按文件名模糊检索", func(t *testing.T) {
		var keys []string
		err := resultGroupsQuery(taskID, &trueVal, "Iron").
			Select("group_key").
			Group("group_key").
			Having("COUNT(*) > 1").
			Pluck("group_key", &keys).Error
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if len(keys) != 1 || keys[0] != "sha1:111" {
			t.Fatalf("expected ['sha1:111'], got %v", keys)
		}

		var total int64
		err = resultGroupsQuery(taskID, &trueVal, "Iron").
			Select("group_key").
			Group("group_key").
			Having("COUNT(*) > 1").
			Count(&total).Error
		if err != nil {
			t.Fatalf("count failed: %v", err)
		}
		if total != 1 {
			t.Fatalf("expected total 1, got %d", total)
		}
	})

	t.Run("按路径模糊检索", func(t *testing.T) {
		var keys []string
		err := resultGroupsQuery(taskID, &trueVal, "backup").
			Select("group_key").
			Group("group_key").
			Having("COUNT(*) > 1").
			Order("MAX(size) * (COUNT(*) - 1) DESC").
			Pluck("group_key", &keys).Error
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		// 命中 sha1:111 (/backup/IronMan.mp4) 和 sha1:444 (/backup/invoice_2026.pdf)
		if len(keys) != 2 || keys[0] != "sha1:111" || keys[1] != "sha1:444" {
			t.Fatalf("expected ['sha1:111', 'sha1:444'], got %v", keys)
		}
	})

	t.Run("未确认候选组中检索", func(t *testing.T) {
		var keys []string
		err := resultGroupsQuery(taskID, &falseVal, "Batman").
			Select("group_key").
			Group("group_key").
			Having("COUNT(*) > 1").
			Pluck("group_key", &keys).Error
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if len(keys) != 1 || keys[0] != "name_size:333" {
			t.Fatalf("expected ['name_size:333'], got %v", keys)
		}

		// 在已确认组中检索 Batman 应当为空
		var verifiedKeys []string
		err = resultGroupsQuery(taskID, &trueVal, "Batman").
			Select("group_key").
			Group("group_key").
			Having("COUNT(*) > 1").
			Pluck("group_key", &verifiedKeys).Error
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if len(verifiedKeys) != 0 {
			t.Fatalf("expected 0 verified keys for Batman, got %v", verifiedKeys)
		}
	})

	t.Run("按 group_key 特征码模糊检索", func(t *testing.T) {
		var keys []string
		err := resultGroupsQuery(taskID, &trueVal, "222").
			Select("group_key").
			Group("group_key").
			Having("COUNT(*) > 1").
			Pluck("group_key", &keys).Error
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if len(keys) != 1 || keys[0] != "sha1:222" {
			t.Fatalf("expected ['sha1:222'], got %v", keys)
		}
	})

	t.Run("检索无匹配内容", func(t *testing.T) {
		var total int64
		err := resultGroupsQuery(taskID, &trueVal, "NonExistentWord").
			Select("group_key").
			Group("group_key").
			Having("COUNT(*) > 1").
			Count(&total).Error
		if err != nil {
			t.Fatalf("count failed: %v", err)
		}
		if total != 0 {
			t.Fatalf("expected total 0, got %d", total)
		}
	})
}

