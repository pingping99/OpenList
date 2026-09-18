package dedup

import (
	"encoding/json"
	"strings"
	"testing"
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
