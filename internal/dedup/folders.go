package dedup

import (
	"context"
	"fmt"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/fs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	log "github.com/sirupsen/logrus"
)

// FindDuplicateFolders 查找当前任务下重合度 >= threshold 的重复文件夹对
func FindDuplicateFolders(taskID string, threshold float64) ([]DupFolderPair, error) {
	if threshold <= 0 {
		threshold = 0.30 // 默认 30% 阈值
	}

	// 1. 读取本任务全部已校验的重复文件（Verified=true）
	var items []DedupFileItem
	if err := db.GetDb().
		Where("task_id = ? AND verified = ?", taskID, true).
		Order("group_key, path ASC").
		Find(&items).Error; err != nil {
		return nil, err
	}
	if len(items) == 0 {
		return []DupFolderPair{}, nil
	}

	// 2. 读取各目录的统计信息（总文件数、总大小）
	dirStats, _ := GetDirStats(taskID)
	return computeFolderPairs(items, dirStats, threshold), nil
}

// computeFolderPairs 核心算法：按目录聚合重复文件，计算两两目录的重合度
func computeFolderPairs(items []DedupFileItem, dirStats map[string]*DirStat, threshold float64) []DupFolderPair {
	if threshold <= 0 {
		threshold = 0.30
	}
	if dirStats == nil {
		dirStats = make(map[string]*DirStat)
	}

	// 1. 按所在父目录对重复文件分组，同时构建组键到目录的倒排索引
	dirToItems := make(map[string][]DedupFileItem)
	dirToGroups := make(map[string]map[string][]DedupFileItem)
	groupToDirs := make(map[string][]string)
	groupDirSeen := make(map[string]map[string]bool)

	for _, it := range items {
		d := path.Dir(it.Path)
		dirToItems[d] = append(dirToItems[d], it)
		if dirToGroups[d] == nil {
			dirToGroups[d] = make(map[string][]DedupFileItem)
		}
		dirToGroups[d][it.GroupKey] = append(dirToGroups[d][it.GroupKey], it)

		if groupDirSeen[it.GroupKey] == nil {
			groupDirSeen[it.GroupKey] = make(map[string]bool)
		}
		if !groupDirSeen[it.GroupKey][d] {
			groupDirSeen[it.GroupKey][d] = true
			groupToDirs[it.GroupKey] = append(groupToDirs[it.GroupKey], d)
		}
	}

	// 2. 通过倒排索引找出所有存在共同重复文件的候选目录对 (避免 O(D^2) 暴力全排列对比)
	type dirPairKey struct {
		dirA string
		dirB string
	}
	candidatePairs := make(map[dirPairKey]bool)

	for _, dirsInGroup := range groupToDirs {
		if len(dirsInGroup) < 2 {
			continue
		}
		for i := 0; i < len(dirsInGroup); i++ {
			dA := dirsInGroup[i]
			for j := i + 1; j < len(dirsInGroup); j++ {
				dB := dirsInGroup[j]
				if dA == dB {
					continue
				}
				key := dirPairKey{dirA: dA, dirB: dB}
				if key.dirA > key.dirB {
					key.dirA, key.dirB = dB, dA
				}
				candidatePairs[key] = true
			}
		}
	}

	var pairs []DupFolderPair

	// 3. 对每个候选目录对计算重合度
	for cand := range candidatePairs {
		dirA := cand.dirA
		dirB := cand.dirB

		// 排除同名或存在层级包含关系的祖先与后代目录
		if dirA == dirB || utils.IsSubPath(dirA, dirB) || utils.IsSubPath(dirB, dirA) {
			continue
		}

		groupsA := dirToGroups[dirA]
		groupsB := dirToGroups[dirB]

		// 找出 A 和 B 共享的重复文件
		var matched []DupFolderMatchedFile
		var dupBytes int64
		dupCountA := 0
		dupCountB := 0

		for gKey, filesInA := range groupsA {
			filesInB, ok := groupsB[gKey]
			if !ok || len(filesInB) == 0 {
				continue
			}

			matchLen := len(filesInA)
			if len(filesInB) < matchLen {
				matchLen = len(filesInB)
			}
			dupCountA += matchLen
			dupCountB += matchLen

			for k := 0; k < matchLen; k++ {
				matched = append(matched, DupFolderMatchedFile{
					PathA:    filesInA[k].Path,
					PathB:    filesInB[k].Path,
					NameA:    filesInA[k].Name,
					NameB:    filesInB[k].Name,
					Size:     filesInA[k].Size,
					GroupKey: gKey,
				})
				dupBytes += filesInA[k].Size
			}
		}

		if len(matched) == 0 {
			continue
		}

		// 计算两目录的总文件数
		itemsA := dirToItems[dirA]
		totalA := len(itemsA)
		totalSizeA := int64(0)
		for _, it := range itemsA {
			totalSizeA += it.Size
		}
		if s, ok := dirStats[dirA]; ok && s.FileCount > totalA {
			totalA = s.FileCount
			totalSizeA = s.TotalSize
		}

		itemsB := dirToItems[dirB]
		totalB := len(itemsB)
		totalSizeB := int64(0)
		for _, it := range itemsB {
			totalSizeB += it.Size
		}
		if s, ok := dirStats[dirB]; ok && s.FileCount > totalB {
			totalB = s.FileCount
			totalSizeB = s.TotalSize
		}

		if totalA < len(matched) {
			totalA = len(matched)
		}
		if totalB < len(matched) {
			totalB = len(matched)
		}

		ratioA := float64(dupCountA) / float64(totalA)
		ratioB := float64(dupCountB) / float64(totalB)
		similarity := ratioA
		if ratioB > similarity {
			similarity = ratioB
		}

		// 满足阈值条件（默认 >= 30%）
		if similarity < threshold {
			continue
		}

		pairs = append(pairs, DupFolderPair{
			DirA:          dirA,
			DirB:          dirB,
			TotalFilesA:   totalA,
			TotalFilesB:   totalB,
			TotalSizeA:    totalSizeA,
			TotalSizeB:    totalSizeB,
			DupFilesCount: len(matched),
			DupFilesSize:  dupBytes,
			RatioA:        ratioA,
			RatioB:        ratioB,
			Similarity:    similarity,
			MatchedFiles:  matched,
		})
	}

	// 按重合度降序、重复大小降序排序
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].Similarity != pairs[j].Similarity {
			return pairs[i].Similarity > pairs[j].Similarity
		}
		if pairs[i].DupFilesSize != pairs[j].DupFilesSize {
			return pairs[i].DupFilesSize > pairs[j].DupFilesSize
		}
		return pairs[i].DirA < pairs[j].DirA
	})

	return pairs
}

// MergeFolders 将 SourceDir 合并入 TargetDir（删除 SourceDir 中已重复文件，移动独有文件，清空并删除 SourceDir）
func MergeFolders(ctx context.Context, user *model.User, req MergeFoldersReq) (*MergeFoldersResp, error) {
	if req.TaskID == "" {
		return nil, fmt.Errorf("缺少 task_id")
	}
	srcDir := utils.FixAndCleanPath(req.SourceDir)
	dstDir := utils.FixAndCleanPath(req.TargetDir)

	if srcDir == "" || dstDir == "" {
		return nil, fmt.Errorf("源文件夹与目标文件夹路径不能为空")
	}
	if srcDir == "/" || dstDir == "/" || srcDir == "." || dstDir == "." {
		return nil, fmt.Errorf("不允许对根目录执行合并操作")
	}
	if srcDir == dstDir {
		return nil, fmt.Errorf("源文件夹与目标文件夹不能相同")
	}
	if utils.IsSubPath(srcDir, dstDir) || utils.IsSubPath(dstDir, srcDir) {
		return nil, fmt.Errorf("源文件夹与目标文件夹不能存在层级包含关系")
	}

	task, err := GetTaskByID(req.TaskID)
	if err != nil {
		return nil, fmt.Errorf("任务不存在")
	}
	if !canManageTask(user, task) {
		return nil, fmt.Errorf("无权操作该任务")
	}
	if !utils.IsSubPath(task.RootPath, srcDir) || !utils.IsSubPath(task.RootPath, dstDir) {
		return nil, fmt.Errorf("目录超出本次扫描任务的根路径范围")
	}

	// 权限校验
	pathsToCheck := []string{srcDir, dstDir}
	allowed, rejected := authorizeRemovePaths(ctx, user, task, pathsToCheck)
	if len(rejected) > 0 || len(allowed) < 2 {
		return nil, fmt.Errorf("没有足够的写权限操作指定目录")
	}

	strategy := strings.ToLower(strings.TrimSpace(req.ConflictStrategy))
	if strategy == "" {
		strategy = "rename"
	}

	resp := &MergeFoldersResp{
		SourceDir: srcDir,
		TargetDir: dstDir,
	}

	// 1. 获取两目录内记录的已校验重复文件
	var itemsInTask []DedupFileItem
	if err := db.GetDb().
		Where("task_id = ? AND verified = ?", req.TaskID, true).
		Find(&itemsInTask).Error; err != nil {
		return nil, err
	}

	srcDups := make(map[string]DedupFileItem) // path -> item
	targetGroupKeys := make(map[string]bool)  // group_key present in target

	for _, it := range itemsInTask {
		dir := path.Dir(it.Path)
		if dir == dstDir || utils.IsSubPath(dstDir, dir) {
			targetGroupKeys[it.GroupKey] = true
		}
	}
	for _, it := range itemsInTask {
		dir := path.Dir(it.Path)
		if dir == srcDir || utils.IsSubPath(srcDir, dir) {
			// 如果该文件属于目标目录已有相同的 group_key，则认为是两目录间的重复文件
			if targetGroupKeys[it.GroupKey] {
				srcDups[it.Path] = it
			}
		}
	}

	// 2. 删除源目录中所有的重复文件
	var deletedPaths []string
	var reclaimedBytes int64
	for p, it := range srcDups {
		if err := fs.Remove(ctx, p); err != nil {
			log.Warnf("[dedup] merge: failed to remove duplicate file %s: %v", p, err)
			resp.Errors = append(resp.Errors, fmt.Sprintf("删除重复文件失败: %s (%v)", p, err))
			continue
		}
		deletedPaths = append(deletedPaths, p)
		reclaimedBytes += it.Size
		resp.DeletedDupFiles++
		resp.ReclaimedBytes += it.Size
	}

	// 3. 列举源目录当前剩余的文件/子目录，将独有内容安全移入目标目录
	srcObjs, err := fs.List(ctx, srcDir, &fs.ListArgs{Refresh: true})
	if err == nil {
		for _, obj := range srcObjs {
			srcPath := path.Join(srcDir, obj.GetName())
			dstPath := path.Join(dstDir, obj.GetName())

			if obj.IsDir() {
				// 子目录：如果目标目录已有同名子目录，执行递归合并
				targetSubObj, gErr := fs.Get(ctx, dstPath, &fs.GetArgs{NoLog: true})
				if gErr == nil && targetSubObj.IsDir() {
					_, mErr := fs.Merge(ctx, srcPath, dstDir)
					if mErr != nil {
						resp.Errors = append(resp.Errors, fmt.Sprintf("合并子目录失败: %s (%v)", srcPath, mErr))
					} else {
						resp.MovedUniqueFiles++
					}
					continue
				}
				// 目标无同名子目录，直接移动整个子目录
				if _, mErr := fs.Move(ctx, srcPath, dstDir); mErr != nil {
					resp.Errors = append(resp.Errors, fmt.Sprintf("移动子目录失败: %s (%v)", srcPath, mErr))
				} else {
					resp.MovedUniqueFiles++
				}
				continue
			}

			// 是独有文件：检查目标路径是否已存在同名冲突
			targetObj, gErr := fs.Get(ctx, dstPath, &fs.GetArgs{NoLog: true})
			if gErr == nil && targetObj != nil {
				// 冲突处理
				switch strategy {
				case "overwrite":
					_ = fs.Remove(ctx, dstPath)
					if _, mErr := fs.Move(ctx, srcPath, dstDir); mErr != nil {
						resp.Errors = append(resp.Errors, fmt.Sprintf("覆盖移动文件失败: %s (%v)", srcPath, mErr))
					} else {
						resp.MovedUniqueFiles++
					}
				case "skip":
					continue
				case "rename":
					ext := filepath.Ext(obj.GetName())
					base := strings.TrimSuffix(obj.GetName(), ext)
					newName := fmt.Sprintf("%s (1)%s", base, ext)
					for k := 2; k <= 99; k++ {
						checkP := path.Join(dstDir, newName)
						if _, checkErr := fs.Get(ctx, checkP, &fs.GetArgs{NoLog: true}); checkErr != nil {
							break
						}
						newName = fmt.Sprintf("%s (%d)%s", base, k, ext)
					}
					if rErr := fs.Rename(ctx, srcPath, newName); rErr == nil {
						renamedSrc := path.Join(srcDir, newName)
						if _, mErr := fs.Move(ctx, renamedSrc, dstDir); mErr != nil {
							resp.Errors = append(resp.Errors, fmt.Sprintf("移动重命名文件失败: %s (%v)", renamedSrc, mErr))
						} else {
							resp.MovedUniqueFiles++
						}
					} else {
						resp.Errors = append(resp.Errors, fmt.Sprintf("重命名冲突文件失败: %s (%v)", srcPath, rErr))
					}
				}
			} else {
				// 无同名冲突，直接移动
				if _, mErr := fs.Move(ctx, srcPath, dstDir); mErr != nil {
					resp.Errors = append(resp.Errors, fmt.Sprintf("移动独有文件失败: %s (%v)", srcPath, mErr))
				} else {
					resp.MovedUniqueFiles++
				}
			}
		}
	}

	// 4. 检查源目录是否已清空，如已清空则删除源目录
	remains, listErr := fs.List(ctx, srcDir, &fs.ListArgs{Refresh: true, NoLog: true})
	if listErr == nil && len(remains) == 0 {
		if rErr := fs.Remove(ctx, srcDir); rErr == nil {
			resp.SourceRemoved = true
		} else {
			log.Warnf("[dedup] failed to remove empty source dir %s: %v", srcDir, rErr)
		}
	}

	// 5. 更新任务统计及数据库已清理记录
	if len(deletedPaths) > 0 {
		_ = DeleteFileItems(req.TaskID, deletedPaths)
		_ = DropDanglingGroups(req.TaskID)
		task.CleanedFiles += len(deletedPaths)
		task.CleanedBytes += reclaimedBytes
		if task.DupFiles >= len(deletedPaths) {
			task.DupFiles -= len(deletedPaths)
		} else {
			task.DupFiles = 0
		}
		if task.WastedTotal >= reclaimedBytes {
			task.WastedTotal -= reclaimedBytes
		} else {
			task.WastedTotal = 0
		}
		SaveTask(task)
	}

	return resp, nil
}
