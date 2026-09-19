package dedup

import (
	"github.com/gin-gonic/gin"
)

// RegisterRouter 在给定的路由组下注册查重模块 API。
//
// 调用方必须传入已挂载鉴权中间件的路由组（见 server/router.go 中的
// auth.Group("/dedup", middlewares.AuthNotGuest)）。旧实现直接挂在根引擎上，
// 使 /api/dedup/remove 等接口完全无鉴权对外暴露，可被任意人删除文件、枚举目录（P0-1）。
func RegisterRouter(g *gin.RouterGroup) {
	g.POST("/start", HandleStartScan)
	g.GET("/status", HandleGetStatus)
	g.POST("/cancel", HandleCancelScan)
	g.GET("/result", HandleGetResult)
	g.POST("/remove", HandleBatchRemove)
	g.GET("/dirs", HandleListDirs)
	g.GET("/folders", HandleGetDuplicateFolders)
	g.GET("/folders/files", HandleGetFolderFiles)
	g.POST("/folders/merge", HandleMergeFolders)
	g.GET("/history", HandleListHistory)
	g.GET("/history/:id", HandleGetHistoryDetail)
	g.DELETE("/history/:id", HandleDeleteHistory)
	g.POST("/history/clear-empty", HandleClearEmptyHistory)
}
