package dedup

import (
	"github.com/gin-gonic/gin"
)

// RegisterRouter 注册查重模块的专属 API
func RegisterRouter(engine *gin.Engine) {
	// 服务启动时初始化数据库表、标记中断任务
	InitDB()
	MarkInterruptedTasks()

	// 专属 API 分组
	g := engine.Group("/api/dedup")
	{
		g.POST("/start", HandleStartScan)
		g.GET("/status", HandleGetStatus)
		g.POST("/cancel", HandleCancelScan)
		g.GET("/result", HandleGetResult)
		g.POST("/remove", HandleBatchRemove)
		g.GET("/dirs", HandleListDirs)
		g.GET("/history", HandleListHistory)
		g.GET("/history/:id", HandleGetHistoryDetail)
	}
}
