package dedup

import (
	_ "embed"

	"github.com/gin-gonic/gin"
)

//go:embed web/index.html
var uiHTML string

// RegisterRouter 注册查重模块的路由和内嵌页面
func RegisterRouter(engine *gin.Engine) {
	// 独立 UI 访问入口: http://host:port/dedup
	engine.GET("/dedup", func(c *gin.Context) {
		c.Header("Content-Type", "text/html; charset=utf-8")
		c.String(200, uiHTML)
	})

	// 专属 API 分组
	g := engine.Group("/api/dedup")
	{
		g.POST("/start", HandleStartScan)
		g.GET("/status", HandleGetStatus)
		g.POST("/cancel", HandleCancelScan)
		g.POST("/remove", HandleBatchRemove)
		g.GET("/dirs", HandleListDirs)
	}
}
