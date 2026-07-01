package routes

import (
	"github.com/Wei-Shaw/sub2api/internal/handler"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

// RegisterMonitorRoutes 注册实时请求/响应监控 WebSocket 路由（fork 增强）。
//
// 挂在顶层 *gin.Engine 上，只继承全局 Recovery/Logger/CORS/SecurityHeaders，
// 不经过 apiKeyAuth / bodyLimit —— 鉴权由 WebSocket 握手时 Sec-WebSocket-Protocol
// 里携带的 api key（"key.<plaintext>"）完成。
func RegisterMonitorRoutes(r *gin.Engine, hub *service.MonitorHub, apiKeyService *service.APIKeyService) {
	r.GET("/v1/monitor/requests", handler.MonitorWSHandler(hub, apiKeyService))
}
