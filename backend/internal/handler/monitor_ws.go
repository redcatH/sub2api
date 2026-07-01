package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

// fork 增强：实时 API Key 请求/响应监控 WebSocket 端点。
//
// 客户端在 WebSocket 握手的 Sec-WebSocket-Protocol 里携带被监控的 key：
//   protocols = ["sub2api-monitor", "key.<plaintext-api-key>"]
// 服务端 select "sub2api-monitor"（不回显 key），用 key.<...> 解析出 api key，
// GetByKey + IsActive 校验后订阅其 apiKey.ID 的 channel。

const (
	monitorWSProtocol = "sub2api-monitor"
	keyProtocolPrefix = "key."

	monitorWSCloseInvalidKey = 4003

	monitorWSMaxConns      int32 = 100
	monitorWSMaxConnsPerIP int32 = 20

	monitorWSWriteTimeout = 10 * time.Second
	monitorWSPongWait     = 60 * time.Second
	monitorWSPingInterval = 30 * time.Second
	monitorWSMaxReadBytes = 1024
)

// monitorUpgrader 选择 "sub2api-monitor" 子协议，确保 key.<...> 不在握手响应里回显。
// CheckOrigin 默认放行：依赖 key 自身鉴权（key 走 subprotocol，不出现在 URL），
// 且兼容反代/跨域部署（origin 校验在反代场景容易误拒）。
var monitorUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
	Subprotocols: []string{monitorWSProtocol},
}

var (
	monitorWSConns   atomic.Int32
	monitorWSIPMu    sync.Mutex
	monitorWSIPCount = make(map[string]int32)
)

// extractKeyFromSubprotocol 从 Sec-WebSocket-Protocol 头解析 key.<...> 前缀的 api key。
func extractKeyFromSubprotocol(c *gin.Context) string {
	if c == nil {
		return ""
	}
	raw := strings.TrimSpace(c.GetHeader("Sec-WebSocket-Protocol"))
	if raw == "" {
		return ""
	}
	for _, part := range strings.Split(raw, ",") {
		p := strings.TrimSpace(part)
		if strings.HasPrefix(p, keyProtocolPrefix) {
			token := strings.TrimSpace(strings.TrimPrefix(p, keyProtocolPrefix))
			if token != "" {
				return token
			}
		}
	}
	return ""
}

func tryAcquireMonitorGlobalSlot() bool {
	for {
		cur := monitorWSConns.Load()
		if cur >= monitorWSMaxConns {
			return false
		}
		if monitorWSConns.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}

func tryAcquireMonitorWSSlot(ip string) bool {
	if !tryAcquireMonitorGlobalSlot() {
		return false
	}
	ip = strings.TrimSpace(ip)
	if ip == "" || monitorWSMaxConnsPerIP <= 0 {
		return true
	}
	monitorWSIPMu.Lock()
	defer monitorWSIPMu.Unlock()
	if monitorWSIPCount[ip] >= monitorWSMaxConnsPerIP {
		monitorWSConns.Add(-1)
		return false
	}
	monitorWSIPCount[ip]++
	return true
}

func releaseMonitorWSSlot(ip string) {
	monitorWSConns.Add(-1)
	ip = strings.TrimSpace(ip)
	if ip == "" {
		return
	}
	monitorWSIPMu.Lock()
	defer monitorWSIPMu.Unlock()
	if v := monitorWSIPCount[ip]; v > 1 {
		monitorWSIPCount[ip] = v - 1
	} else {
		delete(monitorWSIPCount, ip)
	}
}

func closeMonitorWS(conn *websocket.Conn, code int, reason string) {
	if conn == nil {
		return
	}
	msg := websocket.FormatCloseMessage(code, reason)
	_ = conn.WriteControl(websocket.CloseMessage, msg, time.Now().Add(monitorWSWriteTimeout))
	_ = conn.Close()
}

// MonitorWSHandler 处理 GET /v1/monitor/requests。
// 通过 WebSocket subprotocol 携带的 api key 鉴权，校验通过后实时下发该 key 的请求/响应快照。
func MonitorWSHandler(hub *service.MonitorHub, apiKeyService *service.APIKeyService) gin.HandlerFunc {
	return func(c *gin.Context) {
		if hub == nil || apiKeyService == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "monitor not initialized"})
			return
		}

		key := extractKeyFromSubprotocol(c)
		if key == "" {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "missing api key"})
			return
		}

		apiKey, err := apiKeyService.GetByKey(c.Request.Context(), key)
		if err != nil || apiKey == nil || !apiKey.IsActive() {
			// upgrade-then-close，避免客户端在 401 上重连死循环。
			conn, uerr := monitorUpgrader.Upgrade(c.Writer, c.Request, nil)
			if uerr != nil {
				c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid api key"})
				return
			}
			closeMonitorWS(conn, monitorWSCloseInvalidKey, "invalid_key")
			return
		}

		if !tryAcquireMonitorWSSlot(c.ClientIP()) {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "too many connections"})
			return
		}
		defer releaseMonitorWSSlot(c.ClientIP())

		conn, err := monitorUpgrader.Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			logger.LegacyPrintf("handler.monitor_ws", "[MonitorWS] upgrade failed: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()

		sub := hub.Subscribe(apiKey.ID)
		defer hub.Unsubscribe(apiKey.ID, sub)

		handleMonitorWebSocket(c.Request.Context(), conn, sub)
	}
}

// handleMonitorWebSocket 读写循环：读循环处理 pong/close；写循环把订阅 channel 的
// 快照 JSON 编码后下发，定时 ping 保活。任一端断开即退出并触发 Unsubscribe（调用方 defer）。
func handleMonitorWebSocket(parentCtx context.Context, conn *websocket.Conn, sub *service.MonitorSubscriber) {
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	var closeOnce sync.Once
	closeConn := func() {
		closeOnce.Do(func() { _ = conn.Close() })
	}

	closeFrameCh := make(chan []byte, 1)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer cancel()

		conn.SetReadLimit(monitorWSMaxReadBytes)
		_ = conn.SetReadDeadline(time.Now().Add(monitorWSPongWait))
		conn.SetPongHandler(func(string) error {
			return conn.SetReadDeadline(time.Now().Add(monitorWSPongWait))
		})
		conn.SetCloseHandler(func(code int, text string) error {
			select {
			case closeFrameCh <- websocket.FormatCloseMessage(code, text):
			default:
			}
			cancel()
			return nil
		})
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	pingTicker := time.NewTicker(monitorWSPingInterval)
	defer pingTicker.Stop()

	writeWithTimeout := func(msgType int, data []byte) error {
		_ = conn.SetWriteDeadline(time.Now().Add(monitorWSWriteTimeout))
		return conn.WriteMessage(msgType, data)
	}
	sendClose := func(cf []byte) {
		if cf == nil {
			cf = websocket.FormatCloseMessage(websocket.CloseNormalClosure, "")
		}
		_ = writeWithTimeout(websocket.CloseMessage, cf)
	}

	ch := sub.Channel()
	done := sub.Done()

	for {
		select {
		case snap := <-ch:
			payload, err := json.Marshal(snap)
			if err != nil {
				logger.LegacyPrintf("handler.monitor_ws", "[MonitorWS] marshal failed: %v", err)
				continue
			}
			if err := writeWithTimeout(websocket.TextMessage, payload); err != nil {
				cancel()
				closeConn()
				wg.Wait()
				return
			}
		case <-pingTicker.C:
			if err := writeWithTimeout(websocket.PingMessage, nil); err != nil {
				cancel()
				closeConn()
				wg.Wait()
				return
			}
		case cf := <-closeFrameCh:
			sendClose(cf)
			closeConn()
			wg.Wait()
			return
		case <-done:
			sendClose(nil)
			closeConn()
			wg.Wait()
			return
		case <-ctx.Done():
			sendClose(nil)
			closeConn()
			wg.Wait()
			return
		}
	}
}
