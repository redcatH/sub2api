package middleware

import (
	"bytes"
	"io"
	"net/http"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/Wei-Shaw/sub2api/internal/pkg/httputil"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

// monitorCaptureWriter 包装 gin.ResponseWriter，在向客户端转发响应的同时，
// 把响应字节缓冲到 buf（截断到 limit）。与 opsCaptureWriter 的区别：
// 无条件缓冲（不限 status>=400），用于实时监控所有响应。
//
// 单请求内 gin 串行调用 Write/WriteString，故 buf 无需加锁（与 opsCaptureWriter 一致）。
type monitorCaptureWriter struct {
	gin.ResponseWriter
	limit int
	buf   bytes.Buffer
	full  bool // 已缓冲到上限，后续 Write 跳过缓冲
}

func (w *monitorCaptureWriter) capture(p []byte) {
	if w.full || w.limit <= 0 || w.buf.Len() >= w.limit {
		return
	}
	remaining := w.limit - w.buf.Len()
	if len(p) > remaining {
		_, _ = w.buf.Write(p[:remaining])
		w.full = true
		return
	}
	_, _ = w.buf.Write(p)
}

func (w *monitorCaptureWriter) Write(b []byte) (int, error) {
	w.capture(b)
	return w.ResponseWriter.Write(b)
}

func (w *monitorCaptureWriter) WriteString(s string) (int, error) {
	w.capture([]byte(s))
	return w.ResponseWriter.WriteString(s)
}

// snapshotBody 将 body 字节截断到 MonitorMaxBodySnapshot 并转为字符串。
func snapshotBody(b []byte) string {
	if len(b) > service.MonitorMaxBodySnapshot {
		return string(b[:service.MonitorMaxBodySnapshot])
	}
	return string(b)
}

// MonitorTap 是实时请求/响应监控的采集中间件（fork 增强）。
//
// 必须挂在 apiKeyAuth 之后（需要从 context 取 apiKey.ID）。
// 零空闲：当 hub 无任何订阅者时，仅做一次 atomic 计数检查 + context lookup 后直接 c.Next()，
// 不读 body、不包装 writer，对正常请求近乎无成本。
//
// 有订阅者时：读 request body 并回填（下游 handler 读到完全相同的字节）→ 包装 c.Writer
// 为 capture writer → c.Next() → 发布配对快照（request body + 最终响应 body + status + 耗时）。
//
// 注意：读 body 失败必须 AbortWithError 传播（否则 RequestBodyLimit 的 MaxBytesReader
// 限制会被静默绕过）。
func MonitorTap(hub *service.MonitorHub) gin.HandlerFunc {
	return func(c *gin.Context) {
		apiKey, ok := GetAPIKeyFromContext(c)
		if hub == nil || !ok || apiKey == nil || !hub.HasSubscribers(apiKey.ID) {
			c.Next()
			return
		}

		body, err := httputil.ReadRequestBodyWithPrealloc(c.Request)
		if err != nil {
			AbortWithError(c, http.StatusRequestEntityTooLarge, "REQUEST_TOO_LARGE", "request body too large")
			return
		}
		// 回填：下游 handler 调用同一个 ReadRequestBodyWithPrealloc 时，
		// Content-Encoding 已被删除 → enc=="" 分支直接返回 raw，字节完全一致。
		c.Request.Body = io.NopCloser(bytes.NewReader(body))

		originalWriter := c.Writer
		cw := &monitorCaptureWriter{
			ResponseWriter: c.Writer,
			limit:          service.MonitorMaxBodySnapshot,
		}
		c.Writer = cw
		// 必须 defer 还原 c.Writer：本中间件运行在 opsErrorLogger 之后，
		// opsErrorLogger 的 defer 用 `if c.Writer == w` 条件恢复 + releaseOpsCaptureWriter(w)
		// （后者会把 w.ResponseWriter 置 nil 放回 pool）。若不还原，条件失败 → w 被置 nil，
		// 此时若 handler panic，全局 Recovery 写 500 会走到 monitorCaptureWriter → 已 nil 的 w → panic。
		defer func() {
			if c.Writer == cw {
				c.Writer = originalWriter
			}
		}()

		start := time.Now()
		c.Next()
		duration := time.Since(start).Milliseconds()

		model := ""
		if v := gjson.GetBytes(body, "model"); v.Exists() {
			model = v.String()
		}
		clientReqID, _ := c.Request.Context().Value(ctxkey.ClientRequestID).(string)

		hub.Publish(apiKey.ID, service.MonitorSnapshot{
			AtMs:            time.Now().UnixMilli(),
			Method:          c.Request.Method,
			Path:            c.Request.URL.Path,
			Model:           model,
			ClientRequestID: clientReqID,
			Body:            snapshotBody(body),
			Status:          cw.Status(),
			ResponseBody:    snapshotBody(cw.buf.Bytes()),
			DurationMs:      duration,
		})
	}
}
