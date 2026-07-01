package middleware

import (
	"bytes"
	"compress/gzip"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

func setAPIKeyForTest(c *gin.Context, id int64) {
	c.Set(string(ContextKeyAPIKey), &service.APIKey{ID: id})
}

// 无订阅者时：tap 直接 c.Next()，不读 body，handler 读到完整原始 body。
func TestMonitorTap_NoSubscribers_HandlerReadsBodyUnchanged(t *testing.T) {
	gin.SetMode(gin.TestMode)
	hub := service.NewMonitorHub()

	var handlerSaw string
	r := gin.New()
	r.Use(func(c *gin.Context) { setAPIKeyForTest(c, 5); c.Next() })
	r.Use(MonitorTap(hub))
	r.POST("/x", func(c *gin.Context) {
		buf := make([]byte, 4096)
		n, _ := c.Request.Body.Read(buf)
		handlerSaw = string(buf[:n])
		c.Status(200)
	})

	body := `{"a":1}`
	req := httptest.NewRequest("POST", "/x", bytes.NewReader([]byte(body)))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("status=%d", w.Code)
	}
	if handlerSaw != body {
		t.Fatalf("handler saw %q, want %q", handlerSaw, body)
	}
}

// 有订阅者时：tap 读 body 并回填（handler 读到相同字节）→ 包装 writer → 发布配对快照。
func TestMonitorTap_CapturesRequestAndResponse(t *testing.T) {
	gin.SetMode(gin.TestMode)
	hub := service.NewMonitorHub()
	sub := hub.Subscribe(11)
	defer hub.Unsubscribe(11, sub)

	var handlerSaw string
	r := gin.New()
	r.Use(func(c *gin.Context) {
		ctx := context.WithValue(c.Request.Context(), ctxkey.ClientRequestID, "req-123")
		c.Request = c.Request.WithContext(ctx)
		setAPIKeyForTest(c, 11)
		c.Next()
	})
	r.Use(MonitorTap(hub))
	r.POST("/echo", func(c *gin.Context) {
		buf := make([]byte, 4096)
		n, _ := c.Request.Body.Read(buf)
		handlerSaw = string(buf[:n])
		c.JSON(200, gin.H{"ok": true})
	})

	body := `{"model":"claude-x","messages":[]}`
	req := httptest.NewRequest("POST", "/echo", bytes.NewReader([]byte(body)))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	// handler 收到完整 body（回填正确）
	if handlerSaw != body {
		t.Fatalf("handler saw %q, want %q", handlerSaw, body)
	}
	// 客户端收到正常 response（capture writer 正确转发）
	if w.Code != 200 || !bytes.Contains(w.Body.Bytes(), []byte(`"ok":true`)) {
		t.Fatalf("client response: %d %s", w.Code, w.Body.String())
	}
	// 快照含 request + response + status + model + client_request_id
	select {
	case snap := <-sub.Channel():
		if snap.Body != body {
			t.Fatalf("snapshot body=%q, want %q", snap.Body, body)
		}
		if snap.Status != 200 {
			t.Fatalf("snapshot status=%d", snap.Status)
		}
		if snap.Model != "claude-x" {
			t.Fatalf("snapshot model=%q", snap.Model)
		}
		if snap.ClientRequestID != "req-123" {
			t.Fatalf("snapshot client_request_id=%q", snap.ClientRequestID)
		}
		if !bytes.Contains([]byte(snap.ResponseBody), []byte(`"ok":true`)) {
			t.Fatalf("snapshot response_body=%q", snap.ResponseBody)
		}
	case <-time.After(time.Second):
		t.Fatal("no snapshot published")
	}
}

// gzip 请求：snapshot.Body 应为解压后的内容（证明回填的是解码字节）。
func TestMonitorTap_GzipRequestDecompressed(t *testing.T) {
	gin.SetMode(gin.TestMode)
	hub := service.NewMonitorHub()
	sub := hub.Subscribe(12)
	defer hub.Unsubscribe(12, sub)

	plain := `{"model":"gz","messages":[]}`
	var gzBuf bytes.Buffer
	gw := gzip.NewWriter(&gzBuf)
	_, _ = gw.Write([]byte(plain))
	_ = gw.Close()

	r := gin.New()
	r.Use(func(c *gin.Context) { setAPIKeyForTest(c, 12); c.Next() })
	r.Use(MonitorTap(hub))
	r.POST("/g", func(c *gin.Context) { c.Status(200) })

	req := httptest.NewRequest("POST", "/g", &gzBuf)
	req.Header.Set("Content-Encoding", "gzip")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	select {
	case snap := <-sub.Channel():
		if snap.Body != plain {
			t.Fatalf("snapshot should contain decompressed body %q, got %q", plain, snap.Body)
		}
	case <-time.After(time.Second):
		t.Fatal("no snapshot published")
	}
}

// MaxBytesReader 超限：tap 必须传播错误（413），不能吞掉后让超大 body 进 handler。
func TestMonitorTap_OversizeBodyAbortsNotSwallowed(t *testing.T) {
	gin.SetMode(gin.TestMode)
	hub := service.NewMonitorHub()
	sub := hub.Subscribe(13)
	defer hub.Unsubscribe(13, sub)

	big := bytes.Repeat([]byte("a"), 1000)
	handlerCalled := false
	r := gin.New()
	r.Use(func(c *gin.Context) {
		// 模拟 RequestBodyLimit 的 MaxBytesReader 包装
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 100)
		setAPIKeyForTest(c, 13)
		c.Next()
	})
	r.Use(MonitorTap(hub))
	r.POST("/b", func(c *gin.Context) { handlerCalled = true; c.Status(200) })

	req := httptest.NewRequest("POST", "/b", bytes.NewReader(big))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", w.Code)
	}
	if handlerCalled {
		t.Fatal("handler must not run on oversize body")
	}
}

// capture writer 截断：超 limit 的响应只缓冲前 limit 字节，且仍正确转发给客户端。
func TestMonitorCaptureWriter_TruncatesButForwardsAll(t *testing.T) {
	gin.SetMode(gin.TestMode)
	hub := service.NewMonitorHub()
	sub := hub.Subscribe(14)
	defer hub.Unsubscribe(14, sub)

	full := bytes.Repeat([]byte("Z"), service.MonitorMaxBodySnapshot*2) // 两倍上限
	r := gin.New()
	r.Use(func(c *gin.Context) { setAPIKeyForTest(c, 14); c.Next() })
	r.Use(MonitorTap(hub))
	r.POST("/big", func(c *gin.Context) {
		_, _ = c.Writer.Write(full) // 写入完整长响应
	})

	req := httptest.NewRequest("POST", "/big", bytes.NewReader([]byte(`{}`)))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	// 客户端收到完整响应（未被截断）
	if !bytes.Equal(w.Body.Bytes(), full) {
		t.Fatalf("client should receive full response (len=%d), got len=%d", len(full), w.Body.Len())
	}
	// snapshot 的 response_body 被截断到上限
	select {
	case snap := <-sub.Channel():
		if len(snap.ResponseBody) != service.MonitorMaxBodySnapshot {
			t.Fatalf("snapshot response_body len=%d, want %d", len(snap.ResponseBody), service.MonitorMaxBodySnapshot)
		}
	case <-time.After(time.Second):
		t.Fatal("no snapshot published")
	}
}

// defer 恢复 c.Writer：链路返回后，外层中间件观察到的 c.Writer 应已还原，
// 而非残留 monitorCaptureWriter（否则外层 opsErrorLogger 的条件恢复会失败，panic 路径崩溃）。
func TestMonitorTap_RestoresWriterAfterChain(t *testing.T) {
	gin.SetMode(gin.TestMode)
	hub := service.NewMonitorHub()
	sub := hub.Subscribe(20)
	defer hub.Unsubscribe(20, sub)

	var observedWriter gin.ResponseWriter
	r := gin.New()
	// 外层中间件：c.Next() 后捕获 c.Writer（模拟 opsErrorLogger 之后的位置）
	r.Use(func(c *gin.Context) { c.Next(); observedWriter = c.Writer })
	r.Use(MonitorTap(hub))
	r.POST("/x", func(c *gin.Context) { c.Status(200) })

	req := httptest.NewRequest("POST", "/x", bytes.NewReader([]byte(`{}`)))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if observedWriter == nil {
		t.Fatal("observedWriter is nil")
	}
	if _, ok := observedWriter.(*monitorCaptureWriter); ok {
		t.Fatal("c.Writer should be restored after chain returns, still *monitorCaptureWriter")
	}
}
