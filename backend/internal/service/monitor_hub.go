package service

import (
	"sync"
	"sync/atomic"
)

// Monitor 实时请求/响应快照的投递通道与缓冲上限。
//
// 这些常量是 fork 增强功能（/v1/monitor/requests WebSocket 实时监控）的一部分，
// 独立于上游，集中放在此文件便于调整。
const (
	// MonitorChanBuffer 是每个订阅者 channel 的缓冲长度。慢消费者超过该长度后
	// 新快照被丢弃（best-effort，missed 计数累加），避免阻塞网关请求。
	MonitorChanBuffer = 64
	// MonitorMaxBodySnapshot 限制单条快照里 request/response body 的截断长度，
	// 控制内存与 WS 帧大小。流式 SSE 长响应只保留前 N 字节。
	// 4MB：覆盖 Claude Code 等客户端的大请求（含 CLAUDE.md + 完整 skills 列表）
	// 及大多数流式响应；仍保留上限以防超大响应耗尽内存。如需不截断可继续调大。
	MonitorMaxBodySnapshot = 4 * 1024 * 1024
)

// MonitorSnapshot 描述一次网关请求的配对快照（请求 body + 最终响应 body）。
// 由 MonitorTap 中间件在请求结束时发布，由 MonitorWS 订阅者消费并经 WebSocket 下发。
type MonitorSnapshot struct {
	AtMs            int64  `json:"at_ms"`
	Method          string `json:"method"`
	Path            string `json:"path"`
	Model           string `json:"model,omitempty"`
	ClientRequestID string `json:"client_request_id,omitempty"`
	Body            string `json:"body"`
	Status          int    `json:"status"`
	ResponseBody    string `json:"response_body"`
	DurationMs      int64  `json:"duration_ms"`
	// Missed 表示在本条之前因订阅者 channel 满（慢消费者）而丢弃的快照数（best-effort）。
	Missed int `json:"missed,omitempty"`
}

// MonitorSubscriber 表示一个 WebSocket 客户端对某个 apiKeyID 的订阅。
type MonitorSubscriber struct {
	ch     chan MonitorSnapshot
	done   chan struct{}
	once   sync.Once
	missed atomic.Int32
}

// Channel 返回用于接收快照的只读 channel（WS 写循环 select 它）。
func (s *MonitorSubscriber) Channel() <-chan MonitorSnapshot { return s.ch }

// Done 在 Unsubscribe 时被关闭，用于通知 WS 写循环退出。
func (s *MonitorSubscriber) Done() <-chan struct{} { return s.done }

func (s *MonitorSubscriber) signalDone() {
	s.once.Do(func() { close(s.done) })
}

// MonitorHub 是按 apiKeyID 分组的内存 pub/sub。
//
// 设计目标：
//   - 零空闲开销：无任何订阅者时，HasSubscribers 仅做一次 atomic 计数检查即返回 false，
//     MonitorTap 据此跳过读 body / 包装 writer，对正常请求近乎无成本。
//   - 订阅驱动生命周期：订阅者随 WebSocket 连接创建，连接断开时 Unsubscribe，
//     即“页面关闭=停止监控”，无持久化、无后台 goroutine、无需 Stop()。
type MonitorHub struct {
	mu    sync.RWMutex
	subs  map[int64]map[*MonitorSubscriber]struct{}
	total atomic.Int64
}

// NewMonitorHub 构造一个空的 MonitorHub（wire singleton，无依赖）。
func NewMonitorHub() *MonitorHub {
	return &MonitorHub{subs: make(map[int64]map[*MonitorSubscriber]struct{})}
}

// HasSubscribers 是 MonitorTap 的快速门控。
// 先用 atomic total 做无锁判断；仅当全局存在订阅者时才取读锁查特定 apiKeyID。
func (h *MonitorHub) HasSubscribers(apiKeyID int64) bool {
	if apiKeyID == 0 || h.total.Load() == 0 {
		return false
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.subs[apiKeyID]) > 0
}

// Subscribe 为 apiKeyID 注册一个订阅者，返回带缓冲 channel 的 subscriber。
func (h *MonitorHub) Subscribe(apiKeyID int64) *MonitorSubscriber {
	s := &MonitorSubscriber{
		ch:   make(chan MonitorSnapshot, MonitorChanBuffer),
		done: make(chan struct{}),
	}
	h.mu.Lock()
	set, ok := h.subs[apiKeyID]
	if !ok {
		set = make(map[*MonitorSubscriber]struct{})
		h.subs[apiKeyID] = set
	}
	set[s] = struct{}{}
	h.total.Add(1) // 在 Unlock 之前累加，避免“已插入 map 但 total 未增”的纳秒级漏捕窗口
	h.mu.Unlock()
	return s
}

// Unsubscribe 移除订阅者。可安全多次调用（once 保证 done 仅关闭一次）。
func (h *MonitorHub) Unsubscribe(apiKeyID int64, s *MonitorSubscriber) {
	if s == nil {
		return
	}
	h.mu.Lock()
	if set := h.subs[apiKeyID]; set != nil {
		if _, ok := set[s]; ok {
			delete(set, s)
			h.total.Add(-1)
			if len(set) == 0 {
				delete(h.subs, apiKeyID)
			}
		}
	}
	h.mu.Unlock()
	s.signalDone()
}

// Publish 非阻塞地向该 apiKeyID 的所有订阅者投递快照。
//
// channel 满（慢消费者）则丢弃并累加 missed；missed 计数会在该订阅者下一次成功
// 收到的快照里附带。注意 missed 为 best-effort 统计（高并发 publish 下可能存在
// 微小竞态），监控而非计费用途可接受。
func (h *MonitorHub) Publish(apiKeyID int64, snap MonitorSnapshot) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	set := h.subs[apiKeyID]
	for s := range set {
		// swap 取出累积 missed（清零），投递成功则随本条报告；失败则恢复并 +1。
		prev := s.missed.Swap(0)
		out := snap
		out.Missed = int(prev)
		select {
		case s.ch <- out:
			// 已投递
		default:
			s.missed.Add(int32(prev) + 1)
		}
	}
}
