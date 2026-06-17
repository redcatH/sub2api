package service

import (
	"fmt"
	"strings"
	"time"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// upstreamSSECodeMapping 讯飞等上游业务错误码到通用错误码的映射表。
// key: 原始5位数字错误码，value: 通用错误信息。
// 映射原则：隐藏上游供应商标识，保留错误语义以便客户端区分错误类型。
//
// 错误码来源：讯飞 WebSocket API 官方文档。
// 分类依据：参考 OpenAI 错误类型体系（invalid_request_error / authentication_error /
// rate_limit_error / server_error / content_filter / upstream_error）。
var upstreamSSECodeMapping = map[int]struct {
	Code    string // 通用错误码（字符串，非数字）
	Type    string // OpenAI 错误类型
	Message string // 通用错误消息
}{
	// ── WebSocket 连接层错误 (10000-10002) ──
	10000: {Code: "upstream_connection_error", Type: "upstream_error", Message: "Upstream connection error"},
	10001: {Code: "upstream_connection_error", Type: "upstream_error", Message: "Upstream connection error"},
	10002: {Code: "upstream_connection_error", Type: "upstream_error", Message: "Upstream connection error"},

	// ── 请求格式/参数错误 (10003-10005, 10018, 10163, 10404) ──
	10003: {Code: "invalid_request_error", Type: "invalid_request_error", Message: "Invalid request format"},
	10004: {Code: "invalid_request_error", Type: "invalid_request_error", Message: "Invalid request schema"},
	10005: {Code: "invalid_request_error", Type: "invalid_request_error", Message: "Invalid request parameters"},
	10018: {Code: "invalid_request_error", Type: "invalid_request_error", Message: "Invalid request usage"},
	10163: {Code: "invalid_request_error", Type: "invalid_request_error", Message: "Invalid request parameters"},
	10404: {Code: "invalid_request_error", Type: "invalid_request_error", Message: "Invalid request parameters"},

	// ── 并发/流量限制 (10006-10007) ──
	10006: {Code: "concurrent_limit_exceeded", Type: "rate_limit_error", Message: "Concurrent connection limit exceeded"},
	10007: {Code: "rate_limit_exceeded", Type: "rate_limit_error", Message: "Rate limit exceeded"},

	// ── 服务容量不足 (10008) ──
	10008: {Code: "insufficient_quota", Type: "rate_limit_error", Message: "Upstream quota insufficient"},

	// ── 上游引擎连接/处理错误 (10009-10012, 10222-10223) ──
	10009: {Code: "upstream_connection_error", Type: "upstream_error", Message: "Upstream connection error"},
	10010: {Code: "upstream_internal_error", Type: "upstream_error", Message: "Upstream request processing error"},
	10011: {Code: "upstream_internal_error", Type: "upstream_error", Message: "Upstream request processing error"},
	10012: {Code: "upstream_internal_error", Type: "upstream_error", Message: "Upstream request processing error"},
	10222: {Code: "upstream_network_error", Type: "upstream_error", Message: "Upstream network error"},
	10223: {Code: "upstream_routing_error", Type: "upstream_error", Message: "Upstream routing error"},

	// ── 内容审核/敏感信息 (10013-10014, 10019) ──
	10013: {Code: "content_filtered", Type: "content_filter", Message: "Content filtered"},
	10014: {Code: "content_filtered", Type: "content_filter", Message: "Content filtered"},
	10019: {Code: "content_filtered", Type: "content_filter", Message: "Content filtered"},

	// ── 认证/授权错误 (10015-10016) ──
	10015: {Code: "authentication_error", Type: "authentication_error", Message: "Authentication failed"},
	10016: {Code: "insufficient_quota", Type: "rate_limit_error", Message: "Upstream quota insufficient"},

	// ── 服务端过载 (10110) ──
	10110: {Code: "engine_overloaded", Type: "server_error", Message: "Upstream engine overloaded"},

	// ── Token/上下文长度超限 (10907, 10910) ──
	10907: {Code: "context_length_exceeded", Type: "invalid_request_error", Message: "Context length exceeded"},
	10910: {Code: "context_length_exceeded", Type: "invalid_request_error", Message: "Context length exceeded"},

	// ── 配额/授权不足 (11200-11203, 11210) ──
	11200: {Code: "insufficient_quota", Type: "rate_limit_error", Message: "Upstream quota insufficient"},
	11201: {Code: "insufficient_quota", Type: "rate_limit_error", Message: "Upstream quota insufficient"},
	11202: {Code: "insufficient_quota", Type: "rate_limit_error", Message: "Upstream quota insufficient"},
	11203: {Code: "insufficient_quota", Type: "rate_limit_error", Message: "Upstream quota insufficient"},
	11210: {Code: "insufficient_quota", Type: "rate_limit_error", Message: "Upstream quota insufficient"},

	// ── 模型不可用 (11221) ──
	11221: {Code: "model_not_available", Type: "invalid_request_error", Message: "Requested model not available"},
}

// upstreamIdentifiers 用于检测 SSE 帧是否包含上游供应商信息。
// 匹配到任意标识符的帧会被改写为通用格式，防止客户端识别上游供应商。
var upstreamIdentifiers = []string{"Xunfei", "Sid:", "ModelArts"}

// sanitizeUpstreamSSELine 检查 SSE 行是否包含上游供应商错误信息，
// 如果是则改写为通用格式，否则原样返回。
//
// 处理逻辑：
//  1. 非数据行（空行、事件行等）原样返回
//  2. 数据行提取有效载荷后检查是否包含上游标识
//  3. 包含上游标识的错误帧改写为通用格式
//  4. 不包含上游标识的帧原样返回
func sanitizeUpstreamSSELine(line string) string {
	// 快速排除：非数据行直接返回
	trimmed := strings.TrimSpace(line)
	if !strings.HasPrefix(trimmed, "data:") {
		return line
	}

	// 提取数据行中的有效载荷
	payload, ok := extractSSEDataPayload(trimmed)
	if !ok || payload == "" || payload == "[DONE]" {
		return line
	}

	// 快速排除：不含上游标识的帧直接返回
	if !containsUpstreamIdentifier(payload) {
		return line
	}

	// 仅处理包含错误对象的帧
	if !gjson.Get(payload, "error").Exists() {
		return line
	}

	// 改写错误帧
	sanitized := rewriteUpstreamErrorPayload(payload)
	if sanitized == payload {
		return line
	}

	// 重建数据行，保留前缀空白
	prefix := line[:len(line)-len(strings.TrimLeft(line, " \t"))]
	return prefix + "data: " + sanitized
}

// extractSSEDataPayload 从数据行中提取有效载荷内容。
func extractSSEDataPayload(line string) (string, bool) {
	start := len("data:")
	for start < len(line) {
		if line[start] != ' ' && line[start] != '\t' {
			break
		}
		start++
	}
	if start >= len(line) {
		return "", false
	}
	return line[start:], true
}

// containsUpstreamIdentifier 检查有效载荷是否包含上游供应商标识。
func containsUpstreamIdentifier(payload string) bool {
	for _, id := range upstreamIdentifiers {
		if strings.Contains(payload, id) {
			return true
		}
	}
	return false
}

// rewriteUpstreamErrorPayload 将包含上游信息的错误有效载荷改写为通用格式。
//
// OpenAI 风格输入: {"error":{"code":10012,"message":"Xunfei request failed with Sid:..."}}
// OpenAI 风格输出: {"error":{"code":"upstream_internal_error","message":"Upstream request processing error","type":"upstream_error"}}
//
// Anthropic 风格输入: {"error":{"message":"Xunfei claude request failed...","type":"api_error"},"type":"error"}
// Anthropic 风格输出: {"error":{"message":"Upstream request processing error","type":"api_error"},"type":"error"}
func rewriteUpstreamErrorPayload(payload string) string {
	// 提取原始错误码（数字型，讯飞为5位数字 10000-19999）
	codeInt := int(gjson.Get(payload, "error.code").Int())

	var mapped struct {
		Code    string
		Type    string
		Message string
	}

	if m, ok := upstreamSSECodeMapping[codeInt]; ok && codeInt > 0 {
		mapped = m
	} else if codeInt >= 10000 && codeInt <= 19999 {
		// 未映射的讯飞错误码范围，使用通用映射
		mapped = struct {
			Code    string
			Type    string
			Message string
		}{Code: "upstream_error", Type: "upstream_error", Message: "Upstream service error"}
	} else {
		// 非讯飞错误码范围或无错误码，仅替换消息中的上游标识
		mapped = struct {
			Code    string
			Type    string
			Message string
		}{Code: "upstream_error", Type: "upstream_error", Message: "Upstream service error"}
	}

	result := payload

	// 设置错误码（数字转字符串）
	if gjson.Get(payload, "error.code").Exists() {
		result, _ = sjson.Set(result, "error.code", mapped.Code)
	}

	// 设置错误消息（附带时间戳，便于客户端与后台日志对齐）
	result, _ = sjson.Set(result, "error.message", fmt.Sprintf("%s (ref:%s)", mapped.Message, time.Now().Format("15:04:05")))

	// 设置错误类型（如果不存在）
	if !gjson.Get(payload, "error.type").Exists() {
		result, _ = sjson.Set(result, "error.type", mapped.Type)
	}

	return result
}
