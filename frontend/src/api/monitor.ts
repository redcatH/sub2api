import { buildGatewayUrl } from '@/api/url'

/**
 * fork 增强：实时 API Key 请求/响应监控 WebSocket 客户端。
 *
 * 连接 GET /v1/monitor/requests，通过 WebSocket subprotocol 携带被监控的 api key：
 *   protocols = ["sub2api-monitor", "key.<plaintext-api-key>"]
 * 服务端校验 key 后下发该 key 的每条请求/响应配对快照。
 *
 * key 走 subprotocol（不进 URL），避免 access log / 浏览器历史泄露。
 */

export const MONITOR_WS_BASE_PROTOCOL = 'sub2api-monitor'
export const MONITOR_WS_CLOSE_CODES = {
  INVALID_KEY: 4003,
} as const

export interface RequestSnapshot {
  at_ms: number
  method: string
  path: string
  model?: string
  client_request_id?: string
  body: string
  status: number
  response_body: string
  duration_ms: number
  missed?: number
}

export type MonitorStatus = 'connecting' | 'connected' | 'reconnecting' | 'closed' | 'offline'

export interface SubscribeMonitorOptions {
  wsBaseUrl?: string
  onStatusChange?: (status: MonitorStatus) => void
  onFatalClose?: (event: CloseEvent) => void
  maxReconnectAttempts?: number
  reconnectBaseDelayMs?: number
  reconnectMaxDelayMs?: number
}

/**
 * subscribeMonitor 订阅指定 api key 的实时请求/响应快照。
 * @returns cleanup 函数，调用以断开（页面关闭/卸载即停止监控）。
 */
export function subscribeMonitor(
  apiKey: string,
  onMessage: (snap: RequestSnapshot) => void,
  options: SubscribeMonitorOptions = {},
): () => void {
  let ws: WebSocket | null = null
  let reconnectAttempts = 0
  const maxReconnectAttempts = Number.isFinite(options.maxReconnectAttempts as number)
    ? (options.maxReconnectAttempts as number)
    : Infinity
  const baseDelayMs = options.reconnectBaseDelayMs ?? 1000
  const maxDelayMs = options.reconnectMaxDelayMs ?? 30000
  let reconnectTimer: ReturnType<typeof setTimeout> | null = null
  let shouldReconnect = true
  let isConnecting = false
  let hasConnectedOnce = false

  const setStatus = (status: MonitorStatus) => options.onStatusChange?.(status)

  const clearReconnectTimer = () => {
    if (reconnectTimer) {
      clearTimeout(reconnectTimer)
      reconnectTimer = null
    }
  }

  const scheduleReconnect = () => {
    if (!shouldReconnect) return
    if (hasConnectedOnce && reconnectAttempts >= maxReconnectAttempts) return
    if (typeof navigator !== 'undefined' && 'onLine' in navigator && !navigator.onLine) {
      setStatus('offline')
      return
    }
    const expDelay = baseDelayMs * Math.pow(2, reconnectAttempts)
    const delay = Math.min(expDelay, maxDelayMs)
    const jitter = Math.floor(Math.random() * 250)
    clearReconnectTimer()
    reconnectTimer = setTimeout(() => {
      reconnectAttempts++
      connect()
    }, delay + jitter)
  }

  const handleOnline = () => {
    if (!shouldReconnect) return
    if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) return
    connect()
  }
  const handleOffline = () => setStatus('offline')

  const connect = () => {
    if (!shouldReconnect) return
    if (isConnecting) return
    if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) return
    if (hasConnectedOnce && reconnectAttempts >= maxReconnectAttempts) return

    isConnecting = true
    setStatus(hasConnectedOnce ? 'reconnecting' : 'connecting')

    const wsBaseUrl = options.wsBaseUrl || import.meta.env.VITE_WS_BASE_URL
    const wsURL = wsBaseUrl
      ? new URL(`${window.location.protocol === 'https:' ? 'wss:' : 'ws:'}//${wsBaseUrl}/v1/monitor/requests`)
      : new URL(buildGatewayUrl('/v1/monitor/requests').replace(/^http/, 'ws'))

    // key 走 subprotocol，不进 URL。
    const protocols: string[] = [MONITOR_WS_BASE_PROTOCOL, `key.${apiKey}`]
    ws = new WebSocket(wsURL.toString(), protocols)

    ws.onopen = () => {
      reconnectAttempts = 0
      isConnecting = false
      hasConnectedOnce = true
      clearReconnectTimer()
      setStatus('connected')
    }

    ws.onmessage = (e) => {
      try {
        const data = JSON.parse(e.data) as RequestSnapshot
        onMessage(data)
      } catch (err) {
        console.warn('[MonitorWS] Failed to parse message:', err)
      }
    }

    ws.onerror = (error) => {
      console.error('[MonitorWS] Connection error:', error)
    }

    ws.onclose = (event) => {
      isConnecting = false
      clearReconnectTimer()
      ws = null
      // 服务端用 4003 表示 key 无效 —— 不要重连死循环。
      if (event && typeof event.code === 'number' && event.code === MONITOR_WS_CLOSE_CODES.INVALID_KEY) {
        shouldReconnect = false
        setStatus('closed')
        options.onFatalClose?.(event)
        return
      }
      scheduleReconnect()
    }
  }

  window.addEventListener('online', handleOnline)
  window.addEventListener('offline', handleOffline)
  connect()

  return () => {
    shouldReconnect = false
    window.removeEventListener('online', handleOnline)
    window.removeEventListener('offline', handleOffline)
    clearReconnectTimer()
    if (ws) ws.close()
    ws = null
    setStatus('closed')
  }
}
