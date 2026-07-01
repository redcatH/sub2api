<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted } from 'vue'
import { useRoute } from 'vue-router'
import { subscribeMonitor, type RequestSnapshot, type MonitorStatus } from '@/api/monitor'

const route = useRoute()

const apiKeyInput = ref('')
const keyVisible = ref(false)
const status = ref<MonitorStatus | 'idle'>('idle')
const snapshots = ref<RequestSnapshot[]>([])
const expandedKey = ref<string | null>(null)
const fatalMsg = ref('')
let cleanup: (() => void) | null = null

const isConnected = computed(() => status.value === 'connected')
const isConnecting = computed(() => status.value === 'connecting' || status.value === 'reconnecting')
const statusText = computed(() => {
  switch (status.value) {
    case 'connecting': return '连接中…'
    case 'connected': return '已连接'
    case 'reconnecting': return '重连中…'
    case 'offline': return '离线'
    case 'closed': return '已断开'
    default: return ''
  }
})

onMounted(() => {
  // URL ?key= 预填（不自动连接），读后立即剥掉 query，避免 key 残留浏览器历史
  const k = route.query.key
  if (typeof k === 'string' && k.trim()) {
    apiKeyInput.value = k.trim()
    if (window.history && typeof window.history.replaceState === 'function') {
      window.history.replaceState({}, '', window.location.pathname)
    }
  }
})

const snapKey = (s: RequestSnapshot) => `${s.at_ms}-${s.client_request_id || ''}`

const connect = () => {
  const key = apiKeyInput.value.trim()
  if (!key) return
  cleanup?.()
  snapshots.value = []
  expandedKey.value = null
  fatalMsg.value = ''
  status.value = 'connecting'
  cleanup = subscribeMonitor(
    key,
    (snap) => {
      snapshots.value.unshift(snap)
      if (snapshots.value.length > 500) snapshots.value.length = 500
    },
    {
      onStatusChange: (s) => { status.value = s },
      onFatalClose: () => { fatalMsg.value = 'API key 无效或已失效，已停止重连' },
    },
  )
}

const disconnect = () => {
  cleanup?.()
  cleanup = null
  status.value = 'idle'
}

const toggleExpand = (s: RequestSnapshot) => {
  const k = snapKey(s)
  expandedKey.value = expandedKey.value === k ? null : k
}

const formatTime = (ms: number) => new Date(ms).toLocaleTimeString()
const statusClass = (code: number) => {
  if (code === 0) return 'text-gray-400'
  if (code < 200 || code >= 400) return 'text-red-500'
  if (code >= 300) return 'text-yellow-500'
  return 'text-green-500'
}
const maskKey = (k: string) => (k.length > 8 ? `${k.slice(0, 4)}…${k.slice(-4)}` : '****')
const prettyBody = (s: RequestSnapshot, field: 'body' | 'response_body') => {
  const raw = s[field]
  if (!raw) return ''
  try {
    return JSON.stringify(JSON.parse(raw), null, 2)
  } catch {
    return raw
  }
}

onUnmounted(() => { cleanup?.() })
</script>

<template>
  <div class="relative flex min-h-screen flex-col bg-gray-50 dark:bg-dark-950">
    <!-- Header -->
    <header class="relative z-20 px-6 py-4">
      <nav class="mx-auto flex max-w-6xl items-center justify-between">
        <router-link to="/home" class="text-lg font-semibold tracking-tight text-gray-900 dark:text-white">
          Request Monitor
        </router-link>
        <router-link to="/home" class="text-sm text-gray-500 hover:text-gray-700 dark:text-dark-400 dark:hover:text-white">
          ← 返回首页
        </router-link>
      </nav>
    </header>

    <main class="flex-1 w-full mx-auto px-6 py-8" :class="isConnected ? 'max-w-6xl' : 'max-w-5xl'">
      <!-- 未连接：居中输入卡片 -->
      <template v-if="!isConnected">
        <div class="text-center mb-10 mt-8">
          <h1 class="text-3xl sm:text-4xl font-bold tracking-tight mb-3 text-gray-900 dark:text-white">
            实时请求监控
          </h1>
          <p class="text-gray-500 dark:text-dark-400 text-base max-w-md mx-auto">
            输入 API Key，实时查看该 Key 发出的每个请求与响应
          </p>
        </div>

        <div class="max-w-xl mx-auto">
          <div class="bg-white dark:bg-dark-900 rounded-2xl shadow-sm border border-gray-100 dark:border-dark-800 p-6">
            <label class="block text-sm font-medium text-gray-700 dark:text-dark-300 mb-2">API Key</label>
            <div class="relative">
              <input
                v-model="apiKeyInput"
                :type="keyVisible ? 'text' : 'password'"
                placeholder="sk-..."
                class="w-full rounded-lg border border-gray-200 dark:border-dark-700 bg-gray-50 dark:bg-dark-800 px-4 py-2.5 pr-12 text-gray-900 dark:text-white placeholder-gray-400 focus:border-blue-500 focus:ring-1 focus:ring-blue-500 outline-none transition"
                @keyup.enter="connect"
              />
              <button
                type="button"
                @click="keyVisible = !keyVisible"
                class="absolute right-3 top-1/2 -translate-y-1/2 text-xs text-gray-400 hover:text-gray-600 dark:hover:text-dark-300"
              >
                {{ keyVisible ? '隐藏' : '显示' }}
              </button>
            </div>

            <button
              :disabled="!apiKeyInput.trim() || isConnecting"
              @click="connect"
              class="mt-4 w-full rounded-lg bg-blue-600 hover:bg-blue-700 disabled:bg-gray-300 dark:disabled:bg-dark-700 disabled:cursor-not-allowed text-white font-medium py-2.5 transition"
            >
              {{ isConnecting ? '连接中…' : '开始监控' }}
            </button>

            <p v-if="fatalMsg" class="mt-3 text-sm text-red-500">{{ fatalMsg }}</p>

            <p class="mt-4 text-xs text-gray-400 dark:text-dark-500 leading-relaxed">
              Key 仅通过 WebSocket 握手传输，不写入 URL；页面关闭即停止监控，无数据持久化。
            </p>
          </div>
        </div>
      </template>

      <!-- 已连接：顶部摘要条 + 列表 -->
      <template v-else>
        <div class="flex items-center justify-between bg-white dark:bg-dark-900 rounded-xl border border-gray-100 dark:border-dark-800 px-4 py-3 mb-4">
          <div class="flex items-center gap-3 min-w-0">
            <span class="inline-block h-2.5 w-2.5 rounded-full bg-green-500 animate-pulse"></span>
            <span class="text-sm font-medium text-gray-900 dark:text-white">{{ statusText }}</span>
            <span class="text-sm text-gray-400 dark:text-dark-500 truncate font-mono">
              {{ maskKey(apiKeyInput.trim()) }}
            </span>
            <span v-if="snapshots.length" class="text-xs text-gray-400 dark:text-dark-500">
              {{ snapshots.length }} 条
            </span>
          </div>
          <button
            @click="disconnect"
            class="rounded-lg border border-gray-200 dark:border-dark-700 px-3 py-1.5 text-sm text-gray-600 dark:text-dark-300 hover:bg-gray-50 dark:hover:bg-dark-800 transition"
          >
            断开
          </button>
        </div>

        <div v-if="!snapshots.length" class="text-center py-20 text-gray-400 dark:text-dark-500">
          等待该 Key 发出请求…
        </div>

        <div v-else class="space-y-2">
          <div
            v-for="snap in snapshots"
            :key="snapKey(snap)"
            class="bg-white dark:bg-dark-900 rounded-lg border border-gray-100 dark:border-dark-800 overflow-hidden"
          >
            <button
              @click="toggleExpand(snap)"
              class="w-full flex items-center gap-3 px-4 py-2.5 text-left hover:bg-gray-50 dark:hover:bg-dark-800 transition"
            >
              <span class="text-xs text-gray-400 dark:text-dark-500 font-mono w-20 shrink-0">{{ formatTime(snap.at_ms) }}</span>
              <span class="text-xs font-mono font-semibold text-blue-600 dark:text-blue-400 w-16 shrink-0">{{ snap.method }}</span>
              <span class="text-sm text-gray-700 dark:text-dark-300 truncate flex-1 font-mono">{{ snap.path }}</span>
              <span v-if="snap.model" class="text-xs text-gray-400 dark:text-dark-500 truncate max-w-[160px]">{{ snap.model }}</span>
              <span class="text-xs font-mono font-semibold shrink-0" :class="statusClass(snap.status)">{{ snap.status || '—' }}</span>
              <span class="text-xs text-gray-400 dark:text-dark-500 shrink-0 w-16 text-right">{{ snap.duration_ms }}ms</span>
              <span v-if="snap.missed" class="text-xs text-yellow-500 shrink-0" :title="`已丢弃 ${snap.missed} 条`">−{{ snap.missed }}</span>
            </button>

            <div v-if="expandedKey === snapKey(snap)" class="px-4 pb-4 pt-2 border-t border-gray-100 dark:border-dark-800 grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <div class="text-xs font-medium text-gray-500 dark:text-dark-400 mb-1">Request Body</div>
                <pre class="text-xs bg-gray-50 dark:bg-dark-950 rounded p-3 overflow-auto max-h-80 text-gray-800 dark:text-dark-200 whitespace-pre-wrap break-all">{{ prettyBody(snap, 'body') || '(空)' }}</pre>
              </div>
              <div>
                <div class="text-xs font-medium text-gray-500 dark:text-dark-400 mb-1">Response Body</div>
                <pre class="text-xs bg-gray-50 dark:bg-dark-950 rounded p-3 overflow-auto max-h-80 text-gray-800 dark:text-dark-200 whitespace-pre-wrap break-all">{{ prettyBody(snap, 'response_body') || '(空)' }}</pre>
              </div>
            </div>
          </div>
        </div>
      </template>
    </main>
  </div>
</template>
