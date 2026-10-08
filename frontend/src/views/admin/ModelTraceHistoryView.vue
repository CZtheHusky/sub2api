<template>
  <div class="mx-auto max-w-7xl px-4 py-6 sm:px-6 lg:px-8">
    <div class="mb-6 flex flex-wrap items-center justify-between gap-3">
      <div>
        <h1 class="text-2xl font-bold text-gray-900 dark:text-gray-100">{{ t('admin.accounts.modeltraceHistory.title') }}</h1>
        <p class="mt-1 text-sm text-gray-500 dark:text-gray-400">{{ t('admin.accounts.modeltraceHistory.description') }}</p>
      </div>
      <div class="flex items-center gap-2">
        <button @click="loadFingerprint" class="rounded-lg border border-gray-300 px-3 py-2 text-sm hover:bg-gray-50 dark:border-dark-600 dark:text-gray-200 dark:hover:bg-dark-700">
          {{ t('admin.accounts.modeltraceHistory.bank') }}: {{ fingerprintCommit ? fingerprintCommit.slice(0, 10) : '…' }}
        </button>
        <button @click="refreshBank" :disabled="refreshingBank" class="rounded-lg bg-primary-600 px-3 py-2 text-sm text-white hover:bg-primary-700 disabled:opacity-50">
          {{ refreshingBank ? t('admin.accounts.modeltraceHistory.refreshing') : t('admin.accounts.modeltraceHistory.refreshBank') }}
        </button>
      </div>
    </div>

    <div class="mb-4 flex flex-wrap items-center gap-3">
      <select v-model="accountId" class="rounded-lg border border-gray-300 bg-white px-3 py-2 text-sm dark:border-dark-600 dark:bg-dark-800 dark:text-gray-100">
        <option :value="0" disabled>{{ t('admin.accounts.modeltraceHistory.chooseAccount') }}</option>
        <option v-for="a in accounts" :key="a.id" :value="a.id">{{ a.name }} (#{{ a.id }})</option>
      </select>
      <input v-model="modelFilter" :placeholder="t('admin.accounts.modeltraceHistory.modelFilter')" class="w-48 rounded-lg border border-gray-300 px-3 py-2 text-sm dark:border-dark-600 dark:bg-dark-800 dark:text-gray-100" @keyup.enter="reload" />
      <select v-model="outcomeFilter" class="rounded-lg border border-gray-300 bg-white px-3 py-2 text-sm dark:border-dark-600 dark:bg-dark-800 dark:text-gray-100">
        <option value="all">{{ t('admin.accounts.modeltraceHistory.filterAll') }}</option>
        <option value="success">{{ t('admin.accounts.modeltraceHistory.filterSuccess') }}</option>
      </select>
      <button @click="reload" class="rounded-lg bg-primary-600 px-4 py-2 text-sm text-white hover:bg-primary-700">{{ t('admin.accounts.modeltraceHistory.query') }}</button>
    </div>

    <div v-if="!accountId" class="rounded-xl border border-dashed border-gray-300 p-10 text-center text-sm text-gray-400 dark:border-dark-600">
      {{ t('admin.accounts.modeltraceHistory.selectFirst') }}
    </div>

    <div v-else-if="loading" class="rounded-xl border border-gray-200 p-10 text-center text-sm text-gray-400 dark:border-dark-700">
      {{ t('admin.accounts.modeltraceHistory.loading') }}
    </div>

    <div v-else-if="!items.length" class="rounded-xl border border-dashed border-gray-300 p-10 text-center text-sm text-gray-400 dark:border-dark-600">
      {{ t('admin.accounts.modeltraceHistory.empty') }}
    </div>

    <div v-else class="overflow-x-auto rounded-xl border border-gray-200 dark:border-dark-700">
      <table class="min-w-full divide-y divide-gray-200 text-sm dark:divide-dark-700">
        <thead class="bg-gray-50 text-left text-xs uppercase text-gray-500 dark:bg-dark-800 dark:text-gray-400">
          <tr>
            <th class="px-4 py-3">{{ t('admin.accounts.modeltraceHistory.colTime') }}</th>
            <th class="px-4 py-3">{{ t('admin.accounts.modeltraceHistory.colModel') }}</th>
            <th class="px-4 py-3">{{ t('admin.accounts.modeltraceHistory.colVerdict') }}</th>
            <th class="px-4 py-3">{{ t('admin.accounts.modeltraceHistory.colPredicted') }}</th>
            <th class="px-4 py-3">{{ t('admin.accounts.modeltraceHistory.colProbability') }}</th>
            <th class="px-4 py-3">{{ t('admin.accounts.modeltraceHistory.colDuration') }}</th>
            <th class="px-4 py-3">{{ t('admin.accounts.modeltraceHistory.colProxy') }}</th>
            <th class="px-4 py-3">{{ t('admin.accounts.modeltraceHistory.colBank') }}</th>
          </tr>
        </thead>
        <tbody class="divide-y divide-gray-100 dark:divide-dark-700">
          <tr v-for="row in items" :key="row.id" class="cursor-pointer hover:bg-gray-50 dark:hover:bg-dark-800" @click="selected = row">
            <td class="whitespace-nowrap px-4 py-3 text-gray-500 dark:text-gray-400">{{ formatTime(row.occurred_at) }}</td>
            <td class="px-4 py-3 font-medium text-gray-900 dark:text-gray-100">{{ row.model }}</td>
            <td class="px-4 py-3">
              <span :class="verdictClass(row)">{{ verdictLabel(row) }}</span>
            </td>
            <td class="px-4 py-3">{{ row.fingerprint_predicted_model || '—' }}</td>
            <td class="px-4 py-3">{{ row.fingerprint_probability != null ? (row.fingerprint_probability * 100).toFixed(1) + '%' : '—' }}</td>
            <td class="whitespace-nowrap px-4 py-3">{{ row.duration_ms ? (row.duration_ms / 1000).toFixed(1) + 's' : '—' }}</td>
            <td class="px-4 py-3">{{ row.proxy_name || '—' }}</td>
            <td class="px-4 py-3 font-mono text-xs text-gray-400">{{ row.fingerprint_commit ? row.fingerprint_commit.slice(0, 8) : '—' }}</td>
          </tr>
        </tbody>
      </table>
      <div class="flex items-center justify-between border-t border-gray-200 px-4 py-3 text-sm dark:border-dark-700">
        <span class="text-gray-500">{{ t('admin.accounts.modeltraceHistory.total', { total }) }}</span>
        <div class="flex items-center gap-2">
          <button :disabled="page <= 1" class="rounded border px-2 py-1 disabled:opacity-40 dark:border-dark-600" @click="page--; load()">{{ t('admin.accounts.modeltraceHistory.prev') }}</button>
          <span>{{ page }} / {{ totalPages }}</span>
          <button :disabled="page >= totalPages" class="rounded border px-2 py-1 disabled:opacity-40 dark:border-dark-600" @click="page++; load()">{{ t('admin.accounts.modeltraceHistory.next') }}</button>
        </div>
      </div>
    </div>

    <!-- 详情抽屉 -->
    <div v-if="selected" class="fixed inset-0 z-50 flex justify-end bg-black/40" @click.self="selected = null">
      <div class="h-full w-full max-w-lg overflow-y-auto bg-white p-6 shadow-xl dark:bg-dark-900">
        <div class="mb-4 flex items-center justify-between">
          <h2 class="text-lg font-bold text-gray-900 dark:text-gray-100">{{ t('admin.accounts.modeltraceHistory.detailTitle') }}</h2>
          <button class="text-gray-400 hover:text-gray-600" @click="selected = null">✕</button>
        </div>
        <dl class="space-y-3 text-sm">
          <template v-for="(value, key) in detailRows" :key="key">
            <div class="flex justify-between gap-4 border-b border-gray-100 pb-2 dark:border-dark-800">
              <dt class="text-gray-500">{{ key }}</dt>
              <dd class="text-right font-medium text-gray-900 dark:text-gray-200">{{ value }}</dd>
            </div>
          </template>
        </dl>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { useI18n } from 'vue-i18n'
import { apiClient } from '@/api/client'
import * as tickets from '@/api/admin/codexTickets'
import type { TicketAttempt } from '@/api/admin/codexTickets'

const { t, locale } = useI18n()

interface AccountOption { id: number; name: string }

const accounts = ref<AccountOption[]>([])
const accountId = ref(0)
const modelFilter = ref('')
const outcomeFilter = ref('all')
const items = ref<TicketAttempt[]>([])
const total = ref(0)
const page = ref(1)
const pageSize = 20
const loading = ref(false)
const selected = ref<TicketAttempt | null>(null)
const fingerprintCommit = ref('')
const refreshingBank = ref(false)

const totalPages = computed(() => Math.max(1, Math.ceil(total.value / pageSize)))

async function loadAccounts() {
  const { data } = await apiClient.get<{ items: AccountOption[] }>('/admin/accounts', { params: { page: 1, page_size: 200, platform: 'openai' } })
  accounts.value = data.items || []
}

async function loadFingerprint() {
  try {
    const fp = await tickets.fingerprint()
    fingerprintCommit.value = fp.commit
  } catch { /* 忽略，保留旧值 */ }
}

async function refreshBank() {
  refreshingBank.value = true
  try {
    const fp = await tickets.refreshFingerprint()
    fingerprintCommit.value = fp.commit
  } finally {
    refreshingBank.value = false
  }
}

async function load() {
  if (!accountId.value) return
  loading.value = true
  try {
    const data = await tickets.history(accountId.value, {
      model: modelFilter.value.trim() || undefined,
      filter: outcomeFilter.value,
      page: page.value,
      page_size: pageSize
    })
    items.value = data.items || []
    total.value = data.total || 0
  } finally {
    loading.value = false
  }
}

function reload() { page.value = 1; load() }

function formatTime(v: string) {
  const d = new Date(v)
  return d.toLocaleString(locale.value === 'zh' ? 'zh-CN' : 'en-US')
}

function verdictLabel(row: TicketAttempt) {
  if (row.outcome !== 'success') return t('admin.accounts.modeltraceHistory.vFailed')
  if (row.fingerprint_matched === true) return t('admin.accounts.modeltraceHistory.vNormal')
  if (row.fingerprint_matched === false) return t('admin.accounts.modeltraceHistory.vDegraded')
  return t('admin.accounts.modeltraceHistory.vUncertain')
}

function verdictClass(row: TicketAttempt) {
  if (row.outcome !== 'success') return 'rounded-full bg-gray-100 px-2 py-0.5 text-xs text-gray-600 dark:bg-dark-700 dark:text-gray-300'
  if (row.fingerprint_matched === true) return 'rounded-full bg-green-50 px-2 py-0.5 text-xs text-green-700 dark:bg-green-900/30 dark:text-green-300'
  if (row.fingerprint_matched === false) return 'rounded-full bg-red-50 px-2 py-0.5 text-xs text-red-700 dark:bg-red-900/30 dark:text-red-300'
  return 'rounded-full bg-amber-50 px-2 py-0.5 text-xs text-amber-700 dark:bg-amber-900/30 dark:text-amber-300'
}

const detailRows = computed<Record<string, string>>(() => {
  const row = selected.value
  if (!row) return {}
  return {
    [t('admin.accounts.modeltraceHistory.colTime')]: formatTime(row.occurred_at),
    [t('admin.accounts.modeltraceHistory.colModel')]: row.model,
    [t('admin.accounts.modeltraceHistory.detailOutcome')]: row.outcome,
    [t('admin.accounts.modeltraceHistory.detailTrigger')]: row.trigger || '—',
    [t('admin.accounts.modeltraceHistory.colVerdict')]: verdictLabel(row),
    [t('admin.accounts.modeltraceHistory.colPredicted')]: row.fingerprint_predicted_model || '—',
    [t('admin.accounts.modeltraceHistory.colProbability')]: row.fingerprint_probability != null ? (row.fingerprint_probability * 100).toFixed(2) + '%' : '—',
    [t('admin.accounts.modeltraceHistory.detailMatched')]: row.fingerprint_matched == null ? '—' : String(row.fingerprint_matched),
    [t('admin.accounts.modeltraceHistory.detailExpectedCount')]: row.challenge_expected_count != null ? String(row.challenge_expected_count) : '—',
    [t('admin.accounts.modeltraceHistory.detailParsedCount')]: row.parsed_number_count != null ? String(row.parsed_number_count) : '—',
    [t('admin.accounts.modeltraceHistory.detailHttpStatus')]: row.http_status != null ? String(row.http_status) : '—',
    [t('admin.accounts.modeltraceHistory.detailDuration')]: row.duration_ms ? row.duration_ms + ' ms' : '—',
    [t('admin.accounts.modeltraceHistory.colProxy')]: row.proxy_name || '—',
    [t('admin.accounts.modeltraceHistory.detailReason')]: row.reason_code || '—',
    [t('admin.accounts.modeltraceHistory.detailVerification')]: row.verification_method || '—',
    [t('admin.accounts.modeltraceHistory.colBank')]: row.fingerprint_commit || '—'
  }
})

onMounted(() => {
  loadAccounts()
  loadFingerprint()
})
</script>
