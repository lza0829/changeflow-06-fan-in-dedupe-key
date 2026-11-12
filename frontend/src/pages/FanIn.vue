<script setup>
import { computed, onMounted, ref } from 'vue'
import { getJSON, patchJSON, postJSON } from '../api'

const entries = ref([])
const selected = ref([])
const batches = ref([])
const detail = ref(null)
const maxPerEntry = ref(0)
const err = ref('')
const busy = ref(false)

const digest = computed(() => detail.value?.report || null)
const counts = computed(() => digest.value?.counts || null)
const replayed = computed(() => counts.value?.replayed || 0)
const entryTotals = computed(() => {
  const rows = digest.value?.entries || []
  return {
    pulled: rows.reduce((n, e) => n + e.pulled_count, 0),
    candidates: rows.reduce((n, e) => n + e.candidate_count, 0),
    retained: rows.reduce((n, e) => n + e.retained, 0),
    dropped: rows.reduce((n, e) => n + e.dropped, 0),
    applied: rows.reduce((n, e) => n + e.applied, 0),
    quarantined: rows.reduce((n, e) => n + e.quarantined, 0),
    blocked: rows.reduce((n, e) => n + e.blocked, 0),
  }
})

async function loadEntries() {
  entries.value = await getJSON('/api/fan-in/entries')
  const enabled = entries.value.filter((e) => e.enabled).map((e) => e.id)
  if (!selected.value.length) selected.value = enabled
}
async function loadBatches() {
  batches.value = await getJSON('/api/fan-in/batches')
}
async function openBatch(id) {
  err.value = ''
  try {
    detail.value = await getJSON(`/api/fan-in/batches/${id}`)
  } catch (e) {
    err.value = String(e.message || e)
  }
}
async function toggleEntry(entry) {
  await patchJSON(`/api/fan-in/entries/${entry.id}`, { enabled: !entry.enabled })
  await loadEntries()
}
async function resetCursors() {
  busy.value = true
  err.value = ''
  try {
    await postJSON('/api/fan-in/entries/reset', { entry_ids: selected.value.length ? selected.value : null })
    await loadEntries()
  } catch (e) {
    err.value = String(e.message || e)
  } finally {
    busy.value = false
  }
}
async function runBatch() {
  busy.value = true
  err.value = ''
  try {
    detail.value = await postJSON('/api/fan-in/batches', {
      entry_ids: selected.value,
      max_per_entry: Number(maxPerEntry.value) || 0,
    })
    await loadEntries()
    await loadBatches()
  } catch (e) {
    err.value = String(e.message || e)
  } finally {
    busy.value = false
  }
}
function toggleSelect(id) {
  selected.value = selected.value.includes(id)
    ? selected.value.filter((x) => x !== id)
    : [...selected.value, id]
}
function statusTag(status) {
  return status === 'applied'
    ? 'ok'
    : status === 'quarantined' || status === 'blocked_gap'
      ? 'warn'
      : status === 'dropped' || status === 'dropped_at_fan_in'
        ? 'danger'
        : ''
}
onMounted(async () => {
  try {
    await loadEntries()
    await loadBatches()
    if (batches.value.length) await openBatch(batches.value[0].id)
  } catch (e) {
    err.value = String(e.message || e)
  }
})
</script>
<template>
  <div class="page">
    <h1>汇入去重</h1>
    <p class="muted">
      多个本机变更入口共享同一应用通道：拉取 → 汇入候选清单 → 同实体键裁决 → 保留条按连续序 Apply。
      裁决规则固定为 <code>{{ digest?.dedupe_rule || 'revision_desc > event_ts_desc > entry_id_asc > seq_desc' }}</code>。
    </p>
    <p v-if="err" class="tag danger">{{ err }}</p>

    <div class="panel">
      <h3>汇入配置</h3>
      <table>
        <thead>
          <tr><th>选</th><th>入口</th><th>夹具</th><th>启用</th><th>pull 游标</th><th>consumed 游标</th><th>累计候选</th><th>备注</th><th></th></tr>
        </thead>
        <tbody>
          <tr v-for="e in entries" :key="e.id">
            <td><input type="checkbox" :checked="selected.includes(e.id)" @change="toggleSelect(e.id)" /></td>
            <td>{{ e.id }} · {{ e.name }}</td>
            <td><code>{{ e.fixture_path }}</code></td>
            <td><span class="tag" :class="e.enabled ? 'ok' : 'warn'">{{ e.enabled ? '是' : '否' }}</span></td>
            <td>{{ e.pull_cursor }}</td>
            <td>{{ e.consumed_cursor }}</td>
            <td>{{ e.candidate_count_all_batches }}</td>
            <td class="muted">{{ e.note }}</td>
            <td><button class="ghost" @click="toggleEntry(e)">切换启用</button></td>
          </tr>
        </tbody>
      </table>
      <div class="row-actions" style="margin-top: 0.7rem">
        <button :disabled="busy || selected.length < 2" @click="runBatch">
          {{ busy ? '汇入中…' : `发起汇入批次（${selected.length} 个入口）` }}
        </button>
        <label>每入口拉取上限
          <input v-model.number="maxPerEntry" type="number" min="0" step="1" style="width: 6rem" />
        </label>
        <button class="ghost" :disabled="busy" @click="resetCursors">重置所选入口游标</button>
        <span class="muted">至少选择两个入口；0 表示不限。入口游标在批次结束后按源 seq 推进。</span>
      </div>
    </div>

    <div v-if="detail" class="panel">
      <h3>批次 #{{ detail.id }} 对账
        <span class="tag" :class="detail.status === 'succeeded' ? 'ok' : detail.status === 'no_change' ? '' : 'warn'">{{ detail.status }}</span>
      </h3>
      <p class="muted">通道 · {{ detail.report?.target_stream_id }} · {{ detail.created_at }} → {{ detail.finished_at || '进行中' }}</p>

      <div class="tiles">
        <div class="tile"><div class="hero-num">{{ entryTotals.pulled }}</div><div class="muted">入口拉取</div></div>
        <div class="tile"><div class="hero-num">{{ counts?.candidate_total ?? 0 }}</div><div class="muted">汇入候选</div></div>
        <div class="tile"><div class="hero-num">{{ counts?.retained ?? 0 }}</div><div class="muted">去重保留</div></div>
        <div class="tile"><div class="hero-num">{{ counts?.dropped ?? 0 }}</div><div class="muted">去重丢弃</div></div>
        <div class="tile"><div class="hero-num">{{ counts?.applied ?? 0 }}</div><div class="muted">Applied</div></div>
        <div class="tile"><div class="hero-num">{{ counts?.quarantined ?? 0 }}</div><div class="muted">Quarantined</div></div>
        <div class="tile"><div class="hero-num">{{ counts?.blocked ?? 0 }}</div><div class="muted">缺口阻塞</div></div>
        <div class="tile"><div class="hero-num">{{ counts?.ledger_rows ?? 0 }}</div><div class="muted">本批新增账本行</div></div>
        <div class="tile"><div class="hero-num">{{ replayed }}</div><div class="muted">重放复用</div></div>
      </div>

      <h4>各入口</h4>
      <table>
        <thead>
          <tr><th>入口</th><th>拉取</th><th>候选</th><th>保留</th><th>丢弃</th><th>Applied</th><th>隔离</th><th>阻塞</th><th>pull 游标</th><th>consumed 游标</th></tr>
        </thead>
        <tbody>
          <tr v-for="e in digest?.entries || []" :key="e.entry_id">
            <td>{{ e.entry_id }} · {{ e.name }}</td>
            <td>{{ e.pulled_count }}</td>
            <td>{{ e.candidate_count }}</td>
            <td>{{ e.retained }}</td>
            <td>{{ e.dropped }}</td>
            <td>{{ e.applied }}</td>
            <td>{{ e.quarantined }}</td>
            <td>{{ e.blocked }}</td>
            <td>{{ e.pull_cursor_before }} → {{ e.pull_cursor_after }}</td>
            <td>{{ e.consumed_cursor_before }} → {{ e.consumed_cursor_after }}</td>
          </tr>
        </tbody>
      </table>
      <p class="muted" v-if="replayed">
        本批有 {{ replayed }} 条重放（入口游标重置后重扫）：复用既有账本/隔离行，
        不新增账本、不改写 Sink；清单中 <code>replayed=1</code>。
      </p>
      <p class="muted">
        丢弃原因：<code>{{ JSON.stringify(digest?.dropped_by_reason || {}) }}</code>
        · 账本处置：<code>{{ JSON.stringify(digest?.ledger_by_disposition || {}) }}</code>
        · 通道游标 {{ digest?.cursor?.stream_apply_cursor_before }} → {{ digest?.cursor?.stream_apply_cursor_after }}
      </p>

      <h4>对账校验</h4>
      <table>
        <thead><tr><th>校验</th><th>结果</th><th>明细</th></tr></thead>
        <tbody>
          <tr v-for="c in digest?.invariants || []" :key="c.name">
            <td>{{ c.name }}</td>
            <td><span class="tag" :class="c.ok ? 'ok' : 'danger'">{{ c.ok ? '通过' : '不符' }}</span></td>
            <td class="muted">{{ c.detail }}</td>
          </tr>
        </tbody>
      </table>

      <h4>汇入清单</h4>
      <p class="muted">“汇入阶段去重”与“链路隔离”是不同状态：dedupe_status=dropped 的条目不写账本；
        apply_status=quarantined/blocked_gap 的条目是保留条在 Apply 阶段被链路线路拦下。</p>
      <table>
        <thead>
          <tr><th>ID</th><th>入口</th><th>seq</th><th>entity</th><th>rev</th><th>event_ts</th><th>merge #</th><th>apply #</th><th>dedupe</th><th>apply</th><th>保留至</th><th>payload</th></tr>
        </thead>
        <tbody>
          <tr v-for="c in detail.manifest" :key="c.id">
            <td>{{ c.id }}</td>
            <td>{{ c.entry_id }}</td>
            <td>{{ c.seq }}</td>
            <td>{{ c.entity_key }}</td>
            <td>{{ c.revision }}</td>
            <td>{{ c.event_ts }}</td>
            <td>{{ c.merge_seq || '—' }}</td>
            <td>{{ c.apply_seq ?? '—' }}</td>
            <td><span class="tag" :class="statusTag(c.dedupe_status)">{{ c.dedupe_status }}</span>
              <span v-if="c.dedupe_reason" class="muted"> · {{ c.dedupe_reason }}</span></td>
            <td><span class="tag" :class="statusTag(c.apply_status)">{{ c.apply_status }}</span>
              <span v-if="c.replayed" class="tag warn">重放</span></td>
            <td>{{ c.retained_candidate_id || '—' }}</td>
            <td><pre class="mono">{{ JSON.stringify(c.payload) }}</pre></td>
          </tr>
        </tbody>
      </table>

      <h4>本批 Sink 可见行</h4>
      <table>
        <thead><tr><th>entity_key</th><th>last_seq</th><th>last_event_ts</th><th>payload</th></tr></thead>
        <tbody>
          <tr v-for="s in detail.sink_rows" :key="s.entity_key">
            <td>{{ s.entity_key }}</td>
            <td>{{ s.last_seq }}</td>
            <td>{{ s.last_event_ts }}</td>
            <td><pre class="mono">{{ JSON.stringify(s.payload) }}</pre></td>
          </tr>
          <tr v-if="!detail.sink_rows.length"><td colspan="4" class="muted">本批未产生 Sink 行</td></tr>
        </tbody>
      </table>
    </div>

    <div class="panel">
      <h3>汇入批次历史</h3>
      <table>
        <thead>
          <tr><th>批次</th><th>状态</th><th>入口</th><th>候选</th><th>保留</th><th>丢弃</th><th>Applied</th><th>隔离</th><th>阻塞</th><th>时间</th><th></th></tr>
        </thead>
        <tbody>
          <tr v-for="b in batches" :key="b.id">
            <td>{{ b.id }}</td>
            <td><span class="tag" :class="b.status === 'succeeded' ? 'ok' : b.status === 'no_change' ? '' : 'warn'">{{ b.status }}</span></td>
            <td>{{ b.entry_ids.join(', ') }}</td>
            <td>{{ b.candidate_total }}</td>
            <td>{{ b.retained_count }}</td>
            <td>{{ b.dropped_count }}</td>
            <td>{{ b.applied_count }}</td>
            <td>{{ b.quarantined_count }}</td>
            <td>{{ b.blocked_count }}</td>
            <td>{{ b.created_at }}</td>
            <td><button class="ghost" @click="openBatch(b.id)">查看</button></td>
          </tr>
          <tr v-if="!batches.length"><td colspan="11" class="muted">暂无汇入批次</td></tr>
        </tbody>
      </table>
    </div>
  </div>
</template>
