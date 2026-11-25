<template>
  <section class="handoff-panel" data-testid="handoff-panel">
    <header class="handoff-head">
      <div>
        <h3>{{ heading }}</h3>
        <p class="page-desc">{{ description }}</p>
      </div>
      <button class="btn" type="button" data-testid="handoff-refresh" @click="refresh">刷新</button>
    </header>

    <form class="analysis-form" @submit.prevent="refresh">
      <label>
        <span>湖表</span>
        <input v-model="lakeTable" data-testid="handoff-lake-table" placeholder="如 ods_user_behavior" />
      </label>
      <label>
        <span>目标分区</span>
        <input v-model="partitionKey" data-testid="handoff-partition" placeholder="如 20261101" />
      </label>
      <label v-if="mode === 'merge'">
        <span>归并编号</span>
        <input v-model="mergeId" data-testid="handoff-merge-id" placeholder="如 SMERGE-002" />
      </label>
      <button class="btn primary" type="submit" data-testid="handoff-load">读取详情</button>
    </form>

    <p v-if="errorMessage" class="error-text" data-testid="handoff-error">{{ errorMessage }}</p>

    <template v-if="mode === 'merge' && detail">
      <div class="handoff-grid">
        <div class="handoff-card">
          <h4>中断前后关联的清单</h4>
          <table class="data-table">
            <thead>
              <tr><th>版本</th><th>状态</th><th>来源</th><th>文件数</th><th>行数</th></tr>
            </thead>
            <tbody>
              <tr v-for="manifest in detail.manifests" :key="manifest.manifest_id"
                  :data-testid="`manifest-${manifest.version}`"
                  :data-status="manifest.status">
                <td>v{{ manifest.version }}</td>
                <td>{{ manifest.status }}</td>
                <td>{{ manifest.source }}<template v-if="manifest.source_ref">({{ manifest.source_ref }})</template></td>
                <td>{{ manifest.file_count }}</td>
                <td>{{ manifest.rows }}</td>
              </tr>
              <tr v-if="!detail.manifests.length">
                <td colspan="5" class="empty-state">尚无清单记录</td>
              </tr>
            </tbody>
          </table>
        </div>
        <div class="handoff-card">
          <h4>回执与文件</h4>
          <dl class="handoff-detail">
            <dt>归并阶段的检查点</dt>
            <dd data-testid="merge-phase">{{ detail.phase }}</dd>
            <dt>整组原件</dt>
            <dd data-testid="merge-source-count">{{ detail.source_file_ids.length }} 个</dd>
            <dt>归并产物</dt>
            <dd data-testid="merge-product-count">{{ detail.product_file_ids.length }} 个</dd>
            <dt>首次确认回执</dt>
            <dd data-testid="merge-receipt">
              <template v-if="detail.receipt">
                {{ detail.receipt.product_rows }} 行 / 清单 v{{ detail.receipt.manifest_version }}
              </template>
              <template v-else>尚未换版，无回执</template>
            </dd>
          </dl>
          <div class="page-actions">
            <button class="btn" type="button" data-testid="merge-confirm" @click="doConfirmMerge">确认换版</button>
            <button class="btn" type="button" data-testid="merge-reclaim" @click="doReclaim">补领回执</button>
          </div>
          <p v-if="notice" class="handoff-notice" data-testid="handoff-notice">{{ notice }}</p>
        </div>
      </div>
    </template>

    <template v-if="mode === 'compact'">
      <div class="handoff-grid">
        <div class="handoff-card">
          <h4>压实扫描（仅采用一份确认清单）</h4>
          <dl class="handoff-detail">
            <dt>确认清单</dt>
            <dd data-testid="compact-manifest">
              <template v-if="scan?.manifest">
                v{{ scan.manifest.version }} / {{ scan.manifest.file_count }} 个文件 / {{ scan.manifest.rows }} 行
              </template>
              <template v-else>无确认清单</template>
            </dd>
            <dt>本次读取</dt>
            <dd data-testid="compact-scan">{{ scan?.file_count ?? 0 }} 个文件 / {{ scan?.rows ?? 0 }} 行</dd>
            <dt>跳过 staged 产物</dt>
            <dd data-testid="compact-skipped">{{ scan?.skipped_staged.length ?? 0 }} 个</dd>
          </dl>
          <div class="page-actions">
            <button class="btn" type="button" data-testid="compact-scan-btn" @click="doScan">执行扫描</button>
            <button class="btn primary" type="button" data-testid="compact-open" @click="doOpenTicket">发起接单</button>
          </div>
        </div>
        <div class="handoff-card">
          <h4>压实回执</h4>
          <dl class="handoff-detail" v-if="receipt">
            <dt>回执判定</dt>
            <dd data-testid="receipt-disposition">{{ receipt.disposition }}</dd>
            <dt>是否交回重排</dt>
            <dd data-testid="receipt-returned">{{ receipt.returned_for_reschedule ? '是' : '否' }}</dd>
            <dt>处理行数</dt>
            <dd data-testid="receipt-rows">
              {{ receipt.processed_rows === null ? '—（交回重排不申报）' : receipt.processed_rows }}
            </dd>
            <dt>实际采用文件</dt>
            <dd data-testid="receipt-input">{{ receipt.input_file_ids.length }} 个输入</dd>
            <dt>输出文件</dt>
            <dd data-testid="receipt-output">{{ receipt.output_file_ids.length }} 个输出</dd>
            <dt>清单核对</dt>
            <dd data-testid="receipt-aligned">
              <template v-if="receipt.manifest_reconciliation">
                {{ receipt.manifest_reconciliation.aligned ? '工单与当前清单一致' : '工单与当前清单不一致' }}
              </template>
              <template v-else>—</template>
            </dd>
          </dl>
          <p v-else class="page-desc">尚无回执，先发起接单并执行。</p>
          <p v-if="notice" class="handoff-notice" data-testid="handoff-notice">{{ notice }}</p>
        </div>
      </div>
    </template>
  </section>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue'

import {
  confirmMerge,
  executeCompactTicket,
  loadCompactReceipt,
  loadMergeDetail,
  openCompactTicket,
  reclaimMergeReceipt,
  scanCompactPartition,
} from '@/api/backend-service'
import type { CompactReceipt, CompactScan, MergeDetail } from '@/api/backend-service'

const props = defineProps<{ moduleKey: string }>()
const mode = props.moduleKey === 'compact_job' ? 'compact' : 'merge'

const lakeTable = ref('ods_user_behavior')
const partitionKey = ref('20261101')
const mergeId = ref('SMERGE-002')
const detail = ref<MergeDetail | null>(null)
const scan = ref<CompactScan | null>(null)
const receipt = ref<CompactReceipt | null>(null)
const ticketId = ref('')
const notice = ref('')
const errorMessage = ref('')

const heading = computed(() =>
  mode === 'compact' ? '压实接单与回执核对' : '归并详情与回执补领',
)
const description = computed(() =>
  mode === 'compact'
    ? '压实只接收一份已确认的分区文件清单：扫描会跳过归并器尚未换版的 staged 产物。'
    : '查看归并中断前后关联的清单，补领回执只返回首次确认的替换结果。',
)

async function refresh() {
  errorMessage.value = ''
  notice.value = ''
  try {
    if (mode === 'merge') {
      detail.value = await loadMergeDetail(mergeId.value, lakeTable.value)
    } else {
      await doScan()
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '读取失败'
  }
}

async function doScan() {
  errorMessage.value = ''
  try {
    scan.value = await scanCompactPartition(lakeTable.value, partitionKey.value)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '扫描失败'
  }
}

async function doConfirmMerge() {
  errorMessage.value = ''
  try {
    const result = await confirmMerge(mergeId.value, lakeTable.value)
    notice.value = result.phase === 'confirmed' ? '已确认换版，回执已签发' : `当前阶段：${result.phase}`
    detail.value = await loadMergeDetail(mergeId.value, lakeTable.value)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '确认换版失败'
  }
}

async function doReclaim() {
  errorMessage.value = ''
  try {
    const result = await reclaimMergeReceipt(mergeId.value, lakeTable.value)
    const receiptRows = result.receipt.product_rows as number | undefined
    notice.value = `首次确认回执：产物 ${result.product_file_ids.length} 个 / ${receiptRows ?? '—'} 行`
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '补领回执失败'
  }
}

async function doOpenTicket() {
  errorMessage.value = ''
  try {
    const opened = await openCompactTicket(lakeTable.value, partitionKey.value)
    ticketId.value = String(opened.ticket.ticket_id)
    receipt.value = await executeCompactTicket(ticketId.value)
    notice.value = receipt.value.returned_for_reschedule
      ? '工单与当前清单不一致，已交回重排'
      : `已按清单 v${opened.manifest.version} 完成压实`
    scan.value = await scanCompactPartition(lakeTable.value, partitionKey.value)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '接单失败'
  }
}

async function loadReceipt() {
  if (!ticketId.value) return
  try {
    receipt.value = await loadCompactReceipt(ticketId.value)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '回执读取失败'
  }
}

defineExpose({ loadReceipt })
</script>

<style scoped>
.handoff-panel { margin-top: 16px; }
.handoff-head { display: flex; justify-content: space-between; align-items: flex-start; }
.handoff-head h3 { margin: 0; }
.handoff-grid { display: grid; grid-template-columns: 1.4fr 1fr; gap: 12px; }
.handoff-card { background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 12px; }
.handoff-card h4 { margin: 0 0 8px; font-size: 14px; }
.handoff-detail { margin: 0; }
.handoff-detail dt { font-size: 12px; color: var(--muted); }
.handoff-detail dd { font-size: 13px; margin: 0 0 8px; }
.handoff-notice { color: #1f6feb; font-size: 13px; margin: 8px 0 0; }
</style>
