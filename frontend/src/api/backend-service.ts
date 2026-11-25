import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

// ---- 归并与压实交接 ----

export type ManifestView = {
  manifest_id: string
  version: number
  status: string
  source: string
  source_ref: string | null
  checksum: string
  file_ids: string[]
  file_count: number
  rows: number
}

export type MergeDetail = {
  merge_id: string
  lake_table: string | null
  phase: string
  source_file_ids: string[]
  product_file_ids: string[]
  manifests: ManifestView[]
  receipt: Record<string, unknown> | null
  checkpoint: Record<string, unknown> | null
}

export type CompactReceipt = {
  ticket_id: string
  lake_table: string
  partition_key: string
  status: string | null
  disposition: string
  reason: string | null
  ticket_manifest_id: string
  ticket_manifest_version: number
  returned_for_reschedule: boolean
  completed: boolean
  processed_rows: number | null
  read_files: number
  input_file_ids: string[]
  output_file_ids: string[]
  rebased_file_ids?: string[]
  adopted_manifest: ManifestView | null
  output_manifest_version?: number
  snapshot_id?: string
  manifest_reconciliation?: {
    aligned: boolean
    ticket_manifest: ManifestView | null
    ticket_output_manifest: ManifestView | null
    adopted_manifest: ManifestView | null
    current_manifest: ManifestView | null
    adopted_files_missing_from_current: string[]
    current_files_not_adopted: string[]
  }
}

async function postJson<T>(path: string, body: unknown): Promise<T> {
  const response = await request(path, { method: 'POST', body: JSON.stringify(body) })
  const payload = await response.json()
  if (!response.ok) {
    throw new Error(typeof payload?.detail === 'string' ? payload.detail : '接口返回异常，数据未更新')
  }
  return payload as T
}

async function getJson<T>(path: string): Promise<T> {
  const response = await request(path)
  const payload = await response.json()
  if (!response.ok) {
    throw new Error(typeof payload?.detail === 'string' ? payload.detail : '接口返回异常，数据未更新')
  }
  return payload as T
}

export function loadMergeDetail(mergeId: string, lakeTable: string): Promise<MergeDetail> {
  const query = new URLSearchParams({ lake_table: lakeTable }).toString()
  return getJson<MergeDetail>(`/api/handoff/merge/${encodeURIComponent(mergeId)}?${query}`)
}

export function reclaimMergeReceipt(
  mergeId: string,
  lakeTable: string,
): Promise<{ receipt: Record<string, unknown>; product_file_ids: string[]; request_matches_first_confirmation: boolean }> {
  const query = new URLSearchParams({ lake_table: lakeTable }).toString()
  return getJson(`/api/handoff/merge/${encodeURIComponent(mergeId)}/receipt?${query}`)
}

export function confirmMerge(
  mergeId: string,
  lakeTable: string,
): Promise<{ phase: string; receipt?: Record<string, unknown> }> {
  return postJson(`/api/handoff/merge/${encodeURIComponent(mergeId)}/confirm`, {
    values: { 所属湖表: lakeTable },
  })
}

export type CompactScan = {
  lake_table: string
  partition_key: string
  manifest: ManifestView | null
  file_ids: string[]
  file_count: number
  rows: number
  skipped_staged: string[]
}

export function scanCompactPartition(lakeTable: string, partitionKey: string): Promise<CompactScan> {
  const query = new URLSearchParams({ lake_table: lakeTable, partition_key: partitionKey }).toString()
  return getJson<CompactScan>(`/api/handoff/compact/scan?${query}`)
}

export function openCompactTicket(
  lakeTable: string,
  partitionKey: string,
): Promise<{ ticket: Record<string, unknown>; manifest: ManifestView }> {
  return postJson('/api/handoff/compact/tickets', {
    values: { 所属湖表: lakeTable, 目标分区: partitionKey, 触发条件: '工作台压实入口' },
  })
}

export function executeCompactTicket(ticketId: string): Promise<CompactReceipt> {
  return postJson(`/api/handoff/compact/tickets/${encodeURIComponent(ticketId)}/execute`, {})
}

export function loadCompactReceipt(ticketId: string): Promise<CompactReceipt> {
  return getJson(`/api/handoff/compact/tickets/${encodeURIComponent(ticketId)}/receipt`)
}
