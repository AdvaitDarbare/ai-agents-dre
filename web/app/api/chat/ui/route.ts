import { randomUUID } from 'node:crypto';

import { createUIMessageStream, createUIMessageStreamResponse, type UIMessageStreamWriter } from 'ai';

import { parseDeepDiveEnvelope } from '@/dre/copilot-constants';
import type { CopilotMessage } from '@/dre/copilot-types';

const BACKEND_URL = process.env.NEXT_PUBLIC_BACKEND_URL || 'http://127.0.0.1:8000';

type JsonObject = Record<string, unknown>;

type DatasetRow = {
  name: string;
};

type HistoryRow = {
  run_id?: string;
  status?: string;
  reason?: string;
  quality_score?: number;
};

type PendingContract = {
  dataset_name: string;
  proposed_at?: string;
  row_count?: number;
  column_count?: number;
  pending_files?: string[];
};

type PulseRow = {
  name: string;
  status: string;
  quality_score?: number;
  lifecycle?: string;
  reason?: string;
};

type SloSummaryResponse = {
  dataset_name?: string;
  window?: number;
  summary?: Array<{
    slo_name?: string;
    attainment?: number;
    passed_runs?: number;
    total_runs?: number;
    latest_status?: string;
    budget_burn?: number;
    latest_timestamp?: string | null;
  }>;
  checks?: Array<{
    slo_name?: string;
    total_checks?: number;
    pass_checks?: number;
    pass_rate?: number;
    last_status?: string;
    total_error_budget_burn?: number;
    avg_error_budget_burn?: number;
    last_seen?: string | null;
  }>;
};

type DiagnosticsRecord = {
  id: number;
  run_id?: string | null;
  dataset_name: string;
  column_name?: string | null;
  check_type: string;
  severity?: string;
  violation_count?: number;
  sample_records?: Array<Record<string, unknown>>;
  metadata?: Record<string, unknown>;
  created_at?: string | null;
};

type DiagnosticsResponse = {
  dataset_name: string;
  run_id?: string | null;
  check_type?: string | null;
  limit?: number;
  records: DiagnosticsRecord[];
  total: number;
};

type DatasetPreview = {
  columns?: string[];
  data?: Array<Record<string, unknown>>;
  total_rows?: number;
};

type ContractResponse = {
  yaml_content?: string;
};

type DimensionScoresResponse = {
  overall_score?: number;
  dimensions?: Array<Record<string, unknown>>;
};

type RunVerdictResponse = {
  run_id?: string;
  status?: string;
  reason?: string;
  quality_score?: number;
  dimension_scores?: Record<string, unknown> | null;
  full_verdict?: Record<string, unknown> | null;
};

async function fetchBackendJson<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${BACKEND_URL}${path}`, {
    cache: 'no-store',
    ...init,
    headers: {
      'content-type': 'application/json',
      ...(init?.headers || {}),
    },
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`Backend ${path} failed: ${response.status} ${body}`);
  }

  return (await response.json()) as T;
}

function extractLatestUserQuery(messages: JsonObject[]): string {
  for (let i = messages.length - 1; i >= 0; i -= 1) {
    const message = messages[i];
    if (String(message.role || '').toLowerCase() !== 'user') {
      continue;
    }

    const content = message.content;
    if (typeof content === 'string' && content.trim()) {
      return content.trim();
    }

    const parts = message.parts;
    if (Array.isArray(parts)) {
      const text = parts
        .filter((part) => part && typeof part === 'object' && (part as JsonObject).type === 'text')
        .map((part) => String((part as JsonObject).text || '').trim())
        .filter(Boolean)
        .join(' ')
        .trim();

      if (text) return text;
    }
  }

  return '';
}

function extractMessageText(message: JsonObject): string {
  const content = message.content;
  if (typeof content === 'string' && content.trim()) return content.trim();
  const parts = message.parts;
  if (!Array.isArray(parts)) return '';
  return parts
    .filter((part) => part && typeof part === 'object' && (part as JsonObject).type === 'text')
    .map((part) => String((part as JsonObject).text || '').trim())
    .filter(Boolean)
    .join(' ')
    .trim();
}

function extractConversationTurns(messages: JsonObject[], maxTurns = 8): Array<{ role: string; text: string }> {
  const turns: Array<{ role: string; text: string }> = [];
  for (const message of messages) {
    const role = String(message.role || '').toLowerCase();
    if (role !== 'user' && role !== 'assistant') continue;
    const rawText = extractMessageText(message);
    if (!rawText) continue;
    const parsed = role === 'user' ? parseDeepDiveEnvelope(rawText) : null;
    const text = (parsed?.visible_prompt || rawText).trim();
    if (!text) continue;
    turns.push({ role, text });
  }
  if (turns.length <= maxTurns) return turns;
  return turns.slice(turns.length - maxTurns);
}

function includesAny(text: string, tokens: string[]): boolean {
  const lower = text.toLowerCase();
  return tokens.some((token) => lower.includes(token));
}

function chooseDatasetFromQuery(query: string, datasets: DatasetRow[]): string | null {
  const lowered = query.toLowerCase();
  const direct = datasets.find((dataset) => lowered.includes(dataset.name.toLowerCase()));
  if (direct) return direct.name;
  return datasets[0]?.name || null;
}

const DIMENSION_PATTERNS: Record<string, string[]> = {
  validity: ['PATTERN', 'ALLOWED', 'TYPE_MISMATCH', 'SCHEMA_TYPE_MISMATCH', 'SCHEMA_MISSING_COLUMN', 'INVALID'],
  completeness: ['MISSING', 'NULL', 'ROW_COUNT'],
  uniqueness: ['DUPLICATE', 'UNIQUE', 'PRIMARY_KEY'],
  accuracy: ['RANGE', 'ACCURACY', 'CUSTOM_CHECK'],
  timeliness: ['FRESHNESS', 'TIMELINESS', 'ANOMALY_FRESHNESS'],
  consistency: ['CUSTOM_CHECK', 'CONSISTENCY', 'ANOMALY_'],
};

function chooseDimensionFromQuery(query: string): string | null {
  const lower = query.toLowerCase();
  if (lower.includes('validity')) return 'validity';
  if (lower.includes('completeness')) return 'completeness';
  if (lower.includes('uniqueness') || lower.includes('duplicate')) return 'uniqueness';
  if (lower.includes('accuracy')) return 'accuracy';
  if (lower.includes('timeliness') || lower.includes('freshness')) return 'timeliness';
  if (lower.includes('consistency')) return 'consistency';
  return null;
}

function diagnosticsMatchDimension(row: DiagnosticsRecord, dimension: string | null): boolean {
  if (!dimension) return true;
  const patterns = DIMENSION_PATTERNS[dimension] || [];
  const checkType = String(row.check_type || '').toUpperCase();
  if (patterns.some((pattern) => checkType.includes(pattern))) return true;
  const metadataText = JSON.stringify(row.metadata || {}).toUpperCase();
  return patterns.some((pattern) => metadataText.includes(pattern));
}

function writeTextChunk(
  writer: UIMessageStreamWriter<CopilotMessage>,
  id: string,
  text: string,
) {
  writer.write({ type: 'text-start', id });
  const chunks = text.split(/(\s+)/).filter(Boolean);
  for (const chunk of chunks) {
    writer.write({ type: 'text-delta', id, delta: chunk });
  }
  writer.write({ type: 'text-end', id });
}

export async function POST(request: Request) {
  let payload: JsonObject = {};
  try {
    payload = (await request.json()) as JsonObject;
  } catch {
    payload = {};
  }

  const messages = Array.isArray(payload.messages) ? (payload.messages as JsonObject[]) : [];
  const rawQuery = extractLatestUserQuery(messages);
  const deepDiveEnvelope = parseDeepDiveEnvelope(rawQuery);
  const isDeepDive = Boolean(deepDiveEnvelope);
  const query = deepDiveEnvelope?.visible_prompt || rawQuery;
  const hiddenContext = deepDiveEnvelope?.hidden_context || {};
  const conversationTurns = extractConversationTurns(messages, 10);

  if (!query) {
    return Response.json({ error: 'No user message found' }, { status: 400 });
  }

  const stream = createUIMessageStream<CopilotMessage>({
    execute: async ({ writer }) => {
      writer.write({ type: 'start' });

      const introId = `text-${randomUUID()}`;
      let intro = 'Here is the latest platform view.';
      let datasetsCache: DatasetRow[] = [];

      const contextDataset = String((hiddenContext as { dataset?: unknown }).dataset || '').trim();
      try {
        datasetsCache = await fetchBackendJson<DatasetRow[]>('/datasets');
      } catch {
        datasetsCache = [];
      }
      const datasetForContext = contextDataset || chooseDatasetFromQuery(query, datasetsCache);

      let datasetContext: Record<string, unknown> | null = null;
      if (datasetForContext) {
        try {
          const historyRows = await fetchBackendJson<HistoryRow[]>(`/history/${encodeURIComponent(datasetForContext)}?limit=5`);
          const latest = Array.isArray(historyRows) && historyRows.length > 0 ? historyRows[0] : null;
          const runId = String(latest?.run_id || '').trim() || undefined;

          const [contractResult, dimensionsResult, previewResult, verdictResult] = await Promise.allSettled([
            fetchBackendJson<ContractResponse>(`/contract/${encodeURIComponent(datasetForContext)}`),
            fetchBackendJson<DimensionScoresResponse>(`/quality-dimensions/${encodeURIComponent(datasetForContext)}`),
            fetchBackendJson<DatasetPreview>(`/datasets/${encodeURIComponent(datasetForContext)}/data?limit=8`),
            runId ? fetchBackendJson<RunVerdictResponse>(`/verdict/${encodeURIComponent(runId)}`) : Promise.resolve({}),
          ]);

          const contractYaml =
            contractResult.status === 'fulfilled' && typeof contractResult.value.yaml_content === 'string'
              ? contractResult.value.yaml_content
              : '';
          const contractForContext =
            contractYaml.length > 6000 ? `${contractYaml.slice(0, 6000)}\n# ...truncated` : contractYaml;
          const dimensions = dimensionsResult.status === 'fulfilled' ? dimensionsResult.value : {};
          const previewRows =
            previewResult.status === 'fulfilled' && Array.isArray(previewResult.value.data)
              ? previewResult.value.data.slice(0, 5)
              : [];
          const previewColumns =
            previewResult.status === 'fulfilled' && Array.isArray(previewResult.value.columns)
              ? previewResult.value.columns
              : [];
          const verdictPayload: RunVerdictResponse = verdictResult.status === 'fulfilled' ? verdictResult.value : {};
          const fullVerdict =
            verdictPayload.full_verdict && typeof verdictPayload.full_verdict === 'object'
              ? verdictPayload.full_verdict
              : {};
          const profile = fullVerdict && typeof fullVerdict === 'object' ? (fullVerdict as Record<string, unknown>).profile : null;
          const violationsDetail =
            profile && typeof profile === 'object'
              ? ((profile as Record<string, unknown>).violations_detail as Record<string, unknown> | undefined)
              : undefined;

          datasetContext = {
            dataset: datasetForContext,
            latest_run: latest || null,
            recent_runs: Array.isArray(historyRows) ? historyRows.slice(0, 5) : [],
            quality_dimensions: dimensions || {},
            contract_yaml: contractForContext,
            sample_preview: {
              columns: previewColumns,
              rows: previewRows,
            },
            latest_verdict: {
              run_id: verdictPayload.run_id || runId || null,
              status: verdictPayload.status || latest?.status || null,
              reason: verdictPayload.reason || latest?.reason || null,
              quality_score: verdictPayload.quality_score ?? latest?.quality_score ?? null,
              dimension_scores: verdictPayload.dimension_scores || null,
              profile_violations: violationsDetail || {},
            },
          };
        } catch {
          datasetContext = {
            dataset: datasetForContext,
          };
        }
      }

      try {
        const mergedContext: Record<string, unknown> = {
          ...(typeof hiddenContext === 'object' && hiddenContext ? (hiddenContext as Record<string, unknown>) : {}),
          conversation_turns: conversationTurns,
        };
        if (datasetContext) {
          mergedContext.dataset_context = datasetContext;
        }
        const chat = await fetchBackendJson<{ response?: string }>('/chat', {
          method: 'POST',
          body: JSON.stringify({ query, context: mergedContext }),
        });
        if (chat.response && chat.response.trim()) {
          intro = chat.response.trim();
        }
      } catch {
        intro = 'Copilot fallback: showing live platform data cards from the backend APIs.';
      }

      const needsEvidenceTone = includesAny(query, ['where', 'rows', 'records', 'evidence', 'diagnostic']);
      if (needsEvidenceTone && includesAny(intro, ['unable to provide', 'beyond the scope'])) {
        intro = 'Pulling row-level evidence from diagnostics and latest run outputs below.';
      }

      writeTextChunk(writer, introId, intro);

      const wantsPending = includesAny(query, ['pending', 'contract', 'yaml']);
      const wantsFailureDetails = includesAny(query, ['fail', 'failed', 'failing', 'blocked', 'why']);
      const wantsEvidence =
        isDeepDive ||
        wantsFailureDetails ||
        includesAny(query, ['where', 'which rows', 'row', 'records', 'preview', 'evidence', 'diagnostic']);
      const wantsPulse =
        isDeepDive || wantsFailureDetails || includesAny(query, ['health', 'pulse', 'status', 'warning', 'quality']);
      const wantsSlo =
        isDeepDive || wantsFailureDetails || includesAny(query, ['slo', 'budget', 'availability', 'anomaly', 'burn']);

      if (!wantsPending && !wantsPulse && !wantsSlo && !wantsEvidence) {
        writer.write({ type: 'finish' });
        return;
      }

      if (wantsPending) {
        try {
          const pending = await fetchBackendJson<PendingContract[]>('/contracts/pending');
          const toolCallId = `tool-${randomUUID()}`;

          writer.write({
            type: 'tool-input-available',
            toolCallId,
            toolName: 'showPendingContracts',
            input: { limit: 12 },
          });

          writer.write({
            type: 'tool-output-available',
            toolCallId,
            output: {
              count: pending.length,
              generated_at: new Date().toISOString(),
              datasets: pending.slice(0, 12).map((item) => ({
                dataset_name: item.dataset_name,
                proposed_at: item.proposed_at,
                row_count: item.row_count,
                column_count: item.column_count,
                pending_files: Array.isArray(item.pending_files) ? item.pending_files.length : 0,
              })),
            },
          });
        } catch (error) {
          const errorId = `text-${randomUUID()}`;
          writeTextChunk(
            writer,
            errorId,
            `Could not load pending contracts: ${error instanceof Error ? error.message : 'unknown error'}`,
          );
        }
      }

      if (wantsPulse) {
        try {
          const pulse = await fetchBackendJson<PulseRow[]>('/pulse');
          const toolCallId = `tool-${randomUUID()}`;
          const healthy = pulse.filter((row) => row.status === 'PASSED').length;
          const blocked = pulse.filter((row) => row.status === 'BLOCKED').length;
          const warning = pulse.filter((row) => row.status === 'WARNING').length;

          writer.write({
            type: 'tool-input-available',
            toolCallId,
            toolName: 'showPulseSnapshot',
            input: { statusFilter: 'all' },
          });

          writer.write({
            type: 'tool-output-available',
            toolCallId,
            output: {
              total: pulse.length,
              healthy,
              warning,
              blocked,
              generated_at: new Date().toISOString(),
              rows: pulse.slice(0, 8),
            },
          });
        } catch (error) {
          const errorId = `text-${randomUUID()}`;
          writeTextChunk(
            writer,
            errorId,
            `Could not load pulse snapshot: ${error instanceof Error ? error.message : 'unknown error'}`,
          );
        }
      }

      if (wantsSlo) {
        try {
          const datasets = datasetsCache.length > 0 ? datasetsCache : await fetchBackendJson<DatasetRow[]>('/datasets');
          const datasetName = contextDataset || chooseDatasetFromQuery(query, datasets);
          if (datasetName) {
            const summary = await fetchBackendJson<SloSummaryResponse>(
              `/slos/${encodeURIComponent(datasetName)}/summary?window=200`,
            );

            const summaryRows = Array.isArray(summary.summary)
              ? summary.summary
                  .map((row) => ({
                    slo_name: String(row.slo_name || 'unknown'),
                    attainment: Number(row.attainment || 0),
                    passed_runs: Number(row.passed_runs || 0),
                    total_runs: Number(row.total_runs || 0),
                    latest_status: String(row.latest_status || 'UNKNOWN'),
                    budget_burn: Number(row.budget_burn || 0),
                    latest_timestamp: row.latest_timestamp || null,
                  }))
              : [];
            const checkRows = Array.isArray(summary.checks)
              ? summary.checks.map((row) => ({
                  slo_name: String(row.slo_name || 'unknown'),
                  attainment: Number(row.pass_rate || 0),
                  passed_runs: Number(row.pass_checks || 0),
                  total_runs: Number(row.total_checks || 0),
                  latest_status: String(row.last_status || 'UNKNOWN'),
                  budget_burn: Number(row.total_error_budget_burn ?? row.avg_error_budget_burn ?? 0),
                  latest_timestamp: row.last_seen || null,
                }))
              : [];
            const rows = summaryRows.length > 0 ? summaryRows : checkRows;

            const toolCallId = `tool-${randomUUID()}`;
            writer.write({
              type: 'tool-input-available',
              toolCallId,
              toolName: 'showSloSummary',
              input: { dataset_name: datasetName, window: 200 },
            });

            writer.write({
              type: 'tool-output-available',
              toolCallId,
              output: {
                dataset_name: datasetName,
                window: Number(summary.window || 200),
                generated_at: new Date().toISOString(),
                summary: rows,
              },
            });
          }
        } catch (error) {
          const errorId = `text-${randomUUID()}`;
          writeTextChunk(
            writer,
            errorId,
            `Could not load SLO summary: ${error instanceof Error ? error.message : 'unknown error'}`,
          );
        }
      }

      if (wantsEvidence) {
        try {
          const datasets = datasetsCache.length > 0 ? datasetsCache : await fetchBackendJson<DatasetRow[]>('/datasets');
          const datasetName = contextDataset || chooseDatasetFromQuery(query, datasets);
          if (datasetName) {
            const dimensionFilter = chooseDimensionFromQuery(query);
            const historyRows = await fetchBackendJson<HistoryRow[]>(
              `/history/${encodeURIComponent(datasetName)}?limit=1`,
            );
            const latest = Array.isArray(historyRows) && historyRows.length > 0 ? historyRows[0] : null;
            const runId = String(latest?.run_id || '').trim() || undefined;

            const [diagResult, previewResult] = await Promise.allSettled([
              fetchBackendJson<DiagnosticsResponse>(
                `/diagnostics/${encodeURIComponent(datasetName)}?${new URLSearchParams({
                  ...(runId ? { run_id: runId } : {}),
                  limit: '150',
                }).toString()}`,
              ),
              fetchBackendJson<DatasetPreview>(`/datasets/${encodeURIComponent(datasetName)}/data?limit=20`),
            ]);

            const diagnostics =
              diagResult.status === 'fulfilled' && Array.isArray(diagResult.value.records)
                ? diagResult.value.records
                : [];
            const matched = diagnostics.filter((row) => diagnosticsMatchDimension(row, dimensionFilter));

            const grouped = new Map<string, { check_type: string; violation_count: number; sample_count: number; column_name?: string | null }>();
            for (const row of matched) {
              const key = `${row.check_type}|${row.column_name || ''}`;
              const existing = grouped.get(key) || {
                check_type: row.check_type,
                violation_count: 0,
                sample_count: 0,
                column_name: row.column_name || null,
              };
              existing.violation_count += Number(row.violation_count || 0);
              existing.sample_count += Array.isArray(row.sample_records) ? row.sample_records.length : 0;
              grouped.set(key, existing);
            }

            const evidenceSummary = Array.from(grouped.values())
              .sort((a, b) => b.violation_count - a.violation_count || b.sample_count - a.sample_count)
              .slice(0, 12);

            const evidenceRows = matched.flatMap((row) =>
              Array.isArray(row.sample_records) ? row.sample_records : [],
            );
            const previewRows =
              previewResult.status === 'fulfilled' && Array.isArray(previewResult.value.data)
                ? previewResult.value.data
                : [];
            const sampleRows = (evidenceRows.length > 0 ? evidenceRows : previewRows).slice(0, 10);
            const sampleColumns =
              sampleRows.length > 0
                ? Object.keys(sampleRows[0] || {}).slice(0, 12)
                : previewResult.status === 'fulfilled' && Array.isArray(previewResult.value.columns)
                  ? previewResult.value.columns.slice(0, 12)
                  : [];

            const toolCallId = `tool-${randomUUID()}`;
            writer.write({
              type: 'tool-input-available',
              toolCallId,
              toolName: 'showFailureEvidence',
              input: { dataset_name: datasetName, run_id: runId, dimension: dimensionFilter, limit: 150 },
            });

            writer.write({
              type: 'tool-output-available',
              toolCallId,
              output: {
                dataset_name: datasetName,
                run_id: runId || null,
                run_status: latest?.status || 'UNKNOWN',
                run_reason: latest?.reason || '',
                generated_at: new Date().toISOString(),
                dimension_filter: dimensionFilter,
                evidence_summary: evidenceSummary,
                sample_rows: sampleRows,
                sample_columns: sampleColumns,
              },
            });
          }
        } catch (error) {
          const errorId = `text-${randomUUID()}`;
          writeTextChunk(
            writer,
            errorId,
            `Could not load failure evidence: ${error instanceof Error ? error.message : 'unknown error'}`,
          );
        }
      }

      writer.write({ type: 'finish' });
    },
    onError: (error) => (error instanceof Error ? error.message : 'Copilot stream error'),
  });

  return createUIMessageStreamResponse({ stream });
}
