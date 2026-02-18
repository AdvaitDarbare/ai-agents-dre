export type PulseRow = {
  name: string;
  status: string;
  criticality?: string;
  lifecycle?: string;
  last_scanned?: string | null;
  quality_score?: number;
  reason?: string;
};

export type DatasetRow = {
  name: string;
  lifecycle?: string;
  criticality?: string;
  data_file?: string | null;
};

export type PendingContract = {
  dataset_name: string;
  proposed_at?: string;
  source_file?: string;
  row_count?: number;
  column_count?: number;
  proposed_yaml?: string;
  pending_files?: string[];
  status?: string;
};

export type RunEvent = {
  id: string;
  dataset: string;
  status: string;
  timestamp?: string;
  time?: string;
  date?: string;
  duration?: string | null;
  quality_score?: number;
  reason?: string;
};

export type IncidentItem = {
  incident_id: string;
  run_id?: string;
  dataset_name?: string;
  dataset?: string;
  severity: string;
  status: 'OPEN' | 'ACK' | 'RESOLVED' | string;
  owner?: string | null;
  title?: string;
  description?: string;
  reason?: string;
  quality_score?: number | null;
  anomaly_count?: number | null;
  z_score_max?: number | null;
  timestamp?: string | null;
  created_at?: string | null;
  updated_at?: string | null;
};

export type LineageGraph = {
  datasets?: Record<
    string,
    {
      upstream?: Array<string | { name?: string }>;
      consumers?: Array<string | { name?: string }>;
      owner?: string;
      domain?: string;
      criticality?: string;
    }
  >;
  summary?: {
    dataset_count?: number;
    upstream_edge_count?: number;
    downstream_edge_count?: number;
    managed_upstream_edge_count?: number;
    managed_consumer_edge_count?: number;
    external_upstream_count?: number;
    consumer_count?: number;
    isolated_dataset_count?: number;
    owner_coverage_pct?: number;
  };
  issues?: {
    external_upstream_refs?: Array<{
      dataset: string;
      upstream: string;
    }>;
    invalid_upstream_refs?: Array<{
      dataset: string;
      upstream: string;
    }>;
  };
  graph?: {
    nodes?: Array<{
      id: string;
      kind?: string;
      owner?: string;
      criticality?: string;
    }>;
    edges?: Array<{
      source: string;
      target: string;
      relation?: string;
      managed?: boolean;
    }>;
  };
  context?: {
    dataset?: string;
    max_depth?: number;
    upstream?: Array<{ name: string; depth?: number; managed?: boolean }>;
    downstream?: Array<{ name: string; depth?: number; managed?: boolean }>;
  };
};

export type SourceIntegration = {
  id: string;
  name: string;
  type?: string;
  status: 'CONNECTED' | 'ERROR' | 'DISCONNECTED' | string;
  dataset_count: number;
  discovered_count?: number;
  last_checked?: string;
  details?: Record<string, any>;
};

export type SourceIntegrationsResponse = {
  integrations: SourceIntegration[];
  count?: number;
  generated_at?: string;
};

export type PlatformConfig = {
  generated_at?: string;
  runtime?: Record<string, any>;
};

export type RuntimeResetResponse = {
  status: string;
  generated_at?: string;
  db?: {
    truncated_tables?: string[];
    checkpoint_tables_cleared?: boolean;
  };
  files?: {
    summary?: Record<string, number>;
    removed_count?: number;
    removed_examples?: string[];
  };
  contracts?: {
    generated_contracts_cleared?: boolean;
    preserved_contract_names?: string[];
    removed_contract_count?: number;
    removed_contract_history_count?: number;
  };
};

export type SloCheck = {
  timestamp?: string | null;
  run_id?: string;
  slo_name: string;
  operator?: string;
  target_value?: number | null;
  observed_value?: number | null;
  status: string;
  error_budget_burn?: number | null;
  metadata?: Record<string, any>;
};

export type SloSummary = {
  dataset_name: string;
  window: number;
  overall_status?: 'PASS' | 'FAIL' | string;
  overall_pass_rate?: number | null;
  overall_fail_rate?: number | null;
  total_checks: number;
  failing_checks?: number;
  failing_slo_count?: number;
  failing_slos?: string[];
  overall_error_budget_burn_avg?: number | null;
  overall_error_budget_burn_total?: number;
  checks: Array<{
    slo_name: string;
    total_checks: number;
    pass_checks: number;
    pass_rate?: number | null;
    avg_error_budget_burn?: number | null;
    total_error_budget_burn?: number;
    last_seen?: string | null;
    last_status?: string;
    recent_fail_streak?: number;
  }>;
};

export type AsyncJob = {
  job_id: string;
  action: 'evaluate' | 'delete' | 'bulk_delete' | 'remediation_apply' | string;
  dataset_name: string;
  status: 'QUEUED' | 'RUNNING' | 'COMPLETED' | 'FAILED' | string;
  requested_at?: string | null;
  started_at?: string | null;
  finished_at?: string | null;
  duration_ms?: number | null;
  result?: any;
  error?: string | null;
};

export type GlobalStats = {
  total_runs_today: number;
  pass_rate_today: number;
  avg_duration: number;
};

export type SystemHealthRow = {
  dataset: string;
  upstream: any;
};

export type DatasetRunHistoryItem = {
  timestamp?: string;
  status?: string;
  quality_score?: number;
  reason?: string;
  anomaly_count?: number;
  anomalies?: string[];
  run_id?: string;
};

export type MetricSnapshot = {
  run_timestamp?: string | null;
  metrics?: Record<string, number>;
};

export type BaselineRow = {
  metric: string;
  mean?: number | null;
  std?: number | null;
  type?: string | null;
  last_updated?: string | null;
  sample_count?: number | null;
  upper_3sigma?: number | null;
  lower_3sigma?: number | null;
};

export type MetricTimeseriesPoint = {
  timestamp?: string | null;
  value?: number | null;
  run_id?: string | null;
  day_of_week?: number | null;
  metric_group?: string | null;
  column_name?: string | null;
  segment?: string | null;
  tags?: Record<string, any>;
};

export type MetricTimeseriesResponse = {
  dataset: string;
  metric: string;
  baseline?: {
    mean: number;
    std: number;
    type?: string | null;
    sample_count?: number | null;
    upper_3sigma?: number;
    lower_3sigma?: number;
    upper_2sigma?: number;
    lower_2sigma?: number;
  } | null;
  data: MetricTimeseriesPoint[];
};

export type DatasetPreview = {
  columns: string[];
  data: Array<Record<string, any>>;
  preview_limit?: number;
  total_rows?: number;
};

export type ProfileResponse = Record<string, any>;

export type RunVerdictResponse = {
  run_id: string;
  timestamp?: string | null;
  dataset_name?: string;
  status?: string;
  quality_score?: number | null;
  anomaly_count?: number | null;
  z_score_max?: number | null;
  reason?: string;
  duration_ms?: number | null;
  dimension_scores?: Record<string, any> | null;
  full_verdict?: Record<string, any> | null;
};

export type DiagnosticsRecord = {
  id: number;
  run_id?: string | null;
  dataset_name: string;
  column_name?: string | null;
  check_type: string;
  severity?: string;
  violation_count?: number;
  sample_records?: Array<Record<string, any>>;
  metadata?: Record<string, any>;
  created_at?: string | null;
};

export type DiagnosticsResponse = {
  dataset_name: string;
  run_id?: string | null;
  check_type?: string | null;
  limit?: number;
  records: DiagnosticsRecord[];
  total: number;
};

export type GovernanceHistoryRow = {
  filename: string;
  timestamp: string;
  summary?: string;
};

export type GovernanceFileResponse = {
  content: string;
};

export type RemediationPlanResponse = {
  status?: string;
  original_yaml?: string;
  observed_yaml?: string;
  proposed_yaml?: string;
  deterministic_yaml?: string;
  merge_summary?: any;
  generation?: any;
  error?: string;
};

export type ContractContentResponse = {
  content: string;
};

export type ContractYamlResponse = {
  yaml_content: string;
  path?: string;
};

export type ContractVersionRow = {
  version_id: string;
  dataset_name: string;
  timestamp?: string | null;
  changed_by?: string;
  yaml_content: string;
  change_type?: string;
};

export type PolicyDecision = {
  allowed: boolean;
  action?: string;
  decision?: string;
  requires_approval?: boolean;
  reason?: string;
  risk_level?: string;
  [key: string]: any;
};

export type AuditSummaryRow = {
  action: string;
  status?: string | null;
  count: number;
};

export type WorkflowTimelineEvent = {
  event_id: string;
  timestamp?: string | null;
  channel: 'audit' | 'run' | 'job' | 'tool' | 'incident' | string;
  event: string;
  status?: string | null;
  dataset_name?: string | null;
  message?: string;
  details?: Record<string, any>;
  refs?: {
    run_id?: string | null;
    job_id?: string | null;
    incident_id?: string | null;
  };
};

export type WorkflowTimelineResponse = {
  generated_at?: string;
  dataset_name?: string | null;
  limit?: number;
  events: WorkflowTimelineEvent[];
  summary?: {
    total_events?: number;
    channels?: Record<string, number>;
    statuses?: Record<string, number>;
    active_jobs?: number;
    active_incidents?: number;
  };
  runtime?: {
    langgraph_hitl_enabled?: boolean;
    langgraph_agentic_enabled?: boolean;
  };
};

export type AgenticWorkflowGraph = {
  engine: string;
  mermaid: string;
};

export type AIBrief = {
  dataset_name: string;
  generated_at?: string;
  run_id?: string | null;
  ai_summary?: string;
  deterministic_actions?: string[];
  risk?: Record<string, any>;
  investigation?: Record<string, any>;
  quality?: Record<string, any>;
  impact?: Record<string, any>;
  slo?: Record<string, any>;
  remediation?: Record<string, any>;
};

export type AgenticRemediationAttempt = {
  attempt_no: number;
  input_run_id?: string | null;
  classification?: string;
  proposed_diff_summary?: string | null;
  confidence?: number | null;
  applied?: boolean;
  output_run_id?: string | null;
  result_status?: string | null;
  error?: string | null;
  details?: Record<string, any>;
  created_at?: string | null;
};

export type AgenticRemediationRun = {
  id: string;
  dataset_name: string;
  initial_run_id?: string | null;
  final_run_id?: string | null;
  status: 'AUTO_FIXED' | 'PLAN_REQUIRED' | 'BLOCKED_BY_POLICY' | 'FAILED' | 'RUNNING' | string;
  attempt_count?: number;
  policy_blocks?: number;
  summary?: Record<string, any>;
  created_at?: string | null;
  updated_at?: string | null;
  attempts?: AgenticRemediationAttempt[];
  timeline?: Array<{
    attempt_no?: number;
    step?: string;
    status?: string;
    message?: string;
    timestamp?: string;
  }>;
  plan?: Record<string, any> | null;
  applied_changes?: Array<Record<string, any>>;
};

export type BacktestingResult = {
  dataset_name?: string;
  metric?: string;
  window_size?: number;
  total_points?: number;
  true_positives?: number;
  false_positives?: number;
  false_negatives?: number;
  precision?: number | null;
  recall?: number | null;
  f1_score?: number | null;
  [key: string]: any;
};

// Default to Next proxy so Vercel deployments avoid browser->backend CORS/network issues.
// When NEXT_PUBLIC_BACKEND_URL is set (local dev), use it directly.
const API_BASE = process.env.NEXT_PUBLIC_BACKEND_URL || '/api/backend';
const DEFAULT_API_TIMEOUT_MS = Number(process.env.NEXT_PUBLIC_API_TIMEOUT_MS || '15000');

export class ApiError extends Error {
  status: number;
  statusText: string;
  body: unknown;

  constructor(status: number, statusText: string, body: unknown, fallbackMessage: string) {
    const detail =
      body && typeof body === 'object' && 'detail' in body
        ? (body as { detail?: unknown }).detail
        : undefined;
    const detailMessage =
      detail && typeof detail === 'object' && 'message' in detail
        ? String((detail as { message?: unknown }).message || '')
        : '';

    super(detailMessage || fallbackMessage);
    this.name = 'ApiError';
    this.status = status;
    this.statusText = statusText;
    this.body = body;
  }
}

async function parseResponse<T>(res: Response): Promise<T> {
  if (!res.ok) {
    const text = await res.text();
    let parsed: unknown = null;
    try {
      parsed = text ? JSON.parse(text) : null;
    } catch {
      parsed = null;
    }
    throw new ApiError(
      res.status,
      res.statusText,
      parsed,
      `${res.status} ${res.statusText}: ${text || 'Request failed'}`,
    );
  }
  return (await res.json()) as T;
}

async function fetchWithTimeout(input: string, init?: RequestInit, timeoutMs = DEFAULT_API_TIMEOUT_MS): Promise<Response> {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(input, { ...init, signal: controller.signal });
  } catch (error) {
    if (error instanceof DOMException && error.name === 'AbortError') {
      throw new Error(`Request timed out after ${timeoutMs}ms: ${input}`);
    }
    throw error;
  } finally {
    clearTimeout(timeoutId);
  }
}

export async function getPulse(): Promise<PulseRow[]> {
  const res = await fetchWithTimeout(`${API_BASE}/pulse`, { cache: 'no-store' });
  return parseResponse<PulseRow[]>(res);
}

export async function getDatasets(): Promise<DatasetRow[]> {
  const res = await fetchWithTimeout(`${API_BASE}/datasets`, { cache: 'no-store' });
  return parseResponse<DatasetRow[]>(res);
}

export async function getPendingContracts(): Promise<PendingContract[]> {
  const res = await fetchWithTimeout(`${API_BASE}/contracts/pending`, { cache: 'no-store' });
  return parseResponse<PendingContract[]>(res);
}

export async function approveContract(datasetName: string, approvedYaml: string): Promise<any> {
  const res = await fetch(`${API_BASE}/contracts/approve`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      dataset_name: datasetName,
      approved_yaml: approvedYaml,
    }),
  });
  return parseResponse(res);
}

export async function rejectContract(datasetName: string): Promise<any> {
  const res = await fetch(`${API_BASE}/contracts/pending/${encodeURIComponent(datasetName)}`, {
    method: 'DELETE',
  });
  return parseResponse(res);
}

export async function runScan(datasetName: string, options?: { forceLoad?: boolean }): Promise<void> {
  const res = await fetch(`${API_BASE}/evaluate/${encodeURIComponent(datasetName)}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ force_load: !!options?.forceLoad }),
  });
  await parseResponse(res);
}

export async function deleteDataset(datasetName: string): Promise<void> {
  const res = await fetch(`${API_BASE}/datasets/${encodeURIComponent(datasetName)}`, {
    method: 'DELETE',
  });
  await parseResponse(res);
}

export async function enqueueScanJob(datasetName: string, options?: { forceLoad?: boolean }): Promise<AsyncJob> {
  const res = await fetch(`${API_BASE}/jobs/evaluate/${encodeURIComponent(datasetName)}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ force_load: !!options?.forceLoad }),
  });
  return parseResponse<AsyncJob>(res);
}

export async function enqueueDeleteJob(
  datasetName: string,
  options?: { policyApproved?: boolean; policyReason?: string },
): Promise<AsyncJob> {
  const body: Record<string, unknown> = { confirm: true };
  if (options?.policyApproved) body.policy_approved = true;
  if (options?.policyReason && options.policyReason.trim()) body.policy_reason = options.policyReason.trim();

  const res = await fetch(`${API_BASE}/jobs/delete/${encodeURIComponent(datasetName)}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
  return parseResponse<AsyncJob>(res);
}

export async function enqueueBulkDeleteJob(
  datasetNames: string[],
  options?: { policyApproved?: boolean; policyReason?: string },
): Promise<AsyncJob> {
  const body: Record<string, unknown> = { dataset_names: datasetNames, confirm: true };
  if (options?.policyApproved) body.policy_approved = true;
  if (options?.policyReason && options.policyReason.trim()) body.policy_reason = options.policyReason.trim();

  const res = await fetch(`${API_BASE}/jobs/delete-bulk`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
  return parseResponse<AsyncJob>(res);
}

export async function enqueueBulkEvaluateJob(datasetNames: string[], options?: { forceLoad?: boolean }): Promise<AsyncJob> {
  const res = await fetch(`${API_BASE}/jobs/bulk-evaluate`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ dataset_names: datasetNames, force_load: !!options?.forceLoad }),
  });
  return parseResponse<AsyncJob>(res);
}

export async function enqueueEvaluateAllJob(options?: { includeUnconfigured?: boolean; forceLoad?: boolean }): Promise<AsyncJob> {
  const res = await fetch(`${API_BASE}/jobs/evaluate-all`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      include_unconfigured: options?.includeUnconfigured ?? true,
      force_load: !!options?.forceLoad,
    }),
  });
  return parseResponse<AsyncJob>(res);
}

export async function enqueueApplyRemediationJob(
  datasetName: string,
  proposedYaml: string,
  errorContext: string,
): Promise<AsyncJob> {
  const res = await fetch(`${API_BASE}/jobs/remediation/apply`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      dataset_name: datasetName,
      proposed_yaml: proposedYaml,
      error_context: errorContext,
    }),
  });
  return parseResponse<AsyncJob>(res);
}

export async function listJobs(limit = 50): Promise<AsyncJob[]> {
  const res = await fetchWithTimeout(`${API_BASE}/jobs?limit=${limit}`, { cache: 'no-store' });
  return parseResponse<AsyncJob[]>(res);
}

export async function getJob(jobId: string): Promise<AsyncJob> {
  const res = await fetchWithTimeout(`${API_BASE}/jobs/${encodeURIComponent(jobId)}`, { cache: 'no-store' });
  return parseResponse<AsyncJob>(res);
}

export async function getRecentRuns(limit = 100): Promise<RunEvent[]> {
  const res = await fetchWithTimeout(`${API_BASE}/runs?limit=${limit}`, { cache: 'no-store' });
  return parseResponse<RunEvent[]>(res);
}

export async function getIncidents(params?: {
  limit?: number;
  status?: string;
  severity?: string;
  dataset_name?: string;
}): Promise<IncidentItem[]> {
  const query = new URLSearchParams();
  if (params?.limit) query.set('limit', String(params.limit));
  if (params?.status) query.set('status', params.status);
  if (params?.severity) query.set('severity', params.severity);
  if (params?.dataset_name) query.set('dataset_name', params.dataset_name);
  const suffix = query.toString() ? `?${query.toString()}` : '';
  const res = await fetchWithTimeout(`${API_BASE}/incidents${suffix}`, { cache: 'no-store' });
  return parseResponse<IncidentItem[]>(res);
}

export async function updateIncident(
  incidentId: string,
  payload: { status: string; owner?: string; note?: string },
): Promise<IncidentItem> {
  const res = await fetch(`${API_BASE}/incidents/${encodeURIComponent(incidentId)}`, {
    method: 'PATCH',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(payload),
  });
  return parseResponse<IncidentItem>(res);
}

export async function getLineage(dataset?: string): Promise<LineageGraph> {
  const suffix = dataset ? `?dataset=${encodeURIComponent(dataset)}` : '';
  const res = await fetchWithTimeout(`${API_BASE}/lineage${suffix}`, { cache: 'no-store' });
  return parseResponse<LineageGraph>(res);
}

export async function getSourceIntegrations(): Promise<SourceIntegrationsResponse> {
  const res = await fetch(`${API_BASE}/integrations/sources`, { cache: 'no-store' });
  return parseResponse<SourceIntegrationsResponse>(res);
}

export async function getPlatformConfig(): Promise<PlatformConfig> {
  const res = await fetch(`${API_BASE}/platform/config`, { cache: 'no-store' });
  return parseResponse<PlatformConfig>(res);
}

export async function resetRuntimeState(request: {
  confirm_phrase: string;
  clear_generated_contracts?: boolean;
  preserve_contracts?: string[];
  clear_langgraph_checkpoints?: boolean;
}): Promise<RuntimeResetResponse> {
  const res = await fetch(`${API_BASE}/platform/reset-runtime`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(request),
  });
  return parseResponse<RuntimeResetResponse>(res);
}

export async function checkPolicy(request: {
  action: string;
  dataset_name?: string;
  dataset_names?: string[];
}): Promise<PolicyDecision> {
  const res = await fetch(`${API_BASE}/policy/check`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(request),
  });
  return parseResponse<PolicyDecision>(res);
}

export async function getAuditSummary(params?: {
  window_minutes?: number;
  action?: string;
  dataset_name?: string;
  status?: string;
}): Promise<AuditSummaryRow[]> {
  const query = new URLSearchParams();
  if (params?.window_minutes) query.set('window_minutes', String(params.window_minutes));
  if (params?.action) query.set('action', params.action);
  if (params?.dataset_name) query.set('dataset_name', params.dataset_name);
  if (params?.status) query.set('status', params.status);
  const suffix = query.toString() ? `?${query.toString()}` : '';
  const res = await fetch(`${API_BASE}/audit/summary${suffix}`, { cache: 'no-store' });
  const payload = await parseResponse<{ rows?: AuditSummaryRow[] } | AuditSummaryRow[]>(res);
  if (Array.isArray(payload)) return payload;
  return Array.isArray(payload.rows) ? payload.rows : [];
}

export async function getWorkflowTimeline(params?: {
  dataset_name?: string;
  limit?: number;
}): Promise<WorkflowTimelineResponse> {
  const query = new URLSearchParams();
  if (params?.dataset_name) query.set('dataset_name', params.dataset_name);
  if (params?.limit) query.set('limit', String(params.limit));
  const suffix = query.toString() ? `?${query.toString()}` : '';
  const res = await fetch(`${API_BASE}/workflow/timeline${suffix}`, { cache: 'no-store' });
  return parseResponse<WorkflowTimelineResponse>(res);
}

export function openWorkflowTimelineStream(params?: {
  dataset_name?: string;
  limit?: number;
  interval_ms?: number;
}): EventSource {
  const query = new URLSearchParams();
  if (params?.dataset_name) query.set('dataset_name', params.dataset_name);
  if (params?.limit) query.set('limit', String(params.limit));
  if (params?.interval_ms) query.set('interval_ms', String(params.interval_ms));
  const suffix = query.toString() ? `?${query.toString()}` : '';
  return new EventSource(`${API_BASE}/workflow/timeline/stream${suffix}`);
}

export async function getAgenticWorkflowGraph(): Promise<AgenticWorkflowGraph> {
  const res = await fetch(`${API_BASE}/workflow/agentic/graph`, { cache: 'no-store' });
  return parseResponse<AgenticWorkflowGraph>(res);
}

export async function runAgenticWorkflow(request: {
  dataset_name: string;
  metric?: string;
  auto_execute?: boolean;
  confidence_threshold?: number;
  policy_approved?: boolean;
  policy_reason?: string;
}): Promise<any> {
  const res = await fetch(`${API_BASE}/workflow/agentic/run`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(request),
  });
  return parseResponse(res);
}

export async function runAgenticRemediation(request: {
  dataset_name: string;
  max_retries?: number;
  autonomy_mode?: string;
}): Promise<{
  id: string;
  status: string;
  attempts: number;
  initial_run_id?: string | null;
  final_run_id?: string | null;
  applied_changes?: Array<Record<string, any>>;
  plan?: Record<string, any> | null;
  run?: AgenticRemediationRun;
}> {
  const res = await fetch(`${API_BASE}/workflow/agentic/remediate`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(request),
  });
  return parseResponse(res);
}

export async function getAgenticRemediationRun(remediationRunId: string): Promise<AgenticRemediationRun> {
  const res = await fetch(`${API_BASE}/workflow/agentic/remediate/${encodeURIComponent(remediationRunId)}`, {
    cache: 'no-store',
  });
  return parseResponse<AgenticRemediationRun>(res);
}

export function openAgenticRemediationStream(remediationRunId: string, intervalMs = 1500): EventSource {
  return new EventSource(
    `${API_BASE}/workflow/agentic/remediate/${encodeURIComponent(remediationRunId)}/stream?interval_ms=${intervalMs}`,
  );
}

export async function getAIBrief(datasetName: string, runId?: string): Promise<AIBrief> {
  const suffix = runId ? `?run_id=${encodeURIComponent(runId)}` : '';
  const res = await fetchWithTimeout(`${API_BASE}/ai/brief/${encodeURIComponent(datasetName)}${suffix}`, {
    cache: 'no-store',
  });
  return parseResponse<AIBrief>(res);
}

export async function getSloSummary(datasetName: string, window = 200): Promise<SloSummary> {
  const res = await fetch(
    `${API_BASE}/slos/${encodeURIComponent(datasetName)}/summary?window=${window}`,
    { cache: 'no-store' },
  );
  return parseResponse<SloSummary>(res);
}

export async function getSloHistory(datasetName: string, limit = 100): Promise<SloCheck[]> {
  const res = await fetch(`${API_BASE}/slos/${encodeURIComponent(datasetName)}?limit=${limit}`, {
    cache: 'no-store',
  });
  return parseResponse<SloCheck[]>(res);
}

export async function getHistory(datasetName: string, limit = 50): Promise<DatasetRunHistoryItem[]> {
  const res = await fetch(`${API_BASE}/history/${encodeURIComponent(datasetName)}?limit=${limit}`, { cache: 'no-store' });
  return parseResponse<DatasetRunHistoryItem[]>(res);
}

export async function getDatasetMetrics(datasetName: string): Promise<MetricSnapshot> {
  const res = await fetch(`${API_BASE}/metrics/${encodeURIComponent(datasetName)}`, { cache: 'no-store' });
  return parseResponse<MetricSnapshot>(res);
}

export async function getBaselines(datasetName: string): Promise<BaselineRow[]> {
  const res = await fetch(`${API_BASE}/baselines/${encodeURIComponent(datasetName)}`, { cache: 'no-store' });
  return parseResponse<BaselineRow[]>(res);
}

export async function getMetricTimeseries(
  datasetName: string,
  metric = 'row_count',
  limit = 30,
): Promise<MetricTimeseriesResponse> {
  const res = await fetch(
    `${API_BASE}/metrics/${encodeURIComponent(datasetName)}/timeseries?metric=${encodeURIComponent(metric)}&limit=${limit}`,
    { cache: 'no-store' },
  );
  return parseResponse<MetricTimeseriesResponse>(res);
}

export async function getQualityDimensions(datasetName: string): Promise<any> {
  const res = await fetch(`${API_BASE}/quality-dimensions/${encodeURIComponent(datasetName)}`, { cache: 'no-store' });
  return parseResponse<any>(res);
}

export async function getGlobalStats(): Promise<GlobalStats> {
  const res = await fetchWithTimeout(`${API_BASE}/stats/global`, { cache: 'no-store' });
  return parseResponse<GlobalStats>(res);
}

export async function getSystemHealth(): Promise<SystemHealthRow[]> {
  const res = await fetchWithTimeout(`${API_BASE}/health/system`, { cache: 'no-store' });
  return parseResponse<SystemHealthRow[]>(res);
}

export async function runBacktesting(
  datasetName: string,
  metric = 'row_count',
  limit = 500,
): Promise<BacktestingResult> {
  const res = await fetch(
    `${API_BASE}/backtesting/${encodeURIComponent(datasetName)}?metric=${encodeURIComponent(metric)}&limit=${limit}`,
    { cache: 'no-store' },
  );
  return parseResponse<BacktestingResult>(res);
}

export async function getDatasetPreview(datasetName: string, limit = 100): Promise<DatasetPreview> {
  const res = await fetch(`${API_BASE}/datasets/${encodeURIComponent(datasetName)}/data?limit=${limit}`, {
    cache: 'no-store',
  });
  return parseResponse<DatasetPreview>(res);
}

export async function getDatasetProfile(datasetName: string): Promise<ProfileResponse> {
  const res = await fetch(`${API_BASE}/profile/${encodeURIComponent(datasetName)}`, { cache: 'no-store' });
  return parseResponse<ProfileResponse>(res);
}

export async function getRunVerdict(runId: string): Promise<RunVerdictResponse> {
  const res = await fetch(`${API_BASE}/verdict/${encodeURIComponent(runId)}`, { cache: 'no-store' });
  return parseResponse<RunVerdictResponse>(res);
}

export async function getDiagnosticsRecords(
  datasetName: string,
  params?: { run_id?: string; check_type?: string; limit?: number },
): Promise<DiagnosticsResponse> {
  const query = new URLSearchParams();
  if (params?.run_id) query.set('run_id', params.run_id);
  if (params?.check_type) query.set('check_type', params.check_type);
  if (params?.limit) query.set('limit', String(params.limit));
  const suffix = query.toString() ? `?${query.toString()}` : '';
  const res = await fetch(`${API_BASE}/diagnostics/${encodeURIComponent(datasetName)}${suffix}`, { cache: 'no-store' });
  return parseResponse<DiagnosticsResponse>(res);
}

export async function getRemediationPlan(datasetName: string): Promise<RemediationPlanResponse> {
  const res = await fetch(`${API_BASE}/remediation/${encodeURIComponent(datasetName)}`, { cache: 'no-store' });
  return parseResponse<RemediationPlanResponse>(res);
}

export async function applyRemediation(request: {
  dataset_name: string;
  proposed_yaml: string;
  error_context: string;
  policy_approved?: boolean;
  policy_reason?: string;
}): Promise<any> {
  const res = await fetch(`${API_BASE}/remediation/apply`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(request),
  });
  return parseResponse(res);
}

export async function getGovernanceHistory(datasetName: string): Promise<GovernanceHistoryRow[]> {
  const res = await fetch(`${API_BASE}/governance/${encodeURIComponent(datasetName)}/history`, { cache: 'no-store' });
  return parseResponse<GovernanceHistoryRow[]>(res);
}

export async function getGovernanceFile(filename: string): Promise<GovernanceFileResponse> {
  const res = await fetch(`${API_BASE}/governance/file/${encodeURIComponent(filename)}`, { cache: 'no-store' });
  return parseResponse<GovernanceFileResponse>(res);
}

export async function rollbackGovernance(request: { dataset_name: string; filename: string }): Promise<any> {
  const res = await fetch(`${API_BASE}/governance/rollback`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(request),
  });
  return parseResponse(res);
}

export async function proposeContract(datasetName: string, filePath?: string | null): Promise<any> {
  const res = await fetch(`${API_BASE}/contracts/propose`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ dataset_name: datasetName, file_path: filePath ?? null }),
  });
  return parseResponse(res);
}

export async function getContractContent(datasetName: string): Promise<ContractContentResponse> {
  const res = await fetch(`${API_BASE}/contracts/${encodeURIComponent(datasetName)}`, { cache: 'no-store' });
  return parseResponse<ContractContentResponse>(res);
}

export async function getContractYaml(datasetName: string): Promise<ContractYamlResponse> {
  const res = await fetch(`${API_BASE}/contract/${encodeURIComponent(datasetName)}`, { cache: 'no-store' });
  return parseResponse<ContractYamlResponse>(res);
}

export async function saveContract(request: { dataset_name: string; yaml_content: string; summary?: string }): Promise<any> {
  const res = await fetch(`${API_BASE}/contracts/save`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      dataset_name: request.dataset_name,
      yaml_content: request.yaml_content,
      summary: request.summary ?? 'Manual definition update',
    }),
  });
  return parseResponse(res);
}

export async function listContractVersions(datasetName: string): Promise<ContractVersionRow[]> {
  const res = await fetch(`${API_BASE}/contract-history/${encodeURIComponent(datasetName)}`, { cache: 'no-store' });
  return parseResponse<ContractVersionRow[]>(res);
}

export async function getContractVersion(datasetName: string, versionId: string): Promise<ContractYamlResponse> {
  const res = await fetch(`${API_BASE}/contract/${encodeURIComponent(datasetName)}/version/${encodeURIComponent(versionId)}`, {
    cache: 'no-store',
  });
  return parseResponse<ContractYamlResponse>(res);
}

export async function saveContractVersion(
  datasetName: string,
  request: { yaml_content: string; change_type?: string; changed_by?: string },
): Promise<any> {
  const res = await fetch(`${API_BASE}/contract/${encodeURIComponent(datasetName)}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(request),
  });
  return parseResponse(res);
}

export async function aiModifyContract(
  datasetName: string,
  request: { instruction: string; current_yaml: string },
): Promise<{ modified_yaml: string; explanation?: string }> {
  const res = await fetch(`${API_BASE}/contract/${encodeURIComponent(datasetName)}/ai-modify`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(request),
  });
  return parseResponse<{ modified_yaml: string; explanation?: string }>(res);
}
