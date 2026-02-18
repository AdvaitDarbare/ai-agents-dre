import type { UIMessage } from 'ai';

export type PendingContractsTool = {
  input: { limit?: number };
  output: {
    count: number;
    generated_at: string;
    datasets: Array<{
      dataset_name: string;
      proposed_at?: string;
      row_count?: number | null;
      column_count?: number | null;
      pending_files: number;
    }>;
  };
};

export type PulseSnapshotTool = {
  input: { statusFilter?: 'all' | 'attention' };
  output: {
    total: number;
    healthy: number;
    warning: number;
    blocked: number;
    generated_at: string;
    rows: Array<{
      name: string;
      status: string;
      quality_score?: number;
      lifecycle?: string;
      reason?: string;
    }>;
  };
};

export type SloSummaryTool = {
  input: { dataset_name: string; window?: number };
  output: {
    dataset_name: string;
    window: number;
    generated_at: string;
    summary: Array<{
      slo_name: string;
      attainment: number;
      passed_runs: number;
      total_runs: number;
      latest_status: string;
      budget_burn: number;
      latest_timestamp?: string | null;
    }>;
  };
};

export type FailureEvidenceTool = {
  input: { dataset_name: string; run_id?: string; dimension?: string; limit?: number };
  output: {
    dataset_name: string;
    run_id?: string | null;
    run_status?: string;
    run_reason?: string;
    generated_at: string;
    dimension_filter?: string | null;
    evidence_summary: Array<{
      check_type: string;
      violation_count: number;
      sample_count: number;
      column_name?: string | null;
    }>;
    sample_rows: Array<Record<string, unknown>>;
    sample_columns: string[];
  };
};

export type CopilotTools = {
  showPendingContracts: PendingContractsTool;
  showPulseSnapshot: PulseSnapshotTool;
  showSloSummary: SloSummaryTool;
  showFailureEvidence: FailureEvidenceTool;
};

export type CopilotMessage = UIMessage<unknown, Record<string, never>, CopilotTools>;
