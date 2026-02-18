'use client';

import { useEffect, useMemo, useState } from 'react';
import { Loader2, X } from 'lucide-react';

import {
  getDatasetPreview,
  getDiagnosticsRecords,
  getHistory,
  type DatasetPreview,
  type DatasetRunHistoryItem,
  type DiagnosticsRecord,
} from '@/lib/dre-api';

type DimensionRow = {
  name: string;
  score: number;
  status?: string;
  weight?: number;
  check_count?: { passed?: number; total?: number };
  violations?: string[];
};

type Props = {
  open: boolean;
  datasetName: string;
  dimension: DimensionRow | null;
  onClose: () => void;
};

type LoadState =
  | { status: 'idle' }
  | { status: 'loading' }
  | {
      status: 'ready';
      run: DatasetRunHistoryItem | null;
      allDiagnostics: DiagnosticsRecord[];
      diagnostics: DiagnosticsRecord[];
      sampleRows: Array<Record<string, any>>;
      sampleColumns: string[];
    }
  | { status: 'error'; message: string };

const DIMENSION_CHECK_PATTERNS: Record<string, string[]> = {
  validity: ['PATTERN', 'ALLOWED', 'TYPE_MISMATCH', 'SCHEMA_TYPE_MISMATCH', 'SCHEMA_MISSING_COLUMN', 'INVALID'],
  completeness: ['MISSING', 'NULL', 'ROW_COUNT'],
  uniqueness: ['DUPLICATE', 'UNIQUE', 'PRIMARY_KEY'],
  accuracy: ['RANGE', 'ACCURACY', 'CUSTOM_CHECK'],
  timeliness: ['FRESHNESS', 'TIMELINESS', 'ANOMALY_FRESHNESS'],
  consistency: ['CUSTOM_CHECK', 'CONSISTENCY', 'ANOMALY_'],
};

function normalize(value?: string): string {
  return String(value || '').trim().toLowerCase();
}

function formatValue(value: unknown): string {
  if (value === null || value === undefined) return '';
  if (typeof value === 'object') {
    try {
      return JSON.stringify(value);
    } catch {
      return String(value);
    }
  }
  return String(value);
}

function includeByDimension(dimensionName: string, row: DiagnosticsRecord): boolean {
  const key = normalize(dimensionName);
  const patterns = DIMENSION_CHECK_PATTERNS[key];
  if (!patterns || patterns.length === 0) return true;

  const checkType = String(row.check_type || '').toUpperCase();
  if (patterns.some((pattern) => checkType.includes(pattern))) return true;

  const meta = row.metadata && typeof row.metadata === 'object' ? row.metadata : {};
  const metaText = JSON.stringify(meta).toUpperCase();
  return patterns.some((pattern) => metaText.includes(pattern));
}

export default function QualityDimensionEvidenceModal({ open, datasetName, dimension, onClose }: Props) {
  const [state, setState] = useState<LoadState>({ status: 'idle' });

  useEffect(() => {
    if (!open || !datasetName || !dimension) {
      setState({ status: 'idle' });
      return;
    }

    let cancelled = false;
    setState({ status: 'loading' });

    void (async () => {
      try {
        const history = await getHistory(datasetName, 1);
        const latestRun = Array.isArray(history) && history.length > 0 ? history[0] : null;
        const runId = latestRun?.run_id || undefined;

        const [diagnosticsResult, previewResult] = await Promise.allSettled([
          getDiagnosticsRecords(datasetName, { run_id: runId, limit: 250 }),
          getDatasetPreview(datasetName, 60),
        ]);

        if (cancelled) return;

        const allDiagnostics =
          diagnosticsResult.status === 'fulfilled' && Array.isArray(diagnosticsResult.value.records)
            ? diagnosticsResult.value.records
            : [];
        const filteredDiagnostics = allDiagnostics.filter((row) => includeByDimension(dimension.name, row));

        const evidenceRows = filteredDiagnostics.flatMap((row) =>
          Array.isArray(row.sample_records) ? row.sample_records : [],
        );

        const preview =
          previewResult.status === 'fulfilled'
            ? previewResult.value
            : ({ columns: [], data: [] } as DatasetPreview);
        const previewRows = Array.isArray(preview.data) ? preview.data : [];

        const sampleRows = evidenceRows.length > 0 ? evidenceRows.slice(0, 20) : previewRows.slice(0, 20);
        const sampleColumns =
          sampleRows.length > 0
            ? Object.keys(sampleRows[0] || {})
            : Array.isArray(preview.columns)
              ? preview.columns
              : [];

        setState({
          status: 'ready',
          run: latestRun,
          allDiagnostics,
          diagnostics: filteredDiagnostics,
          sampleRows,
          sampleColumns: sampleColumns.slice(0, 16),
        });
      } catch (error) {
        if (cancelled) return;
        setState({
          status: 'error',
          message: error instanceof Error ? error.message : 'Failed to load evidence',
        });
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [open, datasetName, dimension]);

  const totals = useMemo(() => {
    if (state.status !== 'ready') {
      return { matchedChecks: 0, matchedViolations: 0, allChecks: 0, allViolations: 0, expectedChecks: Number(dimension?.check_count?.total || 0) };
    }
    return {
      matchedChecks: state.diagnostics.length,
      matchedViolations: state.diagnostics.reduce((sum, row) => sum + Number(row.violation_count || 0), 0),
      allChecks: state.allDiagnostics.length,
      allViolations: state.allDiagnostics.reduce((sum, row) => sum + Number(row.violation_count || 0), 0),
      expectedChecks: Number(dimension?.check_count?.total || 0),
    };
  }, [state, dimension]);

  if (!open || !dimension) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-background/80 backdrop-blur-sm p-4">
      <div className="w-full max-w-6xl max-h-[90vh] overflow-hidden rounded-2xl border border-border bg-card shadow-2xl">
        <div className="px-4 py-3 border-b border-border flex items-center justify-between">
          <div>
            <div className="text-sm font-medium">Dimension Evidence</div>
            <div className="text-xs text-muted-foreground">
              {datasetName} · {dimension.name}
            </div>
          </div>
          <button onClick={onClose} className="rounded-lg p-2 hover:bg-accent text-muted-foreground" aria-label="Close">
            <X size={16} />
          </button>
        </div>

        <div className="p-4 space-y-4 overflow-y-auto max-h-[calc(90vh-60px)]">
          <section className="grid gap-3 md:grid-cols-4">
            <article className="rounded-lg border border-border bg-background p-3">
              <div className="text-xs uppercase tracking-wide text-muted-foreground">Status</div>
              <div className="mt-1 text-lg font-semibold">{dimension.status || 'UNKNOWN'}</div>
            </article>
            <article className="rounded-lg border border-border bg-background p-3">
              <div className="text-xs uppercase tracking-wide text-muted-foreground">Score</div>
              <div className="mt-1 text-lg font-semibold">{dimension.score.toFixed(1)}%</div>
            </article>
            <article className="rounded-lg border border-border bg-background p-3">
              <div className="text-xs uppercase tracking-wide text-muted-foreground">Expected Checks (6D)</div>
              <div className="mt-1 text-lg font-semibold">{totals.expectedChecks}</div>
            </article>
            <article className="rounded-lg border border-border bg-background p-3">
              <div className="text-xs uppercase tracking-wide text-muted-foreground">Matched Diagnostics</div>
              <div className="mt-1 text-lg font-semibold">{totals.matchedChecks}</div>
            </article>
            <article className="rounded-lg border border-border bg-background p-3">
              <div className="text-xs uppercase tracking-wide text-muted-foreground">Matched Violations</div>
              <div className="mt-1 text-lg font-semibold">{totals.matchedViolations}</div>
            </article>
            <article className="rounded-lg border border-border bg-background p-3">
              <div className="text-xs uppercase tracking-wide text-muted-foreground">All Run Diagnostics</div>
              <div className="mt-1 text-lg font-semibold">
                {totals.allChecks} ({totals.allViolations} violations)
              </div>
            </article>
          </section>

          {state.status === 'loading' && (
            <div className="rounded-lg border border-border bg-background p-4 text-sm text-muted-foreground flex items-center gap-2">
              <Loader2 size={16} className="animate-spin" /> Loading dimension evidence...
            </div>
          )}

          {state.status === 'error' && (
            <div className="rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">{state.message}</div>
          )}

          {state.status === 'ready' && (
            <>
              <section className="rounded-lg border border-border bg-background overflow-hidden">
                <div className="px-3 py-2 border-b border-border text-sm font-medium">Matched Diagnostics Records</div>
                <div className="p-3">
                  {state.diagnostics.length === 0 ? (
                    <div className="text-sm text-muted-foreground">
                      No diagnostics records matched this dimension mapping for the latest run.
                    </div>
                  ) : (
                    <div className="space-y-2 max-h-56 overflow-auto">
                      {state.diagnostics.slice(0, 40).map((row) => (
                        <div key={row.id} className="rounded-md border border-border bg-card px-3 py-2 text-xs">
                          <div className="font-medium">
                            {row.check_type} · violations {Number(row.violation_count || 0)}
                            {row.column_name ? ` · column ${row.column_name}` : ''}
                          </div>
                          <div className="text-muted-foreground">
                            run {row.run_id || 'n/a'} · {row.severity || 'unknown'}
                          </div>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              </section>

              <section className="rounded-lg border border-border bg-background overflow-hidden">
                <div className="px-3 py-2 border-b border-border text-sm font-medium">All Diagnostics Records (Latest Run)</div>
                <div className="p-3">
                  {state.allDiagnostics.length === 0 ? (
                    <div className="text-sm text-muted-foreground">No diagnostics records were persisted for this run.</div>
                  ) : (
                    <div className="space-y-2 max-h-56 overflow-auto">
                      {state.allDiagnostics.slice(0, 40).map((row) => (
                        <div key={`all-${row.id}`} className="rounded-md border border-border bg-card px-3 py-2 text-xs">
                          <div className="font-medium">
                            {row.check_type} · violations {Number(row.violation_count || 0)}
                            {row.column_name ? ` · column ${row.column_name}` : ''}
                          </div>
                          <div className="text-muted-foreground">
                            run {row.run_id || 'n/a'} · {row.severity || 'unknown'}
                          </div>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              </section>

              <section className="rounded-lg border border-border bg-background overflow-hidden">
                <div className="px-3 py-2 border-b border-border text-sm font-medium">Row Preview</div>
                <div className="p-3">
                  {state.sampleRows.length === 0 ? (
                    <div className="text-sm text-muted-foreground">No row-level sample available for this dimension.</div>
                  ) : (
                    <div className="overflow-auto border border-border rounded-md max-h-80">
                      <table className="w-full text-xs">
                        <thead className="bg-muted/30 text-muted-foreground">
                          <tr>
                            {state.sampleColumns.map((column) => (
                              <th key={column} className="px-2 py-2 text-left whitespace-nowrap">
                                {column}
                              </th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          {state.sampleRows.slice(0, 20).map((row, idx) => (
                            <tr key={`row-${idx}`} className="border-t border-border">
                              {state.sampleColumns.map((column) => (
                                <td key={`${idx}-${column}`} className="px-2 py-2 align-top">
                                  <span className="line-clamp-2">{formatValue(row[column])}</span>
                                </td>
                              ))}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  )}
                </div>
              </section>

              {Array.isArray(dimension.violations) && dimension.violations.length > 0 && (
                <section className="rounded-lg border border-border bg-background p-3">
                  <div className="text-sm font-medium mb-2">Dimension Violation Notes</div>
                  <div className="space-y-1">
                    {dimension.violations.slice(0, 10).map((item, idx) => (
                      <div key={`v-${idx}`} className="text-xs text-muted-foreground">
                        {item}
                      </div>
                    ))}
                  </div>
                </section>
              )}
            </>
          )}
        </div>
      </div>
    </div>
  );
}
