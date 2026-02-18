'use client';

import { useEffect, useMemo, useState } from 'react';
import { Activity, FileText, ShieldCheck, Share2 } from 'lucide-react';

import {
  getAgenticRemediationRun,
  getBaselines,
  getDatasetMetrics,
  getHistory,
  getLineage,
  openAgenticRemediationStream,
  runAgenticRemediation,
  runBacktesting,
  type AgenticRemediationRun,
  type BacktestingResult,
  type BaselineRow,
  type DatasetRunHistoryItem,
  type LineageGraph,
  type MetricSnapshot,
} from '@/lib/dre-api';
import { useSloData } from '@/dre/data/use-slo-data';
import GovernanceHistoryPanel from '@/dre/components/governance-history-panel';
import LineagePanel from '@/dre/components/lineage-panel';
import SloPanel from '@/dre/components/slo-panel';
import QualityRadarChart from '@/dre/components/charts/quality-radar-chart';
import SchemaValidationTable from '@/dre/components/charts/schema-validation-table';
import ColumnQualityBars from '@/dre/components/charts/column-quality-bars';
import QualityScoreTrend from '@/dre/components/charts/quality-score-trend';
import NullRateHeatmap from '@/dre/components/charts/null-rate-heatmap';
import ConstraintViolations from '@/dre/components/charts/constraint-violations';
import VolumeAnomalyChart from '@/dre/components/charts/volume-anomaly-chart';
import DriftChart from '@/dre/components/charts/drift-chart';

type DetailTab = 'quality' | 'anomaly' | 'slos' | 'governance' | 'lineage';

type Props = {
  datasetName: string;
  refreshToken?: number;
  onPreview: () => void;
  onProfile: () => void;
  onForceLoad: () => void;
  onAfterChange: () => void;
};

function formatTs(iso?: string | null): string {
  if (!iso) return 'n/a';
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return 'n/a';
  return date.toLocaleString();
}

export default function ExpandedDatasetDetail({ datasetName, refreshToken = 0, onPreview, onProfile, onForceLoad, onAfterChange }: Props) {
  const [tab, setTab] = useState<DetailTab>('quality');
  const [history, setHistory] = useState<DatasetRunHistoryItem[]>([]);
  const [metrics, setMetrics] = useState<MetricSnapshot | null>(null);
  const [baselines, setBaselines] = useState<BaselineRow[]>([]);
  const [lineage, setLineage] = useState<LineageGraph | null>(null);
  const [loading, setLoading] = useState(true);
  const [backtesting, setBacktesting] = useState<BacktestingResult | null>(null);
  const [backtestingLoading, setBacktestingLoading] = useState(false);
  const [remediationRun, setRemediationRun] = useState<AgenticRemediationRun | null>(null);
  const [remediationRunId, setRemediationRunId] = useState<string | null>(null);
  const [remediationLoading, setRemediationLoading] = useState(false);
  const [remediationError, setRemediationError] = useState<string | null>(null);

  const { summary: sloSummary, checks: sloChecks } = useSloData(datasetName, { window: 200, limit: 100 });

  const refresh = async () => {
    setLoading(true);
    try {
      const [h, m, b, l] = await Promise.all([
        getHistory(datasetName, 20),
        getDatasetMetrics(datasetName),
        getBaselines(datasetName),
        getLineage(datasetName).catch(() => null),
      ]);
      setHistory(Array.isArray(h) ? h : []);
      setMetrics(m || null);
      setBaselines(Array.isArray(b) ? b : []);
      setLineage(l);
    } catch (error) {
      console.error('Failed to load expanded dataset detail', error);
      setHistory([]);
      setMetrics(null);
      setBaselines([]);
      setLineage(null);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void refresh();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [datasetName, refreshToken]);

  useEffect(() => {
    setRemediationRun(null);
    setRemediationRunId(null);
    setRemediationError(null);
  }, [datasetName]);

  useEffect(() => {
    if (!remediationRunId) return;
    const source = openAgenticRemediationStream(remediationRunId, 1500);

    const onUpdate = (event: MessageEvent) => {
      try {
        const payload = JSON.parse(String(event.data || '{}')) as AgenticRemediationRun;
        setRemediationRun(payload);
        const status = String(payload.status || '').toUpperCase();
        if (status === 'AUTO_FIXED' || status === 'PLAN_REQUIRED' || status === 'BLOCKED_BY_POLICY' || status === 'FAILED') {
          source.close();
          void refresh();
        }
      } catch {
        return;
      }
    };

    source.addEventListener('remediation', onUpdate as EventListener);
    return () => {
      source.removeEventListener('remediation', onUpdate as EventListener);
      source.close();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [remediationRunId]);

  const baselineCount = baselines.length;
  const latestRun = history[0];
  const latestStatus = String(latestRun?.status || '').toUpperCase();
  const canRunAutoFix = latestStatus === 'BLOCKED' || latestStatus === 'WARNING';
  const remediationTimeline = useMemo(() => {
    if (!remediationRun?.timeline || !Array.isArray(remediationRun.timeline)) return [];
    return remediationRun.timeline;
  }, [remediationRun]);

  const qualityBlocks = useMemo(() => {
    const blocks: Array<{ title: string; value: string }> = [];
    blocks.push({ title: 'Last Scan', value: formatTs(metrics?.run_timestamp || null) });
    blocks.push({
      title: 'Latest Status',
      value: latestRun?.status ? String(latestRun.status) : 'N/A',
    });
    blocks.push({
      title: 'Quality Score',
      value: typeof latestRun?.quality_score === 'number' ? `${latestRun.quality_score.toFixed(1)}%` : 'N/A',
    });
    blocks.push({
      title: 'Baselines',
      value: String(baselineCount),
    });
    return blocks;
  }, [baselineCount, latestRun?.quality_score, latestRun?.status, metrics?.run_timestamp]);

  return (
    <div className="rounded-xl border border-border bg-card overflow-hidden">
      <div className="px-4 py-3 border-b border-border flex flex-wrap items-center justify-between gap-3">
        <div className="min-w-0">
          <div className="text-xs uppercase tracking-wide text-muted-foreground">Dataset Detail</div>
          <div className="mt-1 text-sm font-medium text-foreground truncate">{datasetName}</div>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={onPreview}
            className="rounded-lg border border-border bg-background px-3 py-2 text-sm font-medium hover:bg-accent"
          >
            Preview
          </button>
          <button
            onClick={onProfile}
            className="rounded-lg border border-border bg-background px-3 py-2 text-sm font-medium hover:bg-accent"
          >
            Profile
          </button>
          <button
            onClick={() => void refresh()}
            className="rounded-lg border border-border bg-background px-3 py-2 text-sm font-medium hover:bg-accent"
          >
            Refresh
          </button>
          <button
            onClick={onForceLoad}
            className="rounded-lg bg-foreground text-background px-3 py-2 text-sm font-medium hover:opacity-90"
          >
            Force Load
          </button>
        </div>
      </div>

      <div className="px-4 py-3 border-b border-border bg-muted/10 flex flex-wrap gap-2">
        {(
          [
            { id: 'quality', label: 'Data Quality', icon: ShieldCheck },
            { id: 'anomaly', label: 'Anomalies & Baselines', icon: Activity },
            { id: 'slos', label: 'SLOs', icon: ShieldCheck },
            { id: 'governance', label: 'Governance', icon: FileText },
            { id: 'lineage', label: 'Lineage', icon: Share2 },
          ] as const
        ).map((item) => (
          <button
            key={item.id}
            onClick={() => setTab(item.id)}
            className={`rounded-lg px-3 py-1.5 text-xs font-medium inline-flex items-center gap-2 ${tab === item.id ? 'bg-foreground text-background' : 'border border-border bg-background text-muted-foreground hover:bg-accent hover:text-foreground'
              }`}
          >
            <item.icon size={14} /> {item.label}
          </button>
        ))}
      </div>

      <div className="p-4 space-y-4">
        {loading && <div className="text-sm text-muted-foreground">Loading details...</div>}

        {!loading && tab === 'quality' && (
          <div className="space-y-4">
            <section className="grid gap-3 md:grid-cols-4">
              {qualityBlocks.map((block) => (
                <article key={block.title} className="rounded-lg border border-border bg-background p-3">
                  <div className="text-xs uppercase tracking-wide text-muted-foreground">{block.title}</div>
                  <div className="mt-1 text-xl font-medium">{block.value}</div>
                </article>
              ))}
            </section>

            <QualityRadarChart datasetName={datasetName} />
            <SchemaValidationTable datasetName={datasetName} />
            <div className="grid gap-4 xl:grid-cols-2">
              <ColumnQualityBars datasetName={datasetName} />
              <QualityScoreTrend datasetName={datasetName} />
            </div>
            <NullRateHeatmap datasetName={datasetName} />
          </div>
        )}

        {!loading && tab === 'anomaly' && (
          <div className="space-y-4">
            <ConstraintViolations datasetName={datasetName} />
            <VolumeAnomalyChart datasetName={datasetName} />
            <DriftChart datasetName={datasetName} metricName="mean_amount" />
            <section className="rounded-xl border border-border bg-background overflow-hidden">
              <div className="px-4 py-3 border-b border-border text-sm font-medium flex items-center justify-between gap-3">
                <span>Auto-Remediation Loop (Full Auto)</span>
                <button
                  onClick={async () => {
                    setRemediationLoading(true);
                    setRemediationError(null);
                    try {
                      const result = await runAgenticRemediation({
                        dataset_name: datasetName,
                        max_retries: 2,
                        autonomy_mode: 'full_auto',
                      });
                      if (result.run) {
                        setRemediationRun(result.run);
                      } else {
                        const latest = await getAgenticRemediationRun(result.id);
                        setRemediationRun(latest);
                      }
                      setRemediationRunId(result.id);
                      onAfterChange();
                    } catch (error) {
                      setRemediationError(error instanceof Error ? error.message : 'Auto-remediation failed');
                    } finally {
                      setRemediationLoading(false);
                    }
                  }}
                  disabled={remediationLoading || !canRunAutoFix}
                  className="rounded-lg border border-border bg-background px-3 py-1.5 text-xs font-medium hover:bg-accent disabled:opacity-50"
                >
                  {remediationLoading ? 'Running...' : 'Auto-fix with AI'}
                </button>
              </div>
              <div className="p-4 space-y-3 text-xs">
                <div className="text-muted-foreground">
                  Trigger status required: BLOCKED or WARNING. Current status: <span className="font-medium text-foreground">{latestStatus || 'UNKNOWN'}</span>
                </div>
                {remediationError && <div className="text-red-600">{remediationError}</div>}
                {!remediationRun && (
                  <div className="text-muted-foreground">
                    Runs classify - propose - policy - apply - re-run with a max of 2 retries, then emits a deterministic plan.
                  </div>
                )}
                {remediationRun && (
                  <div className="space-y-3">
                    <div className="grid gap-2 md:grid-cols-5">
                      <div className="rounded border border-border p-2">
                        <div className="text-muted-foreground">Status</div>
                        <div className="font-medium">{remediationRun.status}</div>
                      </div>
                      <div className="rounded border border-border p-2">
                        <div className="text-muted-foreground">Attempts</div>
                        <div className="font-medium">{remediationRun.attempt_count ?? remediationRun.attempts?.length ?? 0}</div>
                      </div>
                      <div className="rounded border border-border p-2">
                        <div className="text-muted-foreground">Policy Blocks</div>
                        <div className="font-medium">{remediationRun.policy_blocks ?? 0}</div>
                      </div>
                      <div className="rounded border border-border p-2">
                        <div className="text-muted-foreground">Initial Run</div>
                        <div className="font-medium truncate">{remediationRun.initial_run_id || 'n/a'}</div>
                      </div>
                      <div className="rounded border border-border p-2">
                        <div className="text-muted-foreground">Final Run</div>
                        <div className="font-medium truncate">{remediationRun.final_run_id || 'n/a'}</div>
                      </div>
                    </div>

                    <div className="rounded border border-border p-2">
                      <div className="font-medium mb-1">Attempt Timeline</div>
                      {remediationTimeline.length === 0 && (
                        <div className="text-muted-foreground">No stage events recorded.</div>
                      )}
                      {remediationTimeline.length > 0 && (
                        <div className="space-y-1 max-h-56 overflow-auto">
                          {remediationTimeline.map((event, idx) => (
                            <div key={`${idx}-${event.step}`} className="flex items-center gap-2">
                              <span className="rounded border border-border px-1.5 py-0.5 text-[10px]">
                                A{event.attempt_no ?? '?'}
                              </span>
                              <span className="rounded border border-border px-1.5 py-0.5 text-[10px] uppercase">
                                {event.step || 'step'}
                              </span>
                              <span className="text-[10px] uppercase text-muted-foreground">{event.status || 'status'}</span>
                              <span className="text-muted-foreground">{event.message || ''}</span>
                            </div>
                          ))}
                        </div>
                      )}
                    </div>

                    {Array.isArray(remediationRun.attempts) && remediationRun.attempts.length > 0 && (
                      <div className="rounded border border-border p-2">
                        <div className="font-medium mb-1">Attempt Summaries</div>
                        <div className="space-y-1 max-h-44 overflow-auto">
                          {remediationRun.attempts.map((attempt) => (
                            <div key={`attempt-${attempt.attempt_no}`} className="text-muted-foreground">
                              <span className="font-medium text-foreground">Attempt {attempt.attempt_no}</span>
                              {` · ${attempt.classification || 'unknown'} · ${attempt.result_status || 'unknown'}`}
                              {attempt.proposed_diff_summary ? ` · ${attempt.proposed_diff_summary}` : ''}
                            </div>
                          ))}
                        </div>
                      </div>
                    )}

                    {Array.isArray(remediationRun.applied_changes) && remediationRun.applied_changes.length > 0 && (
                      <div className="rounded border border-border p-2">
                        <div className="font-medium mb-1">Applied Changes</div>
                        <pre className="text-[11px] whitespace-pre-wrap overflow-auto max-h-44 text-muted-foreground">
                          {JSON.stringify(remediationRun.applied_changes, null, 2)}
                        </pre>
                      </div>
                    )}

                    {remediationRun.plan && (
                      <div className="rounded border border-border p-2">
                        <div className="font-medium mb-1">Plan Required</div>
                        <pre className="text-[11px] whitespace-pre-wrap overflow-auto max-h-44 text-muted-foreground">
                          {JSON.stringify(remediationRun.plan, null, 2)}
                        </pre>
                      </div>
                    )}
                  </div>
                )}
              </div>
            </section>
            <section className="rounded-xl border border-border bg-background overflow-hidden">
              <div className="px-4 py-3 border-b border-border text-sm font-medium flex items-center justify-between gap-3">
                <span>Backtesting Harness (FP/FN Tuning)</span>
                <button
                  onClick={async () => {
                    setBacktestingLoading(true);
                    try {
                      const result = await runBacktesting(datasetName, 'row_count', 500);
                      setBacktesting(result);
                    } catch (error) {
                      console.error('Backtesting failed', error);
                      setBacktesting(null);
                    } finally {
                      setBacktestingLoading(false);
                    }
                  }}
                  disabled={backtestingLoading}
                  className="rounded-lg border border-border bg-background px-3 py-1.5 text-xs font-medium hover:bg-accent disabled:opacity-50"
                >
                  {backtestingLoading ? 'Running...' : 'Run Backtesting'}
                </button>
              </div>
              <pre className="p-4 text-xs whitespace-pre-wrap overflow-auto max-h-64 text-muted-foreground">
                {backtesting ? JSON.stringify(backtesting, null, 2) : 'Run backtesting to inspect precision/recall and FP/FN behavior.'}
              </pre>
            </section>
          </div>
        )}

        {!loading && tab === 'slos' && <SloPanel datasetName={datasetName} summary={sloSummary} checks={sloChecks} />}

        {!loading && tab === 'governance' && (
          <GovernanceHistoryPanel datasetName={datasetName} scanHistory={history} onAfterSave={onAfterChange} />
        )}

        {!loading && tab === 'lineage' && <LineagePanel lineage={lineage} />}
      </div>
    </div>
  );
}
