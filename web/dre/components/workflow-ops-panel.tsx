'use client';

import { useEffect, useState } from 'react';
import { RefreshCw, Route, Sparkles } from 'lucide-react';

import {
  getWorkflowTimeline,
  openWorkflowTimelineStream,
  runAgenticWorkflow,
  type WorkflowTimelineEvent,
} from '@/lib/dre-api';

type Props = {
  datasetName?: string | null;
};

export default function WorkflowOpsPanel({ datasetName }: Props) {
  const [datasetInput, setDatasetInput] = useState<string>(datasetName || '');
  const [metric, setMetric] = useState<string>('');
  const [confidenceThreshold, setConfidenceThreshold] = useState<number>(0.8);
  const [autoExecute, setAutoExecute] = useState<boolean>(false);
  const [policyApproved, setPolicyApproved] = useState<boolean>(false);
  const [policyReason, setPolicyReason] = useState<string>('');
  const [limit, setLimit] = useState<number>(120);

  const [timelineEvents, setTimelineEvents] = useState<WorkflowTimelineEvent[]>([]);
  const [timelineSummary, setTimelineSummary] = useState<Record<string, any>>({});
  const [runResult, setRunResult] = useState<Record<string, any> | null>(null);
  const [liveStreaming, setLiveStreaming] = useState<boolean>(true);

  const [loading, setLoading] = useState<boolean>(false);
  const [running, setRunning] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!datasetName) return;
    setDatasetInput((prev) => prev || datasetName);
  }, [datasetName]);

  const refresh = async () => {
    try {
      setLoading(true);
      setError(null);
      const timeline = await getWorkflowTimeline({
        dataset_name: datasetInput.trim() ? datasetInput.trim() : undefined,
        limit,
      });
      setTimelineEvents(Array.isArray(timeline.events) ? timeline.events : []);
      setTimelineSummary(timeline.summary || {});
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load workflow runtime data');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void refresh();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [datasetInput, limit]);

  useEffect(() => {
    if (!liveStreaming) return;
    const source = openWorkflowTimelineStream({
      dataset_name: datasetInput.trim() ? datasetInput.trim() : undefined,
      limit,
      interval_ms: 2500,
    });

    const onTimeline = (event: MessageEvent) => {
      try {
        const payload = JSON.parse(String(event.data || '{}')) as Record<string, any>;
        setTimelineEvents(Array.isArray(payload.events) ? payload.events : []);
        setTimelineSummary((payload.summary as Record<string, any>) || {});
      } catch {
        return;
      }
    };

    const onError = () => {
      // EventSource auto-reconnects; preserve existing data on transient errors.
      return;
    };

    source.addEventListener('timeline', onTimeline as EventListener);
    source.addEventListener('error', onError as EventListener);

    return () => {
      source.removeEventListener('timeline', onTimeline as EventListener);
      source.removeEventListener('error', onError as EventListener);
      source.close();
    };
  }, [datasetInput, limit, liveStreaming]);

  const onRunAgentic = async () => {
    const dataset = datasetInput.trim();
    if (!dataset) {
      setError('Dataset name is required to run the agentic loop.');
      return;
    }

    try {
      setRunning(true);
      setError(null);
      const result = await runAgenticWorkflow({
        dataset_name: dataset,
        metric: metric.trim() || undefined,
        auto_execute: autoExecute,
        confidence_threshold: confidenceThreshold,
        policy_approved: policyApproved,
        policy_reason: policyReason.trim() || undefined,
      });
      setRunResult(result || null);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to run agentic workflow');
    } finally {
      setRunning(false);
    }
  };

  return (
    <section className="space-y-5">
      <div className="rounded-xl border border-border bg-card overflow-hidden">
        <div className="px-4 py-3 border-b border-border text-sm font-medium flex items-center gap-2">
          <Route size={14} />
          Run Controls
        </div>
        <div className="p-4 grid gap-3 md:grid-cols-2 xl:grid-cols-3 text-sm">
          <div>
            <label className="text-xs text-muted-foreground uppercase tracking-wide">Dataset</label>
            <input
              value={datasetInput}
              onChange={(e) => setDatasetInput(e.target.value)}
              placeholder="orders"
              className="mt-1 w-full rounded-md border border-border bg-background px-3 py-2"
            />
          </div>
          <div>
            <label className="text-xs text-muted-foreground uppercase tracking-wide">Primary Action</label>
            <div className="mt-1 text-sm text-muted-foreground">Run investigation and remediation logic for this dataset.</div>
          </div>
          <div className="flex items-end">
            <button
              onClick={() => void refresh()}
              disabled={loading}
              className="inline-flex items-center gap-2 rounded-lg border border-border px-3 py-2"
            >
              <RefreshCw size={14} className={loading ? 'animate-spin' : ''} />
              Refresh
            </button>
          </div>
        </div>

        <div className="px-4 pb-4 flex flex-wrap items-center gap-3 text-sm">
          <button
            onClick={() => void onRunAgentic()}
            disabled={running}
            className="inline-flex items-center gap-2 rounded-lg bg-foreground text-background px-3 py-2"
          >
            <Sparkles size={14} />
            {running ? 'Running...' : 'Run Investigation'}
          </button>
          <label className="inline-flex items-center gap-2">
            <input type="checkbox" checked={liveStreaming} onChange={(e) => setLiveStreaming(e.target.checked)} />
            Live updates
          </label>
        </div>

        <details className="mx-4 mb-4 rounded-lg border border-border bg-background">
          <summary className="cursor-pointer px-3 py-2 text-sm font-medium">Advanced Settings</summary>
          <div className="border-t border-border p-3 grid gap-3 md:grid-cols-2 xl:grid-cols-4 text-sm">
            <div>
              <label className="text-xs text-muted-foreground uppercase tracking-wide">Metric (optional)</label>
              <input
                value={metric}
                onChange={(e) => setMetric(e.target.value)}
                placeholder="row_count"
                className="mt-1 w-full rounded-md border border-border bg-card px-3 py-2"
              />
            </div>
            <div>
              <label className="text-xs text-muted-foreground uppercase tracking-wide">Confidence Threshold</label>
              <input
                value={confidenceThreshold}
                min={0}
                max={1}
                step={0.05}
                type="number"
                onChange={(e) => setConfidenceThreshold(Number(e.target.value || 0.8))}
                className="mt-1 w-full rounded-md border border-border bg-card px-3 py-2"
              />
            </div>
            <div>
              <label className="text-xs text-muted-foreground uppercase tracking-wide">Timeline Limit</label>
              <input
                value={limit}
                min={20}
                max={500}
                step={10}
                type="number"
                onChange={(e) => setLimit(Number(e.target.value || 120))}
                className="mt-1 w-full rounded-md border border-border bg-card px-3 py-2"
              />
            </div>
            <div className="space-y-2">
              <label className="inline-flex items-center gap-2">
                <input type="checkbox" checked={autoExecute} onChange={(e) => setAutoExecute(e.target.checked)} />
                Auto execute
              </label>
              <label className="inline-flex items-center gap-2">
                <input type="checkbox" checked={policyApproved} onChange={(e) => setPolicyApproved(e.target.checked)} />
                Policy approved
              </label>
            </div>
            <div className="md:col-span-2 xl:col-span-4">
              <label className="text-xs text-muted-foreground uppercase tracking-wide">Policy Reason</label>
              <input
                value={policyReason}
                onChange={(e) => setPolicyReason(e.target.value)}
                placeholder="Required only when policy approval is enforced"
                className="mt-1 w-full rounded-md border border-border bg-card px-3 py-2"
              />
            </div>
          </div>
        </details>

        {error && <div className="px-4 pb-4 text-sm text-rose-700">{error}</div>}
      </div>

      <div className="grid gap-4 xl:grid-cols-2">
        <div className="rounded-xl border border-border bg-card p-4">
          <div className="text-xs uppercase tracking-wide text-muted-foreground">Last Agentic Run</div>
          {!runResult ? (
            <div className="mt-2 text-sm text-muted-foreground">No run executed from UI yet.</div>
          ) : (
            <div className="mt-2 text-sm">
              <div>Decision: {String(runResult.execution?.decision || 'n/a')}</div>
              <div>Reason: {String(runResult.execution?.reason || 'n/a')}</div>
            </div>
          )}
        </div>
        <div className="rounded-xl border border-border bg-card p-4">
          <div className="text-xs uppercase tracking-wide text-muted-foreground">Workflow Timeline</div>
          <div className="mt-2 text-sm text-muted-foreground">Timeline events: {timelineSummary.total_events || timelineEvents.length || 0}</div>
        </div>
      </div>
    </section>
  );
}
