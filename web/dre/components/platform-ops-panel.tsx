'use client';

import { useEffect, useMemo, useState } from 'react';
import { Activity, ShieldCheck, Workflow } from 'lucide-react';

import {
  checkPolicy,
  getAuditSummary,
  getWorkflowTimeline,
  type AuditSummaryRow,
  type PolicyDecision,
  type WorkflowTimelineEvent,
} from '@/lib/dre-api';

type Props = {
  datasetName?: string | null;
};

export default function PlatformOpsPanel({ datasetName }: Props) {
  const [policy, setPolicy] = useState<PolicyDecision | null>(null);
  const [auditSummary, setAuditSummary] = useState<AuditSummaryRow[]>([]);
  const [timelineEvents, setTimelineEvents] = useState<WorkflowTimelineEvent[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    const run = async () => {
      try {
        setError(null);
        const [decision, summary, timeline] = await Promise.all([
          checkPolicy({ action: 'delete', dataset_name: datasetName || undefined }),
          getAuditSummary({ window_minutes: 24 * 60 }),
          getWorkflowTimeline({ dataset_name: datasetName || undefined, limit: 120 }),
        ]);
        if (cancelled) return;
        setPolicy(decision);
        setAuditSummary(summary);
        setTimelineEvents(Array.isArray(timeline.events) ? timeline.events : []);
      } catch (e) {
        if (cancelled) return;
        setError(e instanceof Error ? e.message : 'Failed to load platform ops data');
      }
    };
    void run();
    return () => {
      cancelled = true;
    };
  }, [datasetName]);

  const topAudit = useMemo(() => auditSummary.slice(0, 6), [auditSummary]);
  const recentEvents = useMemo(() => timelineEvents.slice(0, 20), [timelineEvents]);

  return (
    <section className="space-y-4">
      <div className="rounded-xl border border-border bg-card overflow-hidden">
        <div className="px-4 py-3 border-b border-border text-sm font-medium flex items-center gap-2">
          <Workflow size={14} />
          Reliability Runtime Visibility
        </div>
        <div className="p-4 grid gap-3 md:grid-cols-2 xl:grid-cols-3 text-sm">
          <div className="rounded-lg border border-border bg-background p-3">
            <div className="text-xs uppercase tracking-wide text-muted-foreground">LangGraph Workflow</div>
            <div className="mt-1 font-medium">Enabled</div>
            <div className="mt-1 text-xs text-muted-foreground">Evaluate to persist verdict to file actions, plus HITL resume paths.</div>
          </div>
          <div className="rounded-lg border border-border bg-background p-3">
            <div className="text-xs uppercase tracking-wide text-muted-foreground">HITL Contracts</div>
            <div className="mt-1 font-medium">Enabled</div>
            <div className="mt-1 text-xs text-muted-foreground">Pending approval queue and proposal/approve/reject loop are active.</div>
          </div>
          <div className="rounded-lg border border-border bg-background p-3">
            <div className="text-xs uppercase tracking-wide text-muted-foreground">Ops Controls</div>
            <div className="mt-1 font-medium">Async Jobs + Incidents + RBAC</div>
            <div className="mt-1 text-xs text-muted-foreground">Scan/delete/remediation queueing with incident lifecycle controls.</div>
          </div>
        </div>
      </div>

      <div className="grid gap-4 xl:grid-cols-2">
        <div className="rounded-xl border border-border bg-card overflow-hidden">
          <div className="px-4 py-3 border-b border-border text-sm font-medium flex items-center gap-2">
            <ShieldCheck size={14} />
            Policy Gate Check
          </div>
          <div className="p-4 text-sm">
            {error && <div className="text-rose-700">{error}</div>}
            {!error && !policy && <div className="text-muted-foreground">Loading policy decision...</div>}
            {!error && policy && (
              <div className="space-y-2">
                <div>
                  <span className="text-xs uppercase tracking-wide text-muted-foreground">Action</span>
                  <div className="font-medium">{String(policy.action || 'delete')}</div>
                </div>
                <div>
                  <span className="text-xs uppercase tracking-wide text-muted-foreground">Decision</span>
                  <div className="font-medium">{String(policy.decision || (policy.allowed ? 'allowed' : 'blocked'))}</div>
                </div>
                <div>
                  <span className="text-xs uppercase tracking-wide text-muted-foreground">Approval Required</span>
                  <div className="font-medium">{policy.requires_approval ? 'Yes' : 'No'}</div>
                </div>
                {policy.reason && <div className="text-xs text-muted-foreground">{policy.reason}</div>}
              </div>
            )}
          </div>
        </div>

        <div className="rounded-xl border border-border bg-card overflow-hidden">
          <div className="px-4 py-3 border-b border-border text-sm font-medium flex items-center gap-2">
            <Activity size={14} />
            Audit Activity (24h)
          </div>
          <div className="p-4">
            {topAudit.length === 0 ? (
              <div className="text-sm text-muted-foreground">No audit events yet.</div>
            ) : (
              <div className="space-y-2">
                {topAudit.map((row, idx) => (
                  <div key={`${row.action}-${row.status || 'none'}-${idx}`} className="flex items-center justify-between rounded-lg border border-border bg-background px-3 py-2 text-sm">
                    <div className="min-w-0">
                      <div className="font-medium truncate">{row.action}</div>
                      <div className="text-xs text-muted-foreground">{row.status || 'N/A'}</div>
                    </div>
                    <div className="text-xs font-semibold rounded-full bg-muted px-2 py-1">{row.count}</div>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
      </div>

      <div className="rounded-xl border border-border bg-card overflow-hidden">
        <div className="px-4 py-3 border-b border-border text-sm font-medium flex items-center gap-2">
          <Activity size={14} />
          Unified Workflow Timeline
        </div>
        <div className="p-4">
          {recentEvents.length === 0 ? (
            <div className="text-sm text-muted-foreground">No workflow events yet.</div>
          ) : (
            <div className="max-h-80 overflow-y-auto space-y-2">
              {recentEvents.map((event) => (
                <div
                  key={event.event_id}
                  className="rounded-lg border border-border bg-background px-3 py-2 text-sm flex items-start justify-between gap-3"
                >
                  <div className="min-w-0">
                    <div className="font-medium truncate">{event.message || event.event}</div>
                    <div className="text-xs text-muted-foreground">
                      {(event.channel || 'event').toUpperCase()} · {event.dataset_name || 'global'}
                    </div>
                  </div>
                  <div className="text-right shrink-0">
                    <div className="text-xs font-semibold">{event.status || 'N/A'}</div>
                    <div className="text-xs text-muted-foreground">
                      {event.timestamp ? new Date(event.timestamp).toLocaleTimeString() : 'n/a'}
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    </section>
  );
}
