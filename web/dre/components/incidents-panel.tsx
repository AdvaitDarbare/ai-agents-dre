import { useMemo, useState } from 'react';
import { AlertTriangle, ShieldCheck } from 'lucide-react';

import type { IncidentItem } from '@/lib/dre-api';

type Props = {
  incidents: IncidentItem[];
  onAck: (incidentId: string) => void;
  onResolve: (incidentId: string) => void;
};

function severityClass(severity: string): string {
  if (severity === 'CRITICAL') return 'bg-rose-50 text-rose-700 ring-1 ring-rose-200';
  return 'bg-amber-50 text-amber-700 ring-1 ring-amber-200';
}

function statusClass(status: string): string {
  if (status === 'RESOLVED') return 'bg-emerald-50 text-emerald-700 ring-1 ring-emerald-200';
  if (status === 'ACK') return 'bg-blue-50 text-blue-700 ring-1 ring-blue-200';
  return 'bg-zinc-100 text-zinc-700 ring-1 ring-zinc-200';
}

export default function IncidentsPanel({ incidents, onAck, onResolve }: Props) {
  const [severity, setSeverity] = useState<'ALL' | 'CRITICAL' | 'WARNING'>('ALL');
  const [status, setStatus] = useState<'ALL' | 'OPEN' | 'ACK' | 'RESOLVED'>('ALL');
  const [datasetQuery, setDatasetQuery] = useState('');

  const summary = useMemo(() => {
    const result = { total: incidents.length, open: 0, ack: 0, resolved: 0, criticalOpen: 0 };
    for (const item of incidents) {
      const st = String(item.status || '').toUpperCase();
      const sv = String(item.severity || '').toUpperCase();
      if (st === 'OPEN') result.open += 1;
      if (st === 'ACK') result.ack += 1;
      if (st === 'RESOLVED') result.resolved += 1;
      if (st === 'OPEN' && sv === 'CRITICAL') result.criticalOpen += 1;
    }
    return result;
  }, [incidents]);

  const statusRank = (value: string): number => {
    const normalized = String(value || '').toUpperCase();
    if (normalized === 'OPEN') return 0;
    if (normalized === 'ACK') return 1;
    if (normalized === 'RESOLVED') return 2;
    return 3;
  };

  const filtered = useMemo(() => {
    const query = datasetQuery.trim().toLowerCase();
    return incidents
      .filter((item) => {
      const severityOk = severity === 'ALL' || item.severity === severity;
      const statusOk = status === 'ALL' || item.status === status;
      const datasetName = String(item.dataset_name || item.dataset || '').toLowerCase();
      const datasetOk = !query || datasetName.includes(query);
      return severityOk && statusOk && datasetOk;
      })
      .sort((a, b) => {
        const rank = statusRank(a.status) - statusRank(b.status);
        if (rank !== 0) return rank;
        const ta = new Date(a.updated_at || a.created_at || '').getTime();
        const tb = new Date(b.updated_at || b.created_at || '').getTime();
        return (Number.isFinite(tb) ? tb : 0) - (Number.isFinite(ta) ? ta : 0);
      });
  }, [incidents, severity, status, datasetQuery]);

  return (
    <section className="rounded-xl border border-border bg-card overflow-hidden">
      <div className="px-4 py-3 border-b border-border flex items-center justify-between">
        <div className="text-sm font-medium">Issues & Incident Actions</div>
        <div className="flex items-center gap-2">
          <input
            value={datasetQuery}
            onChange={(e) => setDatasetQuery(e.target.value)}
            placeholder="Filter by dataset"
            className="rounded-lg border border-border bg-background px-2 py-1 text-xs min-w-[160px]"
          />
          <select
            value={severity}
            onChange={(e) => setSeverity(e.target.value as 'ALL' | 'CRITICAL' | 'WARNING')}
            className="rounded-lg border border-border bg-background px-2 py-1 text-xs"
          >
            <option value="ALL">All Severities</option>
            <option value="CRITICAL">Critical</option>
            <option value="WARNING">Warning</option>
          </select>
          <select
            value={status}
            onChange={(e) => setStatus(e.target.value as 'ALL' | 'OPEN' | 'ACK' | 'RESOLVED')}
            className="rounded-lg border border-border bg-background px-2 py-1 text-xs"
          >
            <option value="ALL">All Statuses</option>
            <option value="OPEN">Open</option>
            <option value="ACK">Acknowledged</option>
            <option value="RESOLVED">Resolved</option>
          </select>
        </div>
      </div>
      <div className="px-4 py-2 border-b border-border flex flex-wrap items-center gap-3 text-xs text-muted-foreground">
        <span>Total: {summary.total}</span>
        <span>Open: {summary.open}</span>
        <span>Acknowledged: {summary.ack}</span>
        <span>Resolved: {summary.resolved}</span>
        <span>Critical Open: {summary.criticalOpen}</span>
      </div>

      <div className="divide-y divide-border/60">
        {filtered.map((item) => (
          <article key={item.incident_id} className="p-4">
            <div className="flex items-start justify-between gap-3">
              <div className="min-w-0">
                <div className="flex items-center gap-2 mb-2">
                  <span className={`inline-flex rounded-full px-2 py-1 text-[11px] font-semibold ${severityClass(item.severity)}`}>
                    {item.severity}
                  </span>
                  <span className={`inline-flex rounded-full px-2 py-1 text-[11px] font-semibold ${statusClass(item.status)}`}>
                    {item.status}
                  </span>
                  <span className="text-xs text-muted-foreground">{item.dataset_name || item.dataset || 'unknown'}</span>
                </div>
                <p className="text-sm text-foreground">{item.reason || item.description || item.title || 'No description'}</p>
                <div className="mt-2 text-xs text-muted-foreground">
                  Score: {item.quality_score?.toFixed?.(1) ?? 'N/A'}% | Anomalies: {item.anomaly_count ?? 0} | Z max:{' '}
                  {item.z_score_max?.toFixed?.(2) ?? 'N/A'}
                </div>
              </div>
              <div className="flex items-center gap-2 shrink-0">
                {item.status !== 'ACK' && item.status !== 'RESOLVED' && (
                  <button
                    onClick={() => onAck(item.incident_id)}
                    className="rounded-lg border border-border bg-background px-2 py-1 text-xs font-medium hover:bg-accent"
                  >
                    Ack
                  </button>
                )}
                {item.status !== 'RESOLVED' && (
                  <button
                    onClick={() => onResolve(item.incident_id)}
                    className="rounded-lg border border-border bg-background px-2 py-1 text-xs font-medium hover:bg-accent"
                  >
                    Resolve
                  </button>
                )}
              </div>
            </div>
          </article>
        ))}
        {filtered.length === 0 && (
          <div className="p-10 text-center text-sm text-muted-foreground">
            <div className="mx-auto mb-2 w-fit rounded-full border border-border p-2">
              {incidents.length === 0 ? <ShieldCheck size={16} /> : <AlertTriangle size={16} />}
            </div>
            {incidents.length === 0 ? 'No incidents detected.' : 'No incidents match current filters.'}
          </div>
        )}
      </div>
    </section>
  );
}
