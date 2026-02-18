import { useMemo, useState } from 'react';

import type { RunEvent } from '@/lib/dre-api';

type Props = {
  runs: RunEvent[];
};

function formatWhen(run: RunEvent): string {
  const ts = run.timestamp;
  if (ts) {
    const dt = new Date(ts);
    if (!Number.isNaN(dt.getTime())) {
      return dt.toLocaleString();
    }
  }
  if (run.date && run.time) return `${run.date} ${run.time}`;
  return 'N/A';
}

function statusClass(status: string): string {
  if (status === 'PASSED') return 'bg-emerald-50 text-emerald-700 ring-1 ring-emerald-200';
  if (status === 'BLOCKED') return 'bg-rose-50 text-rose-700 ring-1 ring-rose-200';
  return 'bg-amber-50 text-amber-700 ring-1 ring-amber-200';
}

export default function RunsTable({ runs }: Props) {
  const [status, setStatus] = useState<'ALL' | 'PASSED' | 'WARNING' | 'BLOCKED'>('ALL');
  const [datasetQuery, setDatasetQuery] = useState('');

  const summary = useMemo(() => {
    const totals = { total: runs.length, passed: 0, warning: 0, blocked: 0 };
    for (const run of runs) {
      const normalized = String(run.status || '').toUpperCase();
      if (normalized === 'PASSED') totals.passed += 1;
      else if (normalized === 'WARNING') totals.warning += 1;
      else if (normalized === 'BLOCKED') totals.blocked += 1;
    }
    return totals;
  }, [runs]);

  const filtered = useMemo(() => {
    const query = datasetQuery.trim().toLowerCase();
    return runs.filter((run) => {
      const statusOk = status === 'ALL' || String(run.status || '').toUpperCase() === status;
      const datasetOk = !query || String(run.dataset || '').toLowerCase().includes(query);
      return statusOk && datasetOk;
    });
  }, [runs, status, datasetQuery]);

  return (
    <section className="rounded-xl border border-border bg-card overflow-hidden">
      <div className="px-4 py-3 border-b border-border flex flex-wrap items-center justify-between gap-3">
        <div className="text-sm font-medium">Run History</div>
        <div className="text-xs text-muted-foreground">Total runs: {summary.total}</div>
      </div>
      <div className="px-4 py-3 border-b border-border flex flex-wrap items-center gap-2">
        <input
          value={datasetQuery}
          onChange={(e) => setDatasetQuery(e.target.value)}
          placeholder="Filter by dataset"
          className="rounded-lg border border-border bg-background px-3 py-1.5 text-xs min-w-[180px]"
        />
        <select
          value={status}
          onChange={(e) => setStatus(e.target.value as 'ALL' | 'PASSED' | 'WARNING' | 'BLOCKED')}
          className="rounded-lg border border-border bg-background px-3 py-1.5 text-xs"
        >
          <option value="ALL">All statuses</option>
          <option value="PASSED">Passed</option>
          <option value="WARNING">Warning</option>
          <option value="BLOCKED">Blocked</option>
        </select>
      </div>
      <div className="overflow-x-auto">
        <table className="min-w-full text-sm">
          <thead className="bg-muted/30 text-muted-foreground">
            <tr>
              <th className="text-left px-4 py-3">Dataset</th>
              <th className="text-left px-4 py-3">Status</th>
              <th className="text-left px-4 py-3">Quality</th>
              <th className="text-left px-4 py-3">When</th>
              <th className="text-left px-4 py-3">Reason</th>
            </tr>
          </thead>
          <tbody>
            {filtered.map((run) => (
              <tr key={run.id} className="border-t border-border/60">
                <td className="px-4 py-3 font-medium">{run.dataset}</td>
                <td className="px-4 py-3">
                  <span className={`inline-flex rounded-full px-2 py-1 text-xs font-semibold ${statusClass(run.status)}`}>
                    {run.status}
                  </span>
                </td>
                <td className="px-4 py-3">{run.quality_score?.toFixed?.(1) ?? 'N/A'}%</td>
                <td className="px-4 py-3 text-muted-foreground">
                  {formatWhen(run)}
                </td>
                <td className="px-4 py-3 text-muted-foreground max-w-[380px] truncate">{run.reason || '-'}</td>
              </tr>
            ))}
            {filtered.length === 0 && (
              <tr>
                <td className="px-4 py-8 text-sm text-muted-foreground" colSpan={5}>
                  {runs.length === 0 ? 'No runs yet.' : 'No runs match current filters.'}
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </section>
  );
}
