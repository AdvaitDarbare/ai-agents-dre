import { Activity, CheckCircle2, Clock } from 'lucide-react';

import type { GlobalStats } from '@/lib/dre-api';

type Props = {
  stats: GlobalStats | null;
};

function fmtNumber(value: number | null | undefined): string {
  if (typeof value !== 'number' || Number.isNaN(value)) return 'N/A';
  return value.toLocaleString();
}

export default function SystemStatsRibbon({ stats }: Props) {
  return (
    <section className="grid grid-cols-1 lg:grid-cols-3 gap-4">
      <article className="rounded-xl border border-border bg-card p-4 flex items-center gap-3">
        <div className="rounded-lg bg-orange-100 text-orange-700 p-2">
          <Activity size={18} />
        </div>
        <div>
          <div className="text-xs uppercase tracking-wide text-muted-foreground">Total Runs (Today)</div>
          <div className="mt-1 text-2xl font-medium tracking-tight">{fmtNumber(stats?.total_runs_today)}</div>
        </div>
      </article>
      <article className="rounded-xl border border-border bg-card p-4 flex items-center gap-3">
        <div className="rounded-lg bg-emerald-100 text-emerald-700 p-2">
          <CheckCircle2 size={18} />
        </div>
        <div>
          <div className="text-xs uppercase tracking-wide text-muted-foreground">Pass Rate</div>
          <div className="mt-1 text-2xl font-medium tracking-tight">
            {typeof stats?.pass_rate_today === 'number' ? `${stats.pass_rate_today.toFixed(1)}%` : 'N/A'}
          </div>
        </div>
      </article>
      <article className="rounded-xl border border-border bg-card p-4 flex items-center gap-3">
        <div className="rounded-lg bg-sky-100 text-sky-700 p-2">
          <Clock size={18} />
        </div>
        <div>
          <div className="text-xs uppercase tracking-wide text-muted-foreground">Avg Duration</div>
          <div className="mt-1 text-2xl font-medium tracking-tight">
            {typeof stats?.avg_duration === 'number' ? `${Math.round(stats.avg_duration)} ms` : 'N/A'}
          </div>
        </div>
      </article>
    </section>
  );
}
