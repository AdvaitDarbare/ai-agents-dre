import type { SystemHealthRow } from '@/lib/dre-api';

type Props = {
  rows: SystemHealthRow[];
};

function healthBadge(status?: string): string {
  const normalized = String(status || '').toUpperCase();
  if (normalized === 'UP' || normalized === 'HEALTHY') return 'bg-emerald-50 text-emerald-700 ring-1 ring-emerald-200';
  if (normalized === 'DOWN' || normalized === 'UNHEALTHY') return 'bg-rose-50 text-rose-700 ring-1 ring-rose-200';
  return 'bg-amber-50 text-amber-700 ring-1 ring-amber-200';
}

export default function SystemHealthPanel({ rows }: Props) {
  return (
    <section className="rounded-xl border border-border bg-card overflow-hidden">
      <div className="px-4 py-3 border-b border-border text-sm font-medium">System Health</div>
      {rows.length === 0 ? (
        <div className="px-4 py-6 text-sm text-muted-foreground">No upstream health checks reported yet.</div>
      ) : (
        <div className="overflow-x-auto">
          <table className="min-w-full text-sm">
            <thead className="bg-muted/30 text-muted-foreground">
              <tr>
                <th className="text-left px-4 py-3">Dataset</th>
                <th className="text-left px-4 py-3">Upstream</th>
                <th className="text-left px-4 py-3">Status</th>
                <th className="text-left px-4 py-3">Details</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row, idx) => {
                const upstream = (row.upstream || {}) as Record<string, any>;
                const name = String(upstream.name || upstream.service || upstream.endpoint || 'unknown');
                const status = String(upstream.status || 'UNKNOWN');
                const details = String(upstream.details || upstream.message || '');
                return (
                  <tr key={`${row.dataset}-${name}-${idx}`} className="border-t border-border/60">
                    <td className="px-4 py-3 font-medium text-foreground">{row.dataset}</td>
                    <td className="px-4 py-3 text-muted-foreground">{name}</td>
                    <td className="px-4 py-3">
                      <span className={`inline-flex rounded-full px-2 py-1 text-xs font-semibold ${healthBadge(status)}`}>
                        {status}
                      </span>
                    </td>
                    <td className="px-4 py-3 text-muted-foreground">{details || '-'}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}
