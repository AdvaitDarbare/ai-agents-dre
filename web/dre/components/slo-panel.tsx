import type { SloCheck, SloSummary } from '@/lib/dre-api';

type Props = {
  datasetName: string | null;
  summary: SloSummary | null;
  checks: SloCheck[];
};

export default function SloPanel({ datasetName, summary, checks }: Props) {
  if (!datasetName) {
    return (
      <section className="rounded-xl border border-border bg-card p-4 text-sm text-muted-foreground">
        Select a dataset in Health Pulse to inspect SLOs.
      </section>
    );
  }

  const summaryChecks = summary?.checks || [];

  return (
    <section className="rounded-xl border border-border bg-card overflow-hidden">
      <div className="px-4 py-3 border-b border-border text-sm font-medium">SLOs for {datasetName}</div>
      <div className="p-4 space-y-4">
        <div className="grid gap-3 md:grid-cols-5">
          <article className="rounded-lg border border-border bg-background p-3">
            <div className="text-xs uppercase tracking-wide text-muted-foreground">Overall Pass Rate</div>
            <div className="mt-1 text-2xl font-medium">{summary?.overall_pass_rate?.toFixed?.(2) ?? 'N/A'}%</div>
          </article>
          <article className="rounded-lg border border-border bg-background p-3">
            <div className="text-xs uppercase tracking-wide text-muted-foreground">Failing Checks</div>
            <div className="mt-1 text-2xl font-medium">{summary?.failing_checks ?? 0}</div>
          </article>
          <article className="rounded-lg border border-border bg-background p-3">
            <div className="text-xs uppercase tracking-wide text-muted-foreground">Failing SLOs</div>
            <div className="mt-1 text-2xl font-medium">{summary?.failing_slo_count ?? 0}</div>
          </article>
          <article className="rounded-lg border border-border bg-background p-3">
            <div className="text-xs uppercase tracking-wide text-muted-foreground">Avg Burn</div>
            <div className="mt-1 text-2xl font-medium">{summary?.overall_error_budget_burn_avg?.toFixed?.(2) ?? '0.00'}</div>
          </article>
          <article className="rounded-lg border border-border bg-background p-3">
            <div className="text-xs uppercase tracking-wide text-muted-foreground">Total Checks</div>
            <div className="mt-1 text-2xl font-medium">{summary?.total_checks ?? 0}</div>
          </article>
        </div>

        <div className="rounded-lg border border-border overflow-x-auto">
          <div className="px-3 py-2 text-xs uppercase tracking-wide text-muted-foreground border-b border-border">SLO Breakdown</div>
          <table className="min-w-full text-sm">
            <thead className="bg-muted/30 text-muted-foreground">
              <tr>
                <th className="text-left px-3 py-2">SLO</th>
                <th className="text-left px-3 py-2">Pass Rate</th>
                <th className="text-left px-3 py-2">Last Status</th>
                <th className="text-left px-3 py-2">Fail Streak</th>
                <th className="text-left px-3 py-2">Avg Burn</th>
              </tr>
            </thead>
            <tbody>
              {summaryChecks.map((row, idx) => (
                <tr key={`${row.slo_name}-${idx}`} className="border-t border-border/60">
                  <td className="px-3 py-2">{row.slo_name}</td>
                  <td className="px-3 py-2">{row.pass_rate?.toFixed?.(2) ?? 'N/A'}%</td>
                  <td className="px-3 py-2">{row.last_status ?? 'UNKNOWN'}</td>
                  <td className="px-3 py-2">{row.recent_fail_streak ?? 0}</td>
                  <td className="px-3 py-2">{row.avg_error_budget_burn?.toFixed?.(2) ?? '0.00'}</td>
                </tr>
              ))}
              {summaryChecks.length === 0 && (
                <tr>
                  <td className="px-3 py-4 text-muted-foreground" colSpan={5}>
                    No SLO aggregate data yet.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>

        <div className="rounded-lg border border-border overflow-x-auto">
          <div className="px-3 py-2 text-xs uppercase tracking-wide text-muted-foreground border-b border-border">Recent SLO Checks</div>
          <table className="min-w-full text-sm">
            <thead className="bg-muted/30 text-muted-foreground">
              <tr>
                <th className="text-left px-3 py-2">SLO</th>
                <th className="text-left px-3 py-2">Status</th>
                <th className="text-left px-3 py-2">Observed</th>
                <th className="text-left px-3 py-2">Target</th>
                <th className="text-left px-3 py-2">Severity</th>
                <th className="text-left px-3 py-2">Burn</th>
              </tr>
            </thead>
            <tbody>
              {checks.slice(0, 20).map((row, idx) => (
                <tr key={`${row.run_id || 'run'}-${idx}`} className="border-t border-border/60">
                  <td className="px-3 py-2">{row.slo_name}</td>
                  <td className="px-3 py-2">{row.status}</td>
                  <td className="px-3 py-2">{row.observed_value ?? 'N/A'}</td>
                  <td className="px-3 py-2">{row.target_value ?? 'N/A'}</td>
                  <td className="px-3 py-2">{String(row.metadata?.severity || 'NONE')}</td>
                  <td className="px-3 py-2">{row.error_budget_burn?.toFixed?.(2) ?? '0.00'}</td>
                </tr>
              ))}
              {checks.length === 0 && (
                <tr>
                  <td className="px-3 py-6 text-muted-foreground" colSpan={6}>
                    No SLO checks yet. Run a scan to populate SLO history.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </section>
  );
}
