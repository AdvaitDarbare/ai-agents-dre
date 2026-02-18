import type { DatasetRow, PulseRow } from '@/lib/dre-api';

type Props = {
  datasets: DatasetRow[];
  pulse: PulseRow[];
};

export default function SummaryCards({ datasets, pulse }: Props) {
  const healthy = pulse.filter((row) => row.status === 'PASSED').length;
  const needsAttention = pulse.filter((row) => row.status !== 'PASSED').length;

  return (
    <section className="grid grid-cols-1 lg:grid-cols-3 gap-4">
      <article className="rounded-xl border border-border bg-card p-4">
        <div className="text-xs uppercase tracking-wide text-muted-foreground">Datasets</div>
        <div className="mt-2 text-3xl font-medium tracking-tight">{datasets.length}</div>
      </article>
      <article className="rounded-xl border border-border bg-card p-4">
        <div className="text-xs uppercase tracking-wide text-muted-foreground">Healthy</div>
        <div className="mt-2 text-3xl font-medium tracking-tight text-emerald-700">{healthy}</div>
      </article>
      <article className="rounded-xl border border-border bg-card p-4">
        <div className="text-xs uppercase tracking-wide text-muted-foreground">Needs Attention</div>
        <div className="mt-2 text-3xl font-medium tracking-tight text-amber-700">{needsAttention}</div>
      </article>
    </section>
  );
}
