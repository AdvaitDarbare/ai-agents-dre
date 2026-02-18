import type { PendingContract } from '@/lib/dre-api';
import type { DatasetRow } from '@/lib/dre-api';

type Props = {
  pending: PendingContract[];
  datasets: DatasetRow[];
};

export default function PendingContractsBar({ pending, datasets }: Props) {
  const pendingCount = new Set(pending.map((item) => item.dataset_name)).size;
  const pendingNames = new Set(pending.map((item) => item.dataset_name));
  const unconfiguredNames = new Set(
    datasets
      .filter((item) => String(item.lifecycle || '').toLowerCase() === 'unconfigured')
      .map((item) => item.name),
  );
  const actionNames = Array.from(new Set([...pendingNames, ...unconfiguredNames])).sort((a, b) => a.localeCompare(b));

  return (
    <section className="rounded-xl border border-border bg-card px-4 py-3">
      <div className="text-xs uppercase tracking-wide text-muted-foreground">Contract Queue</div>
      <div className="mt-1 text-sm font-medium text-foreground">Pending: {pendingCount}</div>
      <div className="mt-1 text-sm text-foreground">
        Needs YAML:{' '}
        {actionNames.length > 0 ? actionNames.join(', ') : 'None'}
      </div>
    </section>
  );
}
