import type { LineageGraph } from '@/lib/dre-api';

type Props = {
  lineage: LineageGraph | null;
};

type DatasetNode = {
  upstream?: Array<string | { name?: string }>;
  consumers?: Array<string | { name?: string }>;
  owner?: string;
  domain?: string;
  criticality?: string;
};

type ContextNode = {
  name: string;
  depth?: number;
  managed?: boolean;
};

function namesFromItems(items?: Array<string | { name?: string }>): string[] {
  if (!items) return [];
  return items
    .map((item) => (typeof item === 'string' ? item : item?.name || ''))
    .map((name) => name.trim())
    .filter(Boolean);
}

function toContextNodes(items: unknown): ContextNode[] {
  if (!Array.isArray(items)) return [];
  const rows: ContextNode[] = [];
  for (const item of items) {
    if (typeof item === 'string') {
      const name = item.trim();
      if (name) rows.push({ name, depth: 1, managed: false });
      continue;
    }
    if (!item || typeof item !== 'object') continue;
    const source = item as { name?: unknown; depth?: unknown; managed?: unknown };
    const name = String(source.name || '').trim();
    if (!name) continue;
    const depthNumber = Number(source.depth);
    rows.push({
      name,
      depth: Number.isFinite(depthNumber) ? depthNumber : undefined,
      managed: typeof source.managed === 'boolean' ? source.managed : undefined,
    });
  }
  return rows;
}

function normalizeCriticality(value?: string): string {
  const normalized = String(value || '').trim().toUpperCase();
  if (!normalized) return 'UNKNOWN';
  return normalized;
}

function scopeLabel(managed?: boolean): string {
  if (managed === false) return 'external';
  if (managed === true) return 'managed';
  return 'unknown';
}

export default function LineagePanel({ lineage }: Props) {
  const datasetMap = (lineage?.datasets || {}) as Record<string, DatasetNode>;
  const datasetEntries = Object.entries(datasetMap);
  const summary = lineage?.summary || {};
  const context = lineage?.context || null;

  const selectedDataset = String(context?.dataset || '').trim();
  const selectedNode = selectedDataset ? datasetMap[selectedDataset] : undefined;
  const upstreamScope = toContextNodes(context?.upstream);
  const downstreamScope = toContextNodes(context?.downstream);

  const selectedUpstream =
    upstreamScope.length > 0
      ? upstreamScope
      : namesFromItems(selectedNode?.upstream).map((name) => ({ name, depth: 1, managed: undefined }));
  const selectedDownstream =
    downstreamScope.length > 0
      ? downstreamScope
      : namesFromItems(selectedNode?.consumers).map((name) => ({ name, depth: 1, managed: undefined }));

  const relationshipCount = Number(summary.upstream_edge_count || 0) + Number(summary.downstream_edge_count || 0);

  return (
    <section className="space-y-4">
      <div className="grid gap-3 md:grid-cols-4">
        <article className="rounded-xl border border-border bg-card p-4">
          <div className="text-xs uppercase tracking-wide text-muted-foreground">Datasets</div>
          <div className="mt-1 text-xl font-semibold">{summary.dataset_count ?? datasetEntries.length}</div>
        </article>
        <article className="rounded-xl border border-border bg-card p-4">
          <div className="text-xs uppercase tracking-wide text-muted-foreground">Relationships</div>
          <div className="mt-1 text-xl font-semibold">{relationshipCount}</div>
        </article>
        <article className="rounded-xl border border-border bg-card p-4">
          <div className="text-xs uppercase tracking-wide text-muted-foreground">Isolated Datasets</div>
          <div className="mt-1 text-xl font-semibold">{summary.isolated_dataset_count ?? 0}</div>
        </article>
      </div>

      {selectedDataset ? (
        <section className="rounded-xl border border-border bg-card overflow-hidden">
          <div className="px-4 py-3 border-b border-border text-sm font-medium">Dependency Flow: {selectedDataset}</div>
          <div className="p-4 grid gap-4 lg:grid-cols-[1fr_auto_1fr] items-start">
            <div className="space-y-2">
              <div className="text-xs uppercase tracking-wide text-muted-foreground">Upstream</div>
              {selectedUpstream.length === 0 ? (
                <div className="text-sm text-muted-foreground">No upstream dependencies.</div>
              ) : (
                selectedUpstream.slice(0, 20).map((row) => (
                  <div key={`up-${row.name}`} className="rounded-lg border border-border bg-background px-3 py-2 text-sm">
                    <div className="font-medium">{row.name}</div>
                    <div className="text-xs text-muted-foreground">
                      depth {row.depth ?? 1} | {scopeLabel(row.managed)}
                    </div>
                  </div>
                ))
              )}
            </div>

            <div className="hidden lg:flex h-full items-center text-muted-foreground">-&gt;</div>

            <div className="space-y-2">
              <div className="rounded-xl border border-border bg-secondary px-4 py-3">
                <div className="text-xs uppercase tracking-wide text-muted-foreground">Selected</div>
                <div className="mt-1 text-sm font-semibold break-all">{selectedDataset}</div>
                <div className="mt-2 text-xs text-muted-foreground">
                  owner {selectedNode?.owner || 'unassigned'} | domain {selectedNode?.domain || 'unknown'} | criticality{' '}
                  {normalizeCriticality(selectedNode?.criticality)}
                </div>
              </div>

              <div className="text-xs uppercase tracking-wide text-muted-foreground">Downstream Impact</div>
              {selectedDownstream.length === 0 ? (
                <div className="text-sm text-muted-foreground">No downstream consumers.</div>
              ) : (
                selectedDownstream.slice(0, 20).map((row) => (
                  <div key={`down-${row.name}`} className="rounded-lg border border-border bg-background px-3 py-2 text-sm">
                    <div className="font-medium">{row.name}</div>
                    <div className="text-xs text-muted-foreground">
                      depth {row.depth ?? 1} | {scopeLabel(row.managed)}
                    </div>
                  </div>
                ))
              )}
            </div>
          </div>
        </section>
      ) : (
        <section className="rounded-xl border border-border bg-card p-4">
          <div className="text-sm text-muted-foreground">
            Select a dataset in Data Pulse first to view a focused dependency flow. This page stays global when no dataset context is selected.
          </div>
        </section>
      )}
    </section>
  );
}
