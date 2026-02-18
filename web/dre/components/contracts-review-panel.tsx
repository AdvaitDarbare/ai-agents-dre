import { useMemo, useState } from 'react';
import { Check, FileWarning, X } from 'lucide-react';

import type { PendingContract } from '@/lib/dre-api';

type Props = {
  pending: PendingContract[];
  onApprove: (datasetName: string, yaml: string) => void;
  onReject: (datasetName: string) => void;
};

export default function ContractsReviewPanel({ pending, onApprove, onReject }: Props) {
  const [editing, setEditing] = useState<Record<string, string>>({});

  const unique = useMemo(() => {
    const byDataset = new Map<string, PendingContract>();
    for (const item of pending) {
      if (!byDataset.has(item.dataset_name)) byDataset.set(item.dataset_name, item);
    }
    return Array.from(byDataset.values());
  }, [pending]);

  const yamlFor = (item: PendingContract) => {
    return editing[item.dataset_name] ?? item.proposed_yaml ?? '';
  };

  return (
    <section className="rounded-xl border border-border bg-card overflow-hidden">
      <div className="px-4 py-3 border-b border-border text-sm font-medium">Pending Contract Reviews</div>
      <div className="divide-y divide-border/60">
        {unique.map((item) => (
          <article key={item.dataset_name} className="p-4">
            <div className="flex items-center justify-between gap-3">
              <div>
                <h3 className="font-medium">{item.dataset_name}</h3>
                <div className="text-xs text-muted-foreground">
                  {item.pending_files?.length || 0} pending file(s) | {item.row_count || 0} rows | {item.column_count || 0} columns
                </div>
              </div>
              <div className="flex gap-2">
                <button
                  onClick={() => onApprove(item.dataset_name, yamlFor(item))}
                  className="inline-flex items-center gap-1 rounded-lg border border-border bg-background px-2 py-1 text-xs font-medium hover:bg-accent"
                >
                  <Check size={12} />
                  Approve
                </button>
                <button
                  onClick={() => onReject(item.dataset_name)}
                  className="inline-flex items-center gap-1 rounded-lg border border-border bg-background px-2 py-1 text-xs font-medium hover:bg-accent"
                >
                  <X size={12} />
                  Reject
                </button>
              </div>
            </div>
            <div className="mt-3">
              <textarea
                value={yamlFor(item)}
                onChange={(e) => setEditing((prev) => ({ ...prev, [item.dataset_name]: e.target.value }))}
                className="h-40 w-full rounded-lg border border-border bg-background p-3 font-mono text-xs"
                placeholder="Proposed YAML will appear here."
              />
            </div>
          </article>
        ))}
        {unique.length === 0 && (
          <div className="p-8 text-sm text-muted-foreground flex items-center gap-2">
            <FileWarning size={16} /> No pending contract proposals.
          </div>
        )}
      </div>
    </section>
  );
}
