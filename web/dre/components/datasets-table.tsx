'use client';

import { useMemo, useState } from 'react';
import { AlertCircle, Database, FileText, Loader2, Microscope, Search, Table, Trash2, Zap } from 'lucide-react';

import type { DatasetRow, PendingContract, PulseRow } from '@/lib/dre-api';

type BusyState = Record<string, 'scan' | 'delete'>;

type Row = {
  name: string;
  lifecycle?: string;
  status?: string;
  quality?: number;
  dataFile?: string | null;
  pendingCount?: number;
};

type Props = {
  loading: boolean;
  datasets: DatasetRow[];
  pulse: PulseRow[];
  pending: PendingContract[];
  busy: BusyState;
  selectedDataset?: string | null;
  onSelectDataset: (datasetName: string) => void;
  onRunScan: (datasetName: string) => void;
  onDelete: (datasetName: string) => void;
  onPreview: (datasetName: string) => void;
  onProfile: (datasetName: string) => void;
  onForceLoad: (datasetName: string) => void;
  onOpenWizard: (datasetName: string, filePath?: string | null) => void;
};

function statusClass(status?: string): string {
  if (status === 'PASSED') return 'bg-emerald-50 text-emerald-700 ring-1 ring-emerald-200';
  if (status === 'BLOCKED') return 'bg-rose-50 text-rose-700 ring-1 ring-rose-200';
  if (!status) return 'bg-zinc-100 text-zinc-700 ring-1 ring-zinc-200';
  return 'bg-amber-50 text-amber-700 ring-1 ring-amber-200';
}

export default function DatasetsTable({
  loading,
  datasets,
  pulse,
  pending,
  busy,
  selectedDataset,
  onSelectDataset,
  onRunScan,
  onDelete,
  onPreview,
  onProfile,
  onForceLoad,
  onOpenWizard,
}: Props) {
  const [query, setQuery] = useState('');
  const pendingByDataset = useMemo(() => {
    const map = new Map<string, PendingContract>();
    for (const item of pending) {
      if (!map.has(item.dataset_name)) map.set(item.dataset_name, item);
    }
    return map;
  }, [pending]);

  const rows = useMemo<Row[]>(() => {
    const pulseByName = new Map(pulse.map((p) => [p.name, p]));
    const merged: Row[] = datasets.map((ds) => {
      const p = pulseByName.get(ds.name);
      return {
        name: ds.name,
        lifecycle: ds.lifecycle,
        status: p?.status,
        quality: p?.quality_score,
        dataFile: ds.data_file || null,
        pendingCount: pendingByDataset.get(ds.name)?.pending_files?.length || 0,
      };
    });

    // Include pulse-only datasets that might not be in discover list (should be rare).
    const seen = new Set(merged.map((r) => r.name));
    for (const p of pulse) {
      if (seen.has(p.name)) continue;
      merged.push({ name: p.name, lifecycle: p.lifecycle, status: p.status, quality: p.quality_score, dataFile: null });
    }

    for (const proposal of pending) {
      if (seen.has(proposal.dataset_name)) continue;
      merged.push({
        name: proposal.dataset_name,
        lifecycle: 'unconfigured',
        status: 'PENDING_CONTRACT',
        quality: undefined,
        dataFile: proposal.source_file || proposal.pending_files?.[0] || null,
        pendingCount: proposal.pending_files?.length || 0,
      });
    }

    merged.sort((a, b) => a.name.localeCompare(b.name));
    return merged;
  }, [datasets, pendingByDataset, pending, pulse]);

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    if (!q) return rows;
    return rows.filter((row) => row.name.toLowerCase().includes(q) || String(row.lifecycle || '').toLowerCase().includes(q));
  }, [query, rows]);

  return (
    <section className="space-y-6">
      <div className="w-full max-w-md flex items-center gap-2 rounded-2xl border border-border bg-card px-4 py-3">
        <Search size={18} className="text-muted-foreground" />
        <input
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="Search datasets, lifecycle..."
          className="w-full bg-transparent text-sm outline-none"
        />
      </div>

      {loading ? (
        <div className="rounded-2xl border border-border bg-card p-8 text-sm text-muted-foreground flex items-center gap-2">
          <Loader2 size={16} className="animate-spin" /> Loading datasets...
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-6">
          {filtered.map((row) => {
            const isPending = row.lifecycle === 'unconfigured' || row.status === 'PENDING_CONTRACT';
            const isDeleting = busy[row.name] === 'delete';
            const isScanning = busy[row.name] === 'scan';

            return (
              <article
                key={row.name}
                className={`rounded-3xl border bg-card overflow-hidden ${isPending ? 'border-dashed border-amber-300/70' : 'border-border'
                  } ${selectedDataset === row.name ? 'ring-1 ring-foreground/20' : ''}`}
              >
                <div className="px-6 py-5 border-b border-border/70 flex items-start justify-between gap-3">
                  <div className="flex items-center gap-3 min-w-0">
                    <div className={`h-10 w-10 rounded-xl flex items-center justify-center ${isPending ? 'bg-amber-100 text-amber-700' : 'bg-muted text-foreground'}`}>
                      {isPending ? <FileText size={18} /> : <Table size={18} />}
                    </div>
                    <div className="min-w-0">
                      <h3 className="font-semibold truncate">{row.name}</h3>
                      <p className="text-[11px] uppercase tracking-wide text-muted-foreground">{row.lifecycle || 'unknown'}</p>
                    </div>
                  </div>
                  <span className={`inline-flex rounded-full px-2 py-1 text-[10px] font-semibold ${statusClass(row.status)}`}>
                    {row.status || 'UNKNOWN'}
                  </span>
                </div>

                <div className="px-6 py-5 space-y-4">
                  <div className="grid grid-cols-2 gap-4 text-sm">
                    <div>
                      <p className="text-[10px] uppercase tracking-wide text-muted-foreground">Quality</p>
                      <p className="font-semibold">{row.quality?.toFixed?.(1) ?? 'N/A'}%</p>
                    </div>
                    <div>
                      <p className="text-[10px] uppercase tracking-wide text-muted-foreground">Pending Files</p>
                      <p className="font-semibold">{row.pendingCount || 0}</p>
                    </div>
                  </div>
                  {isPending && (
                    <div className="rounded-xl border border-amber-300/70 bg-amber-50 px-3 py-2 text-xs text-amber-900 flex items-start gap-2">
                      <AlertCircle size={14} className="mt-0.5 shrink-0" />
                      Contract approval required before managed runs.
                    </div>
                  )}
                </div>

                <div className="px-6 py-4 border-t border-border/70 bg-muted/20 space-y-2">
                  <div className="grid grid-cols-2 gap-2">
                    {isPending ? (
                      <button
                        onClick={() => onOpenWizard(row.name, row.dataFile)}
                        className="inline-flex items-center justify-center gap-1 rounded-xl border border-border bg-amber-50 text-amber-800 px-2 py-2 text-xs font-medium hover:bg-amber-100"
                      >
                        <Zap size={12} /> Generate Proposal
                      </button>
                    ) : (
                      <button
                        onClick={() => onRunScan(row.name)}
                        disabled={isScanning}
                        className="inline-flex items-center justify-center gap-1 rounded-xl border border-border bg-background px-2 py-2 text-xs font-medium hover:bg-accent disabled:opacity-50"
                      >
                        <Zap size={12} />
                        {isScanning ? 'Scanning...' : 'Run Scan'}
                      </button>
                    )}
                    <button
                      onClick={() => onPreview(row.name)}
                      className="inline-flex items-center justify-center gap-1 rounded-xl border border-border bg-background px-2 py-2 text-xs font-medium hover:bg-accent"
                    >
                      <Database size={12} /> Preview
                    </button>
                    <button
                      onClick={() => onProfile(row.name)}
                      className="inline-flex items-center justify-center gap-1 rounded-xl border border-border bg-background px-2 py-2 text-xs font-medium hover:bg-accent"
                    >
                      <Microscope size={12} /> Profile
                    </button>
                    <button
                      onClick={() => onForceLoad(row.name)}
                      className="inline-flex items-center justify-center gap-1 rounded-xl border border-border bg-background px-2 py-2 text-xs font-medium hover:bg-accent"
                    >
                      <Zap size={12} className="text-amber-500" /> Force Load
                    </button>
                    <button
                      onClick={() => onSelectDataset(row.name)}
                      className="inline-flex items-center justify-center gap-1 rounded-xl border border-border bg-background px-2 py-2 text-xs font-medium hover:bg-accent"
                    >
                      Inspect
                    </button>
                  </div>
                  <button
                    onClick={() => onDelete(row.name)}
                    disabled={isDeleting}
                    className="w-full inline-flex items-center justify-center gap-1 rounded-xl border border-rose-200 bg-rose-50 text-rose-700 px-2 py-2 text-xs font-medium hover:bg-rose-100 disabled:opacity-50"
                  >
                    <Trash2 size={12} />
                    {isDeleting ? 'Deleting...' : 'Delete Dataset'}
                  </button>
                </div>
              </article>
            );
          })}
        </div>
      )}

      {!loading && filtered.length === 0 && (
        <div className="rounded-2xl border border-border bg-card p-8 text-sm text-muted-foreground">No datasets match the current filter.</div>
      )}
    </section>
  );
}
