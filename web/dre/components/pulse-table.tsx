import { Fragment, useState } from 'react';
import { Activity, ChevronRight, Loader2, Zap } from 'lucide-react';

import type { PulseRow } from '@/lib/dre-api';
import ExpandedDatasetDetail from '@/dre/components/expanded-dataset-detail';

type BusyState = Record<string, 'scan' | 'delete'>;

type Props = {
  loading: boolean;
  pulse: PulseRow[];
  busy: BusyState;
  onRunScan: (datasetName: string) => void;
  onDeepDive?: (datasetName: string) => void;
  refreshToken?: number;
  selectedDataset?: string | null;
  onSelectDataset?: (datasetName: string) => void;
  onPreviewDataset?: (datasetName: string) => void;
  onProfileDataset?: (datasetName: string) => void;
  onForceLoad?: (datasetName: string) => void;
  onAfterDatasetChange?: () => void;
};

function statusClass(status: string): string {
  if (status === 'PASSED') return 'bg-emerald-50 text-emerald-700 ring-1 ring-emerald-200';
  if (status === 'BLOCKED') return 'bg-rose-50 text-rose-700 ring-1 ring-rose-200';
  return 'bg-amber-50 text-amber-700 ring-1 ring-amber-200';
}

export default function PulseTable({
  loading,
  pulse,
  busy,
  onRunScan,
  onDeepDive,
  refreshToken = 0,
  selectedDataset,
  onSelectDataset,
  onPreviewDataset,
  onProfileDataset,
  onForceLoad,
  onAfterDatasetChange,
}: Props) {
  const [expanded, setExpanded] = useState<Set<string>>(() => new Set());

  const toggleExpand = (name: string) => {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(name)) next.delete(name);
      else next.add(name);
      return next;
    });
    onSelectDataset?.(name);
  };

  return (
    <section className="rounded-xl border border-border bg-card overflow-hidden">
      <div className="px-4 py-3 border-b border-border flex items-center gap-2 text-sm font-medium">
        <Activity size={16} />
        Active Dataset Pulse
      </div>

      {loading ? (
        <div className="p-8 text-sm text-muted-foreground flex items-center gap-2">
          <Loader2 size={16} className="animate-spin" /> Loading datasets...
        </div>
      ) : (
        <div className="overflow-x-auto">
          <table className="min-w-full text-sm">
            <thead className="bg-muted/30 text-muted-foreground">
              <tr>
                <th className="text-left px-4 py-3">Dataset</th>
                <th className="text-left px-4 py-3">Lifecycle</th>
                <th className="text-left px-4 py-3">Status</th>
                <th className="text-left px-4 py-3">Quality</th>
                <th className="text-left px-4 py-3">Actions</th>
              </tr>
            </thead>
            <tbody>
              {pulse.map((row) => (
                <Fragment key={row.name}>
                  <tr
                    onClick={() => toggleExpand(row.name)}
                    className={`border-t border-border/60 cursor-pointer hover:bg-muted/20 ${selectedDataset === row.name ? 'bg-muted/30' : ''
                      }`}
                  >
                    <td className="px-4 py-3 font-medium text-foreground">
                      <div className="flex items-center gap-2">
                        <ChevronRight
                          size={16}
                          className={`text-muted-foreground transition-transform ${expanded.has(row.name) ? 'rotate-90' : ''}`}
                        />
                        {row.name}
                      </div>
                    </td>
                    <td className="px-4 py-3 text-muted-foreground">{row.lifecycle || 'unknown'}</td>
                    <td className="px-4 py-3">
                      <span className={`inline-flex rounded-full px-2 py-1 text-xs font-semibold ${statusClass(row.status)}`}>
                        {row.status}
                      </span>
                    </td>
                    <td className="px-4 py-3 text-foreground">{row.quality_score?.toFixed?.(1) ?? 'N/A'}%</td>
                    <td className="px-4 py-3">
                      <div className="flex items-center gap-2">
                        <button
                          onClick={(e) => {
                            e.stopPropagation();
                            onRunScan(row.name);
                          }}
                          disabled={busy[row.name] === 'scan'}
                          className="inline-flex items-center gap-1 rounded-lg border border-border bg-background px-2 py-1 text-xs font-medium transition-colors hover:bg-accent disabled:opacity-50"
                        >
                          <Zap size={12} />
                          {busy[row.name] === 'scan' ? 'Scanning...' : 'Run Scan'}
                        </button>
                        {onSelectDataset && (
                          <button
                            onClick={(e) => {
                              e.stopPropagation();
                              onSelectDataset(row.name);
                            }}
                            className="inline-flex items-center gap-1 rounded-lg border border-border px-2 py-1 text-xs font-medium text-muted-foreground transition-colors hover:bg-accent hover:text-foreground"
                          >
                            Inspect
                          </button>
                        )}
                        {onDeepDive && (
                          <button
                            onClick={(e) => {
                              e.stopPropagation();
                              onDeepDive(row.name);
                            }}
                            className="inline-flex items-center gap-1 rounded-lg border border-border px-2 py-1 text-xs font-medium text-muted-foreground transition-colors hover:bg-accent hover:text-foreground"
                          >
                            Deep Dive
                          </button>
                        )}
                      </div>
                    </td>
                  </tr>

                  {expanded.has(row.name) && (
                    <tr className="border-t border-border/60">
                      <td colSpan={5} className="p-3 bg-muted/10">
                        <ExpandedDatasetDetail
                          datasetName={row.name}
                          refreshToken={refreshToken}
                          onPreview={() => onPreviewDataset?.(row.name)}
                          onProfile={() => onProfileDataset?.(row.name)}
                          onForceLoad={() => onForceLoad?.(row.name)}
                          onAfterChange={() => onAfterDatasetChange?.()}
                        />
                      </td>
                    </tr>
                  )}
                </Fragment>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}
