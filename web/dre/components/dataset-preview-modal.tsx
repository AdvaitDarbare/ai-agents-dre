'use client';

import { useEffect, useMemo, useState } from 'react';
import { Database, Loader2, X } from 'lucide-react';

import { getDatasetPreview, type DatasetPreview } from '@/lib/dre-api';

type Props = {
  open: boolean;
  datasetName: string | null;
  limit?: number;
  onClose: () => void;
};

type LoadState =
  | { status: 'idle' }
  | { status: 'loading' }
  | { status: 'error'; message: string }
  | { status: 'ready'; data: DatasetPreview };

function safeString(value: unknown): string {
  if (value === null) return 'null';
  if (value === undefined) return 'undefined';
  if (typeof value === 'string') return value;
  if (typeof value === 'number' || typeof value === 'boolean') return String(value);
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

export default function DatasetPreviewModal({ open, datasetName, limit = 100, onClose }: Props) {
  const [state, setState] = useState<LoadState>({ status: 'idle' });

  useEffect(() => {
    if (!open || !datasetName) {
      setState({ status: 'idle' });
      return;
    }

    let cancelled = false;
    setState({ status: 'loading' });
    void (async () => {
      try {
        const data = await getDatasetPreview(datasetName, limit);
        if (cancelled) return;
        setState({ status: 'ready', data });
      } catch (error) {
        if (cancelled) return;
        setState({ status: 'error', message: error instanceof Error ? error.message : 'Failed to load dataset preview' });
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [datasetName, limit, open]);

  const table = useMemo(() => {
    if (state.status !== 'ready') return null;
    const columns = Array.isArray(state.data.columns) ? state.data.columns : [];
    const rows = Array.isArray(state.data.data) ? state.data.data : [];
    return { columns, rows };
  }, [state]);

  if (!open) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-background/80 backdrop-blur-sm">
      <div className="bg-card rounded-2xl shadow-2xl w-full max-w-6xl max-h-[90vh] flex flex-col overflow-hidden border border-border">
        <div className="p-4 border-b border-border flex justify-between items-center bg-muted/30">
          <div className="flex items-center gap-3">
            <div className="p-2 bg-primary/10 text-primary rounded-lg">
              <Database size={18} />
            </div>
            <div>
              <h3 className="font-medium text-foreground">Data Preview</h3>
              <p className="text-xs text-muted-foreground">{datasetName || 'unknown'} · first {limit} rows</p>
            </div>
          </div>
          <button onClick={onClose} className="p-2 rounded-lg hover:bg-accent text-muted-foreground" aria-label="Close">
            <X size={18} />
          </button>
        </div>

        <div className="flex-1 overflow-auto bg-background">
          {state.status === 'loading' && (
            <div className="p-10 flex items-center gap-3 text-sm text-muted-foreground">
              <Loader2 size={18} className="animate-spin" /> Loading preview...
            </div>
          )}

          {state.status === 'error' && (
            <div className="p-10 text-sm text-foreground">
              <div className="rounded-xl border border-border bg-muted/30 p-4">
                <div className="text-sm font-medium">Preview failed</div>
                <div className="mt-1 text-xs text-muted-foreground">{state.message}</div>
              </div>
            </div>
          )}

          {state.status === 'ready' && table && (
            <>
              {table.rows.length === 0 || table.columns.length === 0 ? (
                <div className="p-10 text-sm text-muted-foreground">No rows available for preview.</div>
              ) : (
                <table className="min-w-full text-sm">
                  <thead className="sticky top-0 bg-muted/30 text-muted-foreground border-b border-border">
                    <tr>
                      <th className="text-center px-4 py-3 w-16">#</th>
                      {table.columns.map((col) => (
                        <th key={col} className="text-left px-4 py-3 whitespace-nowrap">
                          {col}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {table.rows.map((row, idx) => (
                      <tr key={idx} className="border-b border-border/60 hover:bg-muted/20">
                        <td className="text-center px-4 py-2 text-xs text-muted-foreground">{idx + 1}</td>
                        {table.columns.map((col) => (
                          <td
                            key={`${idx}-${col}`}
                            className="px-4 py-2 max-w-[360px] truncate"
                            title={safeString((row as any)?.[col])}
                          >
                            {safeString((row as any)?.[col])}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </>
          )}
        </div>

        <div className="p-3 border-t border-border bg-muted/30 text-xs text-muted-foreground text-center">
          Read-only preview. Large values are truncated for performance.
        </div>
      </div>
    </div>
  );
}

