'use client';

import { useEffect, useMemo, useState } from 'react';
import { ChevronDown, ChevronRight, Loader2, Microscope, X } from 'lucide-react';

import { getDatasetProfile, type ProfileResponse } from '@/lib/dre-api';

type Props = {
  open: boolean;
  datasetName: string | null;
  onClose: () => void;
};

type LoadState =
  | { status: 'idle' }
  | { status: 'loading' }
  | { status: 'error'; message: string }
  | { status: 'ready'; data: ProfileResponse };

type ProfileLike = {
  dataset_name?: string;
  total_rows?: number;
  total_columns?: number;
  memory_usage_mb?: number;
  overall_quality_score?: number;
  columns?: Record<
    string,
    {
      type?: string;
      null_count?: number;
      unique_count?: number;
      quality_score?: number;
    }
  >;
  column_profiles?: Record<
    string,
    {
      type?: string;
      null_count?: number;
      unique_count?: number;
      quality_score?: number;
    }
  >;
};

function asNumber(value: unknown): number | null {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  return null;
}

function stringifyJson(value: any): string {
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}

function profileErrorMessage(error: unknown): string {
  const raw = error instanceof Error ? error.message : 'Failed to profile dataset';
  const lowered = raw.toLowerCase();
  if (
    lowered.includes('generate/approve yaml first') ||
    (lowered.includes('409') && lowered.includes('contract approval'))
  ) {
    return 'Generate/approve YAML first. Deep profile is available only after contract approval or first completed scan.';
  }
  return raw;
}

export default function ProfileModal({ open, datasetName, onClose }: Props) {
  const [state, setState] = useState<LoadState>({ status: 'idle' });
  const [showAdvanced, setShowAdvanced] = useState(false);

  useEffect(() => {
    if (!open || !datasetName) {
      setState({ status: 'idle' });
      setShowAdvanced(false);
      return;
    }

    let cancelled = false;
    setState({ status: 'loading' });
    void (async () => {
      try {
        const data = await getDatasetProfile(datasetName);
        if (cancelled) return;
        setState({ status: 'ready', data });
      } catch (error) {
        if (cancelled) return;
        setState({ status: 'error', message: profileErrorMessage(error) });
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [datasetName, open]);

  const profile = useMemo<ProfileLike | null>(() => {
    if (state.status !== 'ready') return null;
    if (!state.data || typeof state.data !== 'object') return null;
    return state.data as ProfileLike;
  }, [state]);

  if (!open) return null;

  const name = datasetName || profile?.dataset_name || 'unknown';
  const columnMap = profile?.columns || profile?.column_profiles || {};
  const columnCount = Object.keys(columnMap).length || (asNumber(profile?.total_columns) ?? 0);

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-background/80 backdrop-blur-sm">
      <div className="bg-card rounded-2xl shadow-2xl w-full max-w-5xl max-h-[90vh] flex flex-col overflow-hidden border border-border">
        <div className="p-4 border-b border-border flex justify-between items-center bg-muted/30">
          <div className="flex items-center gap-3">
            <div className="p-2 bg-primary/10 text-primary rounded-lg">
              <Microscope size={18} />
            </div>
            <div>
              <h3 className="font-medium text-foreground">Deep Data Profile</h3>
              <p className="text-xs text-muted-foreground">{name}</p>
            </div>
          </div>
          <button onClick={onClose} className="p-2 rounded-lg hover:bg-accent text-muted-foreground" aria-label="Close">
            <X size={18} />
          </button>
        </div>

        <div className="flex-1 overflow-y-auto p-5 space-y-5">
          {state.status === 'loading' && (
            <div className="p-2 text-sm text-muted-foreground flex items-center gap-2">
              <Loader2 size={18} className="animate-spin" /> Profiling dataset...
            </div>
          )}

          {state.status === 'error' && (
            <div className="rounded-xl border border-border bg-muted/30 p-4">
              <div className="text-sm font-medium text-foreground">Profile failed</div>
              <div className="mt-1 text-xs text-muted-foreground">{state.message}</div>
            </div>
          )}

          {state.status === 'ready' && (
            <>
              <section className="grid gap-3 md:grid-cols-4">
                <article className="rounded-xl border border-border bg-background p-3">
                  <div className="text-xs uppercase tracking-wide text-muted-foreground">Rows Scanned</div>
                  <div className="mt-1 text-2xl font-medium">
                    {asNumber(profile?.total_rows) !== null ? Number(profile?.total_rows).toLocaleString() : 'N/A'}
                  </div>
                </article>
                <article className="rounded-xl border border-border bg-background p-3">
                  <div className="text-xs uppercase tracking-wide text-muted-foreground">Columns</div>
                  <div className="mt-1 text-2xl font-medium">
                    {columnCount || 'N/A'}
                  </div>
                </article>
                <article className="rounded-xl border border-border bg-background p-3">
                  <div className="text-xs uppercase tracking-wide text-muted-foreground">Overall Quality</div>
                  <div className="mt-1 text-2xl font-medium">
                    {asNumber(profile?.overall_quality_score) !== null
                      ? `${Number(profile?.overall_quality_score).toFixed(1)}%`
                      : 'N/A'}
                  </div>
                </article>
                <article className="rounded-xl border border-border bg-background p-3">
                  <div className="text-xs uppercase tracking-wide text-muted-foreground">Memory Usage</div>
                  <div className="mt-1 text-2xl font-medium">
                    {asNumber(profile?.memory_usage_mb) !== null ? `${Number(profile?.memory_usage_mb).toFixed(2)} MB` : 'N/A'}
                  </div>
                </article>
              </section>

              <section className="space-y-3">
                <div className="text-xs uppercase tracking-wide text-muted-foreground">Column Analysis</div>
                {columnMap && Object.keys(columnMap).length > 0 ? (
                  <div className="space-y-2">
                    {Object.entries(columnMap).map(([colName, stats]) => {
                      const quality = asNumber(stats?.quality_score);
                      const qualityText = quality !== null ? `${quality.toFixed(0)}%` : 'N/A';
                      const barWidth = quality !== null ? Math.max(0, Math.min(100, quality)) : 0;
                      return (
                        <div key={colName} className="rounded-xl border border-border bg-background p-3 flex items-center justify-between gap-4">
                          <div className="min-w-0">
                            <div className="flex items-center gap-3">
                              <span className="rounded-lg bg-muted px-2 py-1 text-xs font-mono text-muted-foreground">
                                {stats?.type || 'unknown'}
                              </span>
                              <div className="font-medium truncate">{colName}</div>
                            </div>
                            <div className="mt-1 text-xs text-muted-foreground">
                              Nulls: {asNumber(stats?.null_count) ?? 0} · Unique: {asNumber(stats?.unique_count) ?? 0}
                            </div>
                          </div>

                          <div className="flex items-center gap-4 shrink-0">
                            <div className="text-right">
                              <div className="text-[10px] uppercase tracking-wide text-muted-foreground">Quality</div>
                              <div className="font-medium">{qualityText}</div>
                            </div>
                            <div className="w-32 h-2 bg-muted rounded-full overflow-hidden">
                              <div className="h-full bg-foreground rounded-full" style={{ width: `${barWidth}%` }} />
                            </div>
                          </div>
                        </div>
                      );
                    })}
                  </div>
                ) : (
                  <div className="rounded-xl border border-border bg-muted/20 p-4 text-sm text-muted-foreground">
                    No per-column stats available. Raw profile JSON is shown below.
                  </div>
                )}
              </section>

              <section className="rounded-xl border border-border bg-background overflow-hidden">
                <button
                  onClick={() => setShowAdvanced((prev) => !prev)}
                  className="w-full px-4 py-3 border-b border-border text-sm font-medium flex items-center justify-between hover:bg-muted/30"
                >
                  <span>Advanced · Raw Profile JSON</span>
                  {showAdvanced ? <ChevronDown size={16} /> : <ChevronRight size={16} />}
                </button>
                {showAdvanced && (
                  <pre className="p-4 text-xs overflow-x-auto whitespace-pre-wrap text-muted-foreground">
                    {stringifyJson(state.data)}
                  </pre>
                )}
              </section>
            </>
          )}
        </div>
      </div>
    </div>
  );
}
