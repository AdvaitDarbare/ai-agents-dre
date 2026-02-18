'use client';

import { useEffect, useMemo, useState } from 'react';
import { Grid3x3, Loader2 } from 'lucide-react';

import { ApiError, getDatasetProfile } from '@/lib/dre-api';

type ColumnProfile = {
  null_count?: number;
  non_null_count?: number;
  total_rows?: number;
  null_rate?: number;
};

type ProfileLike = {
  columns?: Record<string, ColumnProfile>;
  column_profiles?: Record<string, ColumnProfile>;
};

function cellClass(rate: number | null): string {
  if (rate === null) return 'bg-muted text-muted-foreground';
  if (rate === 0) return 'bg-emerald-50 text-emerald-700';
  if (rate < 0.01) return 'bg-emerald-100 text-emerald-800';
  if (rate < 0.05) return 'bg-sky-100 text-sky-800';
  if (rate < 0.1) return 'bg-amber-100 text-amber-800';
  if (rate < 0.25) return 'bg-orange-100 text-orange-800';
  return 'bg-rose-100 text-rose-800';
}

export default function NullRateHeatmap({ datasetName }: { datasetName: string }) {
  const [profile, setProfile] = useState<ProfileLike | null>(null);
  const [loading, setLoading] = useState(true);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    const run = async () => {
      setLoading(true);
      setErrorMessage(null);
      try {
        const next = (await getDatasetProfile(datasetName)) as ProfileLike;
        if (cancelled) return;
        setProfile(next);
      } catch (error) {
        if (cancelled) return;
        setProfile(null);
        if (error instanceof ApiError && error.status === 404) {
          setErrorMessage('No active data file was found for this dataset. Re-upload data or run a new scan.');
        } else if (error instanceof ApiError && error.status === 409) {
          setErrorMessage('Generate or approve a contract first to enable deep profiling.');
        } else {
          setErrorMessage(error instanceof Error ? error.message : 'Failed to load profile data.');
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    };
    void run();
    return () => {
      cancelled = true;
    };
  }, [datasetName]);

  const rows = useMemo(() => {
    const source = profile?.columns || profile?.column_profiles || {};
    return Object.entries(source).map(([name, c]) => {
      const n = Number(c?.null_count ?? 0);
      const explicitRate = typeof c?.null_rate === 'number' ? Number(c.null_rate) : null;
      const totalRows = Number(c?.total_rows ?? 0);
      const nn = Number(c?.non_null_count ?? 0);
      const derivedTotal = totalRows > 0 ? totalRows : n + nn;
      const rate = explicitRate !== null ? explicitRate : derivedTotal > 0 ? n / derivedTotal : null;
      const total = derivedTotal > 0 ? derivedTotal : null;
      return { name, nullCount: n, total, rate };
    });
  }, [profile?.column_profiles, profile?.columns]);

  if (loading) {
    return (
      <div className="rounded-xl border border-border bg-card p-8 flex items-center justify-center">
        <Loader2 size={20} className="animate-spin text-muted-foreground" />
      </div>
    );
  }

  if (errorMessage) {
    return <div className="rounded-xl border border-border bg-card p-8 text-sm text-muted-foreground">{errorMessage}</div>;
  }

  if (rows.length === 0) {
    return <div className="rounded-xl border border-border bg-card p-8 text-sm text-muted-foreground">No null-rate data available.</div>;
  }

  return (
    <section className="rounded-xl border border-border bg-card p-4 md:p-6">
      <div className="flex items-center gap-2 mb-4">
        <Grid3x3 size={16} />
        <h4 className="text-sm font-medium">Null Rate Overview</h4>
      </div>

      <div className="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-4 gap-2">
        {rows.map((r) => (
          <div key={r.name} className={`rounded-lg p-3 ${cellClass(r.rate)}`} title={`${r.name}: ${r.rate !== null ? (r.rate * 100).toFixed(2) : 'N/A'}% null`}>
            <div className="text-[10px] uppercase tracking-wide font-semibold truncate">{r.name}</div>
            <div className="mt-1 text-sm font-semibold">{r.rate !== null ? `${(r.rate * 100).toFixed(1)}%` : 'N/A'}</div>
            <div className="text-[10px] opacity-80">{r.nullCount} nulls</div>
          </div>
        ))}
      </div>
    </section>
  );
}
