'use client';

import { useEffect, useMemo, useState } from 'react';
import { BarChart3, Loader2 } from 'lucide-react';

import { ApiError, getDatasetProfile } from '@/lib/dre-api';

type ColumnProfile = {
  quality_score?: number;
  null_count?: number;
  non_null_count?: number;
  unique_count?: number;
  contract_nullable?: boolean | null;
  null_policy?: {
    enabled?: boolean;
    penalized?: boolean;
    penalty_pct?: number;
    explanation?: string;
  };
};

type ProfileLike = {
  overall_quality_score?: number;
  columns?: Record<string, ColumnProfile>;
  column_profiles?: Record<string, ColumnProfile>;
  constraint_violations?: any[];
};

function barColor(score: number): string {
  if (score >= 80) return 'bg-emerald-500';
  if (score >= 51) return 'bg-amber-500';
  return 'bg-rose-500';
}

export default function ColumnQualityBars({ datasetName }: { datasetName: string }) {
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

  const columns = useMemo(() => {
    const source = profile?.columns || profile?.column_profiles || {};
    return Object.entries(source)
      .map(([name, stats]) => {
        const score = Number(stats?.quality_score ?? 100);
        const contractNullable = stats?.contract_nullable;
        const policy = stats?.null_policy;
        const policyPenalized = Boolean(policy?.enabled && policy?.penalized);
        const allowedByYaml = contractNullable !== false;
        const showPolicyNote = policyPenalized && allowedByYaml;
        const penaltyPct = typeof policy?.penalty_pct === 'number' ? Number(policy.penalty_pct) : null;
        const note =
          showPolicyNote && penaltyPct !== null
            ? `Nulls allowed by YAML (nullable=true), but completeness policy penalized this score by ${penaltyPct.toFixed(1)} points.`
            : showPolicyNote
              ? 'Nulls allowed by YAML (nullable=true), but completeness policy penalized this score.'
              : null;
        return { name, score, note };
      })
      .sort((a, b) => a.score - b.score);
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

  if (columns.length === 0) {
    return <div className="rounded-xl border border-border bg-card p-8 text-sm text-muted-foreground">No column quality data available.</div>;
  }

  return (
    <section className="rounded-xl border border-border bg-card p-4 md:p-6">
      <div className="flex items-center gap-2 mb-4">
        <BarChart3 size={16} />
        <h4 className="text-sm font-medium">Column Quality Scores</h4>
      </div>
      <div className="space-y-2">
        {columns.map((col) => (
          <div key={col.name} className="flex items-center gap-3">
            <div className="w-36 truncate text-xs font-medium" title={col.name}>
              {col.name}
            </div>
            <div className="h-4 flex-1 rounded-full bg-muted overflow-hidden">
              <div className={`h-full ${barColor(col.score)}`} style={{ width: `${Math.max(Math.min(col.score, 100), 2)}%` }} />
            </div>
            <div className="w-12 text-right text-xs font-semibold">{col.score.toFixed(0)}%</div>
          </div>
        ))}
      </div>

      {columns.some((c) => Boolean(c.note)) && (
        <div className="mt-3 text-[11px] text-muted-foreground">
          {columns
            .filter((c) => Boolean(c.note))
            .slice(0, 4)
            .map((c) => (
              <div key={`note-${c.name}`}>
                <span className="font-medium">{c.name}:</span> {c.note}
              </div>
            ))}
        </div>
      )}
    </section>
  );
}
