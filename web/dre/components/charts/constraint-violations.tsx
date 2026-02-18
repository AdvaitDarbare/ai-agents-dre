'use client';

import { useEffect, useState } from 'react';
import { AlertCircle, AlertTriangle, Loader2 } from 'lucide-react';

import { getDatasetProfile } from '@/lib/dre-api';

type Violation = {
  type?: string;
  severity?: string;
  message?: string;
  expected?: any;
  actual?: any;
  column?: string;
};

type ProfileLike = {
  constraint_violations?: Violation[];
};

export default function ConstraintViolations({ datasetName }: { datasetName: string }) {
  const [rows, setRows] = useState<Violation[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    const run = async () => {
      setLoading(true);
      try {
        const profile = (await getDatasetProfile(datasetName)) as ProfileLike;
        if (cancelled) return;
        setRows(Array.isArray(profile?.constraint_violations) ? profile.constraint_violations : []);
      } finally {
        if (!cancelled) setLoading(false);
      }
    };
    void run();
    return () => {
      cancelled = true;
    };
  }, [datasetName]);

  if (loading) {
    return (
      <div className="rounded-xl border border-border bg-card p-8 flex items-center justify-center">
        <Loader2 size={20} className="animate-spin text-muted-foreground" />
      </div>
    );
  }

  if (rows.length === 0) {
    return (
      <div className="rounded-xl border border-border bg-card p-6">
        <div className="flex items-center gap-2 text-emerald-700 text-sm font-medium">
          <AlertCircle size={16} /> No schema constraint violations detected.
        </div>
      </div>
    );
  }

  return (
    <section className="rounded-xl border border-border bg-card overflow-hidden">
      <div className="px-4 py-3 border-b border-border text-sm font-medium flex items-center gap-2">
        <AlertTriangle size={14} /> Schema Constraint Violations
      </div>
      <div className="p-4 space-y-2">
        {rows.map((v, idx) => (
          <article key={idx} className="rounded-lg border border-rose-200 bg-rose-50 p-3">
            <div className="text-xs uppercase tracking-wide text-rose-700 font-semibold">{String(v.type || 'VIOLATION').replace(/_/g, ' ')}</div>
            <div className="mt-1 text-sm text-rose-900">{v.message || 'Constraint violation'}</div>
            <div className="mt-2 text-xs text-rose-800">
              {v.column ? `Column: ${v.column}` : null}
              {v.expected !== undefined ? ` · Expected: ${String(v.expected)}` : null}
              {v.actual !== undefined ? ` · Actual: ${String(v.actual)}` : null}
            </div>
          </article>
        ))}
      </div>
    </section>
  );
}
