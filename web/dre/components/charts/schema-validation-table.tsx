'use client';

import { useEffect, useMemo, useState } from 'react';
import { CheckCircle2, Loader2, Table, XCircle } from 'lucide-react';

import { getContractContent, getContractYaml, getDatasetProfile } from '@/lib/dre-api';

type ColumnContract = {
  name: string;
  expectedType?: string;
  nullable?: boolean;
};

type ProfileLike = {
  columns?: Record<string, any>;
  column_profiles?: Record<string, any>;
};

function parseColumnsFromYaml(yaml: string): ColumnContract[] {
  if (!yaml) return [];
  const out: ColumnContract[] = [];
  const parts = yaml.split(/\n-\s*name:\s*/).slice(1);
  for (const part of parts) {
    const lines = part.split('\n');
    const name = lines[0]?.trim();
    if (!name) continue;
    const expectedType = part.match(/data_type:\s*([^\n]+)/)?.[1]?.trim();
    const nullableRaw = part.match(/nullable:\s*([^\n]+)/)?.[1]?.trim();
    out.push({
      name,
      expectedType,
      nullable: nullableRaw ? nullableRaw === 'true' : undefined,
    });
  }
  return out;
}

export default function SchemaValidationTable({ datasetName }: { datasetName: string }) {
  const [loading, setLoading] = useState(true);
  const [contractYaml, setContractYaml] = useState('');
  const [profile, setProfile] = useState<ProfileLike | null>(null);

  useEffect(() => {
    let cancelled = false;
    const run = async () => {
      setLoading(true);
      try {
        const [yamlRes, profileRes] = await Promise.all([
          getContractYaml(datasetName).catch(async () => {
            const fallback = await getContractContent(datasetName);
            return { yaml_content: fallback.content };
          }),
          getDatasetProfile(datasetName).catch(() => ({})),
        ]);
        if (cancelled) return;
        setContractYaml(String(yamlRes?.yaml_content || ''));
        setProfile((profileRes || {}) as ProfileLike);
      } finally {
        if (!cancelled) setLoading(false);
      }
    };
    void run();
    return () => {
      cancelled = true;
    };
  }, [datasetName]);

  const contractColumns = useMemo(() => parseColumnsFromYaml(contractYaml), [contractYaml]);
  const profileColumns = useMemo(() => profile?.columns || profile?.column_profiles || {}, [profile?.column_profiles, profile?.columns]);

  if (loading) {
    return (
      <div className="rounded-xl border border-border bg-card p-8 flex items-center justify-center">
        <Loader2 size={20} className="animate-spin text-muted-foreground" />
      </div>
    );
  }

  if (contractColumns.length === 0) {
    return <div className="rounded-xl border border-border bg-card p-8 text-sm text-muted-foreground">No schema contract found for this dataset.</div>;
  }

  return (
    <section className="rounded-xl border border-border bg-card overflow-hidden">
      <div className="px-4 py-3 border-b border-border text-sm font-medium flex items-center gap-2">
        <Table size={14} /> Schema Validation Detail
      </div>
      <div className="overflow-x-auto">
        <table className="min-w-full text-sm">
          <thead className="bg-muted/30 text-muted-foreground">
            <tr>
              <th className="text-left px-4 py-3">Column</th>
              <th className="text-left px-4 py-3">Expected Type</th>
              <th className="text-left px-4 py-3">Nullable</th>
              <th className="text-left px-4 py-3">Observed</th>
              <th className="text-left px-4 py-3">Status</th>
            </tr>
          </thead>
          <tbody>
            {contractColumns.map((col) => {
              const observed = profileColumns[col.name] || null;
              const observedType = observed?.type || observed?.data_type || null;
              const hasTypeMismatch = observedType && col.expectedType && String(observedType) !== String(col.expectedType);
              return (
                <tr key={col.name} className="border-t border-border/60">
                  <td className="px-4 py-3 font-medium">{col.name}</td>
                  <td className="px-4 py-3 text-muted-foreground">{col.expectedType || 'unknown'}</td>
                  <td className="px-4 py-3 text-muted-foreground">
                    {typeof col.nullable === 'boolean' ? (col.nullable ? 'Yes' : 'No') : 'n/a'}
                  </td>
                  <td className="px-4 py-3 text-muted-foreground">
                    {observed ? (
                      <span>
                        {observedType || 'unknown'} · nulls {Number(observed.null_count ?? 0).toLocaleString()} · unique{' '}
                        {Number(observed.unique_count ?? 0).toLocaleString()}
                      </span>
                    ) : (
                      'No profile data'
                    )}
                  </td>
                  <td className="px-4 py-3">
                    {hasTypeMismatch ? (
                      <span className="inline-flex items-center gap-1 text-rose-700 text-xs font-semibold">
                        <XCircle size={14} /> Mismatch
                      </span>
                    ) : (
                      <span className="inline-flex items-center gap-1 text-emerald-700 text-xs font-semibold">
                        <CheckCircle2 size={14} /> OK
                      </span>
                    )}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </section>
  );
}
