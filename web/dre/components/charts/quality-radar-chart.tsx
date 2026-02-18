'use client';

import { useEffect, useMemo, useState } from 'react';
import { AlertCircle, Hexagon, Loader2 } from 'lucide-react';
import {
  PolarAngleAxis,
  PolarGrid,
  PolarRadiusAxis,
  Radar,
  RadarChart,
  ResponsiveContainer,
  Tooltip,
} from 'recharts';

import { getQualityDimensions } from '@/lib/dre-api';
import QualityDimensionEvidenceModal from '@/dre/components/charts/quality-dimension-evidence-modal';

type DimensionRow = {
  name: string;
  score: number;
  status?: string;
  weight?: number;
  check_count?: { passed?: number; total?: number };
  violations?: string[];
};

type QualityDimensionsResponse = {
  overall_score?: number;
  remediation_status?: string;
  dimensions?: DimensionRow[];
};

function statusClass(status?: string): string {
  const s = String(status || '').toUpperCase();
  if (s === 'PASS') return 'text-emerald-700 bg-emerald-50';
  if (s === 'WARN') return 'text-amber-700 bg-amber-50';
  if (s === 'FAIL') return 'text-rose-700 bg-rose-50';
  return 'text-muted-foreground bg-muted';
}

function scoreAccent(score: number): string {
  if (score >= 90) return 'text-emerald-700';
  if (score >= 75) return 'text-amber-700';
  return 'text-rose-700';
}

function cardTone(status?: string): string {
  const s = String(status || '').toUpperCase();
  if (s === 'PASS') return 'border-emerald-200 bg-emerald-50/40';
  if (s === 'WARN') return 'border-amber-200 bg-amber-50/40';
  if (s === 'FAIL') return 'border-rose-200 bg-rose-50/40';
  return 'border-border bg-background';
}

export default function QualityRadarChart({ datasetName }: { datasetName: string }) {
  const [data, setData] = useState<QualityDimensionsResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedDimension, setSelectedDimension] = useState<DimensionRow | null>(null);

  useEffect(() => {
    let cancelled = false;
    const run = async () => {
      setLoading(true);
      setError(null);
      try {
        const next = (await getQualityDimensions(datasetName)) as QualityDimensionsResponse;
        if (cancelled) return;
        setData(next);
      } catch (e) {
        if (cancelled) return;
        setError(e instanceof Error ? e.message : 'Failed to load quality dimensions');
      } finally {
        if (!cancelled) setLoading(false);
      }
    };
    void run();
    return () => {
      cancelled = true;
    };
  }, [datasetName]);

  const radarData = useMemo(() => {
    return (data?.dimensions || []).map((d) => ({
      dimension: d.name,
      score: typeof d.score === 'number' ? d.score : 0,
      fullMark: 100,
      status: d.status || 'UNKNOWN',
      weight: d.weight ?? 0,
      checkCount: d.check_count || {},
      violations: d.violations || [],
    }));
  }, [data?.dimensions]);

  if (loading) {
    return (
      <div className="rounded-xl border border-border bg-card p-8 flex items-center justify-center">
        <Loader2 size={20} className="animate-spin text-muted-foreground" />
      </div>
    );
  }

  if (error) {
    return (
      <div className="rounded-xl border border-border bg-card p-8 text-center">
        <AlertCircle size={24} className="mx-auto text-rose-600" />
        <div className="mt-2 text-sm text-muted-foreground">{error}</div>
      </div>
    );
  }

  if (radarData.length === 0) {
    return <div className="rounded-xl border border-border bg-card p-8 text-sm text-muted-foreground">No quality dimensions available.</div>;
  }

  const overall = typeof data?.overall_score === 'number' ? data.overall_score : null;

  return (
    <section className="rounded-xl border border-border bg-card p-4 md:p-6">
      <div className="flex items-center justify-between gap-3 mb-4">
        <div className="flex items-center gap-2">
          <Hexagon size={16} className="text-indigo-600" />
          <h4 className="text-sm font-medium">6-Dimensional Quality Framework</h4>
        </div>
        <div className="text-right">
          <div className="text-[10px] uppercase tracking-wide text-muted-foreground">Overall</div>
          <div className={`text-xl font-semibold ${overall !== null ? scoreAccent(overall) : ''}`}>
            {overall !== null ? `${overall.toFixed(1)}%` : 'N/A'}
          </div>
        </div>
      </div>

      <div className="grid gap-4 xl:grid-cols-5">
        <div className="xl:col-span-3 rounded-lg border border-indigo-100 bg-gradient-to-b from-indigo-50/50 to-background p-2">
          <ResponsiveContainer width="100%" height={320}>
            <RadarChart data={radarData}>
              <PolarGrid stroke="#c7d2fe" />
              <PolarAngleAxis dataKey="dimension" tick={{ fill: 'hsl(var(--muted-foreground))', fontSize: 11 }} />
              <PolarRadiusAxis angle={90} domain={[0, 100]} tick={{ fill: 'hsl(var(--muted-foreground))', fontSize: 10 }} tickCount={6} />
              <Radar dataKey="score" stroke="#4f46e5" fill="#6366f1" fillOpacity={0.28} strokeWidth={2.5} />
              <Tooltip />
            </RadarChart>
          </ResponsiveContainer>
        </div>

        <div className="xl:col-span-2 space-y-2">
          {radarData.map((d) => (
            <button
              key={d.dimension}
              type="button"
              onClick={() =>
                setSelectedDimension({
                  name: d.dimension,
                  score: d.score,
                  status: String(d.status || 'UNKNOWN'),
                  weight: Number(d.weight || 0),
                  check_count: {
                    passed: Number(d.checkCount.passed || 0),
                    total: Number(d.checkCount.total || 0),
                  },
                  violations: Array.isArray(d.violations) ? d.violations : [],
                })
              }
              className={`w-full text-left rounded-lg border p-3 transition-colors hover:bg-accent/40 ${cardTone(d.status)}`}
            >
              <div className="flex items-center justify-between gap-2">
                <div className="text-sm font-medium">{d.dimension}</div>
                <span className={`rounded-full px-2 py-0.5 text-[10px] font-semibold ${statusClass(d.status)}`}>{d.status}</span>
              </div>
              <div className="mt-1 text-xs">
                <span className={`font-semibold ${scoreAccent(d.score)}`}>Score {d.score.toFixed(1)}%</span>
                <span className="text-muted-foreground"> · Weight {(Number(d.weight) * 100).toFixed(0)}% · Checks {d.checkCount.passed || 0}/{d.checkCount.total || 0}</span>
              </div>
              {Array.isArray(d.violations) && d.violations.length > 0 && (
                <div className="mt-2 text-xs text-rose-700">Violations: {d.violations.length}</div>
              )}
              <div className="mt-2 text-[11px] text-muted-foreground">Click to inspect failing rows and diagnostics evidence.</div>
            </button>
          ))}
        </div>
      </div>

      <QualityDimensionEvidenceModal
        open={Boolean(selectedDimension)}
        datasetName={datasetName}
        dimension={selectedDimension}
        onClose={() => setSelectedDimension(null)}
      />
    </section>
  );
}
