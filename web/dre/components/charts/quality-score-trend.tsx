'use client';

import { useEffect, useMemo, useState } from 'react';
import { Loader2, ShieldCheck } from 'lucide-react';
import {
  Area,
  AreaChart,
  CartesianGrid,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';

import { getHistory, type DatasetRunHistoryItem } from '@/lib/dre-api';

export default function QualityScoreTrend({ datasetName }: { datasetName: string }) {
  const [history, setHistory] = useState<DatasetRunHistoryItem[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    const run = async () => {
      setLoading(true);
      try {
        const next = await getHistory(datasetName, 30);
        if (cancelled) return;
        setHistory(next);
      } finally {
        if (!cancelled) setLoading(false);
      }
    };
    void run();
    return () => {
      cancelled = true;
    };
  }, [datasetName]);

  const chartData = useMemo(
    () =>
      [...history].reverse().map((h) => ({
        time: h.timestamp
          ? new Date(h.timestamp).toLocaleDateString('en-US', { month: 'short', day: 'numeric' })
          : 'n/a',
        score: Number(h.quality_score ?? 0),
      })),
    [history],
  );

  if (loading) {
    return (
      <div className="rounded-xl border border-border bg-card p-8 flex items-center justify-center">
        <Loader2 size={20} className="animate-spin text-muted-foreground" />
      </div>
    );
  }

  if (chartData.length === 0) {
    return <div className="rounded-xl border border-border bg-card p-8 text-sm text-muted-foreground">No quality trend history available.</div>;
  }

  return (
    <section className="rounded-xl border border-border bg-card p-4 md:p-6">
      <div className="flex items-center gap-2 mb-4">
        <ShieldCheck size={16} />
        <h4 className="text-sm font-medium">Quality Score Trend</h4>
      </div>
      <ResponsiveContainer width="100%" height={260}>
        <AreaChart data={chartData} margin={{ top: 5, right: 10, left: 0, bottom: 0 }}>
          <defs>
            <linearGradient id="qualityGradientNext" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor="hsl(var(--foreground))" stopOpacity={0.2} />
              <stop offset="95%" stopColor="hsl(var(--foreground))" stopOpacity={0} />
            </linearGradient>
          </defs>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="time" tick={{ fontSize: 11 }} />
          <YAxis domain={[0, 100]} tick={{ fontSize: 11 }} />
          <Tooltip formatter={(v: number) => [`${v.toFixed(1)}%`, 'Quality']} />
          <ReferenceLine y={80} stroke="#f59e0b" strokeDasharray="4 4" />
          <ReferenceLine y={50} stroke="#ef4444" strokeDasharray="4 4" />
          <Area type="monotone" dataKey="score" stroke="hsl(var(--foreground))" fill="url(#qualityGradientNext)" strokeWidth={2} />
        </AreaChart>
      </ResponsiveContainer>
    </section>
  );
}
