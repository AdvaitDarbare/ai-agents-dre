'use client';

import { useEffect, useMemo, useState } from 'react';
import { Loader2, TrendingUp } from 'lucide-react';
import {
  Area,
  CartesianGrid,
  ComposedChart,
  Line,
  ReferenceLine,
  ResponsiveContainer,
  Scatter,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';

import { getMetricTimeseries, type MetricTimeseriesResponse } from '@/lib/dre-api';

export default function VolumeAnomalyChart({ datasetName }: { datasetName: string }) {
  const [data, setData] = useState<MetricTimeseriesResponse | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    const run = async () => {
      setLoading(true);
      try {
        const next = await getMetricTimeseries(datasetName, 'row_count', 30);
        if (cancelled) return;
        setData(next);
      } finally {
        if (!cancelled) setLoading(false);
      }
    };
    void run();
    return () => {
      cancelled = true;
    };
  }, [datasetName]);

  const chartData = useMemo(() => {
    const baseline = data?.baseline;
    return (data?.data || []).map((d) => {
      const value = Number(d.value ?? 0);
      const anomaly =
        baseline && typeof baseline.mean === 'number' && typeof baseline.std === 'number' && baseline.std > 0
          ? Math.abs(value - baseline.mean) > 3 * baseline.std
          : false;
      return {
        time: d.timestamp ? new Date(d.timestamp).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }) : 'n/a',
        value,
        mean: baseline?.mean ?? null,
        upper: baseline?.upper_3sigma ?? null,
        lower: typeof baseline?.lower_3sigma === 'number' ? Math.max(0, baseline.lower_3sigma) : null,
        anomaly: anomaly ? value : null,
      };
    });
  }, [data]);

  if (loading) {
    return (
      <div className="rounded-xl border border-border bg-card p-8 flex items-center justify-center">
        <Loader2 size={20} className="animate-spin text-muted-foreground" />
      </div>
    );
  }

  if (chartData.length === 0) {
    return <div className="rounded-xl border border-border bg-card p-8 text-sm text-muted-foreground">No volume history available.</div>;
  }

  return (
    <section className="rounded-xl border border-border bg-card p-4 md:p-6">
      <div className="flex items-center gap-2 mb-4">
        <TrendingUp size={16} />
        <h4 className="text-sm font-medium">Volume Anomaly Detection</h4>
      </div>
      <ResponsiveContainer width="100%" height={260}>
        <ComposedChart data={chartData} margin={{ top: 5, right: 10, left: 0, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="time" tick={{ fontSize: 11 }} />
          <YAxis tick={{ fontSize: 11 }} />
          <Tooltip />
          <Area type="monotone" dataKey="upper" stroke="none" fill="#c7d2fe" fillOpacity={0.3} />
          <Area type="monotone" dataKey="lower" stroke="none" fill="#ffffff" fillOpacity={1} />
          <ReferenceLine y={typeof data?.baseline?.mean === 'number' ? data.baseline.mean : undefined} stroke="#6366f1" strokeDasharray="4 4" />
          <Line type="monotone" dataKey="value" stroke="#4f46e5" strokeWidth={2} dot={{ r: 2 }} />
          <Scatter dataKey="anomaly" fill="#ef4444" />
        </ComposedChart>
      </ResponsiveContainer>
    </section>
  );
}
