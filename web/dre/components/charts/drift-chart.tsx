'use client';

import { useEffect, useMemo, useState } from 'react';
import { GitBranch, Loader2 } from 'lucide-react';
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

import { getMetricTimeseries, type MetricTimeseriesResponse } from '@/lib/dre-api';

export default function DriftChart({ datasetName, metricName = 'mean_amount' }: { datasetName: string; metricName?: string }) {
  const [data, setData] = useState<MetricTimeseriesResponse | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    const run = async () => {
      setLoading(true);
      try {
        const next = await getMetricTimeseries(datasetName, metricName, 30);
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
  }, [datasetName, metricName]);

  const chartData = useMemo(
    () =>
      (data?.data || []).map((d) => ({
        time: d.timestamp ? new Date(d.timestamp).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }) : 'n/a',
        value: Number(d.value ?? 0),
        upper: data?.baseline?.upper_2sigma ?? null,
        lower: data?.baseline?.lower_2sigma ?? null,
      })),
    [data],
  );

  if (loading) {
    return (
      <div className="rounded-xl border border-border bg-card p-8 flex items-center justify-center">
        <Loader2 size={20} className="animate-spin text-muted-foreground" />
      </div>
    );
  }

  if (chartData.length === 0) {
    return <div className="rounded-xl border border-border bg-card p-8 text-sm text-muted-foreground">No distribution data for {metricName}.</div>;
  }

  return (
    <section className="rounded-xl border border-border bg-card p-4 md:p-6">
      <div className="flex items-center gap-2 mb-4">
        <GitBranch size={16} />
        <h4 className="text-sm font-medium">Distribution Drift ({metricName})</h4>
      </div>
      <ResponsiveContainer width="100%" height={260}>
        <AreaChart data={chartData}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="time" tick={{ fontSize: 11 }} />
          <YAxis tick={{ fontSize: 11 }} />
          <Tooltip />
          <Area type="monotone" dataKey="upper" stroke="none" fill="#c7d2fe" fillOpacity={0.3} />
          <Area type="monotone" dataKey="lower" stroke="none" fill="#ffffff" fillOpacity={1} />
          <ReferenceLine y={typeof data?.baseline?.mean === 'number' ? data.baseline.mean : undefined} stroke="#6366f1" strokeDasharray="4 4" />
          <Area type="monotone" dataKey="value" stroke="#6366f1" fill="#c7d2fe" fillOpacity={0.2} strokeWidth={2} />
        </AreaChart>
      </ResponsiveContainer>
    </section>
  );
}
