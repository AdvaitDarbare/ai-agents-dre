import React, { useState, useEffect } from "react";
import {
  ComposedChart,
  Area,
  Line,
  Scatter,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  ReferenceLine,
} from "recharts";
import { Loader2, TrendingUp } from "lucide-react";
import { getMetricTimeseries } from "../../api";

const VolumeAnomalyChart = ({ datasetName }) => {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetch = async () => {
      setLoading(true);
      try {
        const res = await getMetricTimeseries(datasetName, "row_count", 30);
        setData(res.data);
      } catch (e) {
        console.error("Failed to load volume timeseries", e);
      } finally {
        setLoading(false);
      }
    };
    fetch();
  }, [datasetName]);

  if (loading)
    return (
      <div className="flex justify-center p-8">
        <Loader2 className="animate-spin text-slate-300" size={20} />
      </div>
    );

  if (!data?.data?.length)
    return (
      <div className="text-center p-6 text-slate-400 text-xs italic">
        No volume history yet. Run a scan to populate data.
      </div>
    );

  const baseline = data.baseline;
  const chartData = data.data.map((d, i) => {
    const isAnomaly =
      baseline &&
      baseline.std > 0 &&
      Math.abs(d.value - baseline.mean) > 3 * baseline.std;
    const timestamp = new Date(d.timestamp);
    return {
      time: timestamp.toLocaleDateString("en-US", {
        month: "short",
        day: "numeric",
      }),
      fullTime: timestamp.toLocaleString("en-US", {
        month: "short",
        day: "numeric",
        hour: "numeric",
        minute: "2-digit",
        hour12: true,
      }),
      value: d.value,
      upper: baseline ? baseline.upper_3sigma : null,
      lower: baseline ? Math.max(0, baseline.lower_3sigma) : null,
      mean: baseline ? baseline.mean : null,
      anomaly: isAnomaly ? d.value : null,
      idx: i,
    };
  });

  return (
    <div className="bg-white rounded-xl border border-slate-200 p-8">
      <div className="flex items-center gap-4 mb-6">
        <div className="p-2 bg-primary/10 text-primary rounded-lg">
          <TrendingUp size={16} />
        </div>
        <h4 className="text-base font-black uppercase text-slate-600 tracking-wider">
          Volume Anomaly Detection (Statistical)
        </h4>
        {baseline ? (
          <span className="ml-auto text-xs font-bold text-slate-400 bg-slate-100 px-3 py-1 rounded-full">
            Baseline: {baseline.type} ({baseline.sample_count} samples)
          </span>
        ) : (
          <span className="ml-auto text-xs font-bold text-primary bg-primary/10 px-3 py-1 rounded-full border border-primary/20">
            Building baseline... (need 3+ runs)
          </span>
        )}
      </div>
      {!baseline && (
        <div className="bg-primary/5 border border-primary/10 rounded-lg p-4 mb-4">
          <div className="text-sm text-primary/80">
            <strong>Note:</strong> Statistical anomaly detection requires at least 3 historical runs to establish a baseline.
            Currently showing volume history without anomaly detection. Run more scans to enable Z-score based anomaly detection.
          </div>
        </div>
      )}
      <ResponsiveContainer width="100%" height={280}>
        <ComposedChart data={chartData} margin={{ top: 5, right: 10, left: 0, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
          <XAxis dataKey="time" tick={{ fontSize: 12, fill: "#94a3b8" }} />
          <YAxis tick={{ fontSize: 12, fill: "#94a3b8" }} />
          <Tooltip
            contentStyle={{
              fontSize: 13,
              borderRadius: 8,
              border: "1px solid #e2e8f0",
              boxShadow: "0 4px 6px -1px rgba(0,0,0,0.1)",
            }}
            content={({ payload }) => {
              if (!payload || !payload[0]) return null;
              const data = payload[0].payload;
              return (
                <div className="bg-white border border-slate-200 rounded-lg p-4 shadow-lg">
                  <div className="text-sm font-bold text-slate-700 mb-1">
                    {data.fullTime}
                  </div>
                  <div className="text-sm text-slate-600">
                    Row Count: <span className="font-bold text-slate-800">{data.value?.toLocaleString()}</span>
                  </div>
                  {data.anomaly && (
                    <div className="text-sm text-rose-600 font-semibold mt-1">
                      ⚠️ Anomaly Detected
                    </div>
                  )}
                  {baseline && (
                    <>
                      <div className="text-sm text-slate-500 mt-1">
                        Mean: {baseline.mean?.toLocaleString()}
                      </div>
                      <div className="text-sm text-slate-500">
                        Range: {Math.max(0, baseline.lower_3sigma)?.toLocaleString()} - {baseline.upper_3sigma?.toLocaleString()}
                      </div>
                    </>
                  )}
                </div>
              );
            }}
          />
          {baseline && (
            <Area
              type="monotone"
              dataKey="upper"
              stroke="none"
              fill="#c7d2fe"
              fillOpacity={0.4}
            />
          )}
          {baseline && (
            <Area
              type="monotone"
              dataKey="lower"
              stroke="none"
              fill="#ffffff"
              fillOpacity={1}
            />
          )}
          {baseline && (
            <ReferenceLine
              y={baseline.mean}
              stroke="#6366f1"
              strokeDasharray="4 4"
              strokeWidth={1}
              label={{ value: "Mean", position: "right", fontSize: 9, fill: "#6366f1" }}
            />
          )}
          <Line
            type="monotone"
            dataKey="value"
            stroke="#4f46e5"
            strokeWidth={2}
            dot={{ r: 3, fill: "#4f46e5" }}
            activeDot={{ r: 5 }}
          />
          <Scatter
            dataKey="anomaly"
            fill="#ef4444"
            shape="star"
            r={6}
          />
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  );
};

export default VolumeAnomalyChart;
