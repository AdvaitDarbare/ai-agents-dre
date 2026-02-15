import React, { useState, useEffect } from "react";
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  ReferenceLine,
} from "recharts";
import { Loader2, ShieldCheck } from "lucide-react";
import { getHistory } from "../../api";

const QualityScoreTrend = ({ datasetName }) => {
  const [history, setHistory] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetch = async () => {
      setLoading(true);
      try {
        const res = await getHistory(datasetName);
        setHistory(res.data);
      } catch (e) {
        console.error("Failed to load quality trend", e);
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

  if (!history?.length)
    return (
      <div className="text-center p-6 text-slate-400 text-sm italic">
        No quality score history available.
      </div>
    );

  const chartData = [...history].reverse().map((h) => ({
    time: new Date(h.timestamp).toLocaleDateString("en-US", {
      month: "short",
      day: "numeric",
    }),
    score: h.quality_score ?? 0,
    status: h.status,
  }));

  const latest = chartData[chartData.length - 1];
  const getScoreColor = (s) => (s >= 80 ? "#10b981" : s >= 50 ? "#f59e0b" : "#ef4444");

  return (
    <div className="bg-white rounded-xl border border-slate-200 p-8">
      <div className="flex items-center gap-4 mb-6">
        <div className="p-2 bg-primary/10 text-primary rounded-lg">
          <ShieldCheck size={16} />
        </div>
        <h4 className="text-base font-black uppercase text-slate-600 tracking-wider">
          Quality Score Trend
        </h4>
        {latest && (
          <span
            className="ml-auto text-base font-black"
            style={{ color: getScoreColor(latest.score) }}
          >
            {latest.score.toFixed(1)}%
          </span>
        )}
      </div>
      <ResponsiveContainer width="100%" height={280}>
        <AreaChart data={chartData} margin={{ top: 5, right: 10, left: 0, bottom: 0 }}>
          <defs>
            <linearGradient id="qualityGradient" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor="#6366f1" stopOpacity={0.2} />
              <stop offset="95%" stopColor="#6366f1" stopOpacity={0} />
            </linearGradient>
          </defs>
          <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
          <XAxis dataKey="time" tick={{ fontSize: 12, fill: "#94a3b8" }} />
          <YAxis domain={[0, 100]} tick={{ fontSize: 12, fill: "#94a3b8" }} />
          <Tooltip
            contentStyle={{
              fontSize: 13,
              borderRadius: 8,
              border: "1px solid #e2e8f0",
            }}
            formatter={(v) => [`${v.toFixed(1)}%`, "Quality"]}
          />
          <ReferenceLine
            y={80}
            stroke="#f59e0b"
            strokeDasharray="4 4"
            strokeWidth={1}
            label={{ value: "Warning (80%)", position: "right", fontSize: 9, fill: "#f59e0b" }}
          />
          <ReferenceLine
            y={50}
            stroke="#ef4444"
            strokeDasharray="4 4"
            strokeWidth={1}
            label={{ value: "Block (50%)", position: "right", fontSize: 9, fill: "#ef4444" }}
          />
          <Area
            type="monotone"
            dataKey="score"
            stroke="#6366f1"
            fill="url(#qualityGradient)"
            strokeWidth={2}
            dot={{ r: 3, fill: "#6366f1" }}
          />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
};

export default QualityScoreTrend;
