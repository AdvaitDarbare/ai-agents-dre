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
import { Loader2, GitBranch } from "lucide-react";
import { getMetricTimeseries } from "../../api";

const DriftChart = ({ datasetName, metricName = "mean_amount" }) => {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetch = async () => {
      setLoading(true);
      try {
        const res = await getMetricTimeseries(datasetName, metricName, 30);
        setData(res.data);
      } catch (e) {
        console.error("Failed to load drift data", e);
      } finally {
        setLoading(false);
      }
    };
    fetch();
  }, [datasetName, metricName]);

  if (loading)
    return (
      <div className="flex justify-center p-8">
        <Loader2 className="animate-spin text-slate-300" size={20} />
      </div>
    );

  if (!data?.data?.length)
    return (
      <div className="text-center p-6 text-slate-400 text-sm italic">
        No distribution data available for {metricName}.
      </div>
    );

  const baseline = data.baseline;
  const chartData = data.data.map((d) => ({
    time: new Date(d.timestamp).toLocaleDateString("en-US", {
      month: "short",
      day: "numeric",
    }),
    value: d.value,
    upper: baseline ? baseline.upper_2sigma : null,
    lower: baseline ? baseline.lower_2sigma : null,
  }));

  return (
    <div className="bg-white rounded-xl border border-slate-200 p-8">
      <div className="flex items-center gap-4 mb-6">
        <div className="p-2 bg-primary/10 text-primary rounded-lg">
          <GitBranch size={16} />
        </div>
        <h4 className="text-base font-black uppercase text-slate-600 tracking-wider">
          Distribution Drift — {metricName.replace("mean_", "")}
        </h4>
        {baseline && (
          <span className="ml-auto text-xs font-bold text-slate-400 bg-slate-100 px-3 py-1 rounded-full">
            Mean: {baseline.mean?.toFixed(2)}
          </span>
        )}
      </div>
      <ResponsiveContainer width="100%" height={280}>
        <AreaChart data={chartData} margin={{ top: 5, right: 10, left: 0, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
          <XAxis dataKey="time" tick={{ fontSize: 12, fill: "#94a3b8" }} />
          <YAxis tick={{ fontSize: 12, fill: "#94a3b8" }} />
          <Tooltip
            contentStyle={{
              fontSize: 13,
              borderRadius: 8,
              border: "1px solid #e2e8f0",
            }}
          />
          {baseline && (
            <>
              <Area type="monotone" dataKey="upper" stroke="none" fill="#c7d2fe" fillOpacity={0.5} />
              <Area type="monotone" dataKey="lower" stroke="none" fill="#ffffff" fillOpacity={1} />
              <ReferenceLine
                y={baseline.mean}
                stroke="#6366f1"
                strokeDasharray="4 4"
                strokeWidth={1}
              />
            </>
          )}
          <Area
            type="monotone"
            dataKey="value"
            stroke="#6366f1"
            fill="#c7d2fe"
            fillOpacity={0.3}
            strokeWidth={2}
          />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
};

export default DriftChart;
