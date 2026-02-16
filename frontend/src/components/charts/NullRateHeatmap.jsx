import React, { useState, useEffect } from "react";
import { Loader2, Grid3x3 } from "lucide-react";
import { getDatasetProfile } from "../../api";

const getCellColor = (rate) => {
  if (rate === null || rate === undefined) return "bg-slate-100 text-slate-400";
  if (rate === 0) return "bg-emerald-50 text-emerald-600";
  if (rate < 0.01) return "bg-emerald-100 text-emerald-700";
  if (rate < 0.05) return "bg-primary/5 text-primary";
  if (rate < 0.1) return "bg-primary/10 text-primary";
  if (rate < 0.25) return "bg-amber-100 text-amber-700";
  return "bg-rose-100 text-rose-700";
};

const NullRateHeatmap = ({ datasetName }) => {
  const [columnProfiles, setColumnProfiles] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetch = async () => {
      setLoading(true);
      try {
        const res = await getDatasetProfile(datasetName);
        setColumnProfiles(res.data?.column_profiles || []);
      } catch (e) {
        console.error("Failed to load profile data", e);
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

  if (!columnProfiles.length)
    return (
      <div className="text-center p-6 text-slate-400 text-sm italic">
        No column profile data available yet.
      </div>
    );

  return (
    <div className="bg-white rounded-xl border border-slate-200 p-8">
      <div className="flex items-center gap-4 mb-6">
        <div className="p-2 bg-primary/10 text-primary rounded-lg">
          <Grid3x3 size={16} />
        </div>
        <h4 className="text-base font-black uppercase text-slate-600 tracking-wider">
          Null Rate Overview
        </h4>
      </div>

      <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 gap-2">
        {columnProfiles.map((col) => {
          const rate = col.null_count / (col.null_count + col.non_null_count);
          return (
            <div
              key={col.column_name}
              className={`rounded-lg p-4 text-center transition-colors ${getCellColor(rate)}`}
              title={`${col.column_name}: ${(rate * 100).toFixed(2)}% null rate (${col.null_count} nulls / ${col.null_count + col.non_null_count} total)`}
            >
              <div className="text-xs font-black uppercase truncate mb-1">
                {col.column_name}
              </div>
              <div className="text-base font-black">
                {(rate * 100).toFixed(1)}%
              </div>
              <div className="text-xs opacity-60 mt-0.5">
                {col.null_count} nulls
              </div>
            </div>
          );
        })}
      </div>

      {/* Legend */}
      <div className="flex items-center gap-2 mt-4 pt-3 border-t border-slate-100">
        <span className="text-xs font-bold text-slate-400">Null Rate:</span>
        {[
          { label: "0%", cls: "bg-emerald-50" },
          { label: "<1%", cls: "bg-emerald-100" },
          { label: "<5%", cls: "bg-primary/5" },
          { label: "<10%", cls: "bg-primary/10" },
          { label: "<25%", cls: "bg-amber-100" },
          { label: "25%+", cls: "bg-rose-100" },
        ].map((l) => (
          <div key={l.label} className="flex items-center gap-1">
            <div className={`w-3 h-3 rounded ${l.cls}`} />
            <span className="text-xs text-slate-400">{l.label}</span>
          </div>
        ))}
      </div>
    </div>
  );
};

export default NullRateHeatmap;
