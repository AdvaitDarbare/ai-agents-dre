import React, { useState, useEffect } from "react";
import { Loader2, BarChart3 } from "lucide-react";
import { getDatasetProfile } from "../../api";

const getBarColor = (score) => {
  if (score >= 80) return "bg-emerald-500";
  if (score >= 51) return "bg-amber-400";
  return "bg-rose-500";
};

const getBadgeColor = (score) => {
  if (score >= 80) return "text-emerald-600 bg-emerald-50";
  if (score >= 51) return "text-amber-600 bg-amber-50";
  return "text-rose-600 bg-rose-50";
};

const ColumnQualityBars = ({ datasetName }) => {
  const [profile, setProfile] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetch = async () => {
      setLoading(true);
      try {
        const res = await getDatasetProfile(datasetName);
        setProfile(res.data);
      } catch (e) {
        console.error("Failed to load profile", e);
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

  const columns = profile?.column_profiles;
  if (!columns || Object.keys(columns).length === 0)
    return (
      <div className="text-center p-6 text-slate-400 text-sm italic">
        No column profile data available.
      </div>
    );

  const sorted = Object.entries(columns).sort(
    (a, b) => a[1].quality_score - b[1].quality_score
  );

  return (
    <div className="bg-white rounded-xl border border-slate-200 p-8">
      <div className="flex items-center gap-4 mb-6">
        <div className="p-2 bg-emerald-50 text-emerald-500 rounded-lg">
          <BarChart3 size={16} />
        </div>
        <h4 className="text-base font-black uppercase text-slate-600 tracking-wider">
          Column Quality Scores
        </h4>
        <span className="ml-auto text-xs font-bold text-slate-400 bg-slate-100 px-3 py-1 rounded-full">
          Overall: {profile.overall_quality_score?.toFixed(1)}%
        </span>
      </div>
      <div className="space-y-2.5">
        {sorted.map(([colName, col]) => {
          const score = col.quality_score ?? 100;
          return (
            <div key={colName} className="flex items-center gap-3">
              <div className="w-28 text-sm font-bold text-slate-600 truncate" title={colName}>
                {colName}
              </div>
              <div className="flex-1 h-5 bg-slate-100 rounded-full overflow-hidden relative">
                <div
                  className={`h-full rounded-full transition-all duration-500 ${getBarColor(score)}`}
                  style={{ width: `${Math.max(score, 2)}%` }}
                />
              </div>
              <span
                className={`text-xs font-black px-3 py-1 rounded-full min-w-[48px] text-center ${getBadgeColor(score)}`}
              >
                {score.toFixed(0)}%
              </span>
            </div>
          );
        })}
      </div>
      {profile.constraint_violations?.length > 0 && (
        <div className="mt-4 pt-3 border-t border-slate-100">
          <div className="text-sm font-black uppercase text-rose-500 mb-2">
            Constraint Violations ({profile.constraint_violations.length})
          </div>
          {profile.constraint_violations.slice(0, 5).map((v, i) => (
            <div key={i} className="text-sm text-slate-500 mb-1">
              <span className="font-bold text-slate-600">{v.column || v.check_name}:</span>{" "}
              {v.message || v.details || JSON.stringify(v)}
            </div>
          ))}
        </div>
      )}
    </div>
  );
};

export default ColumnQualityBars;
