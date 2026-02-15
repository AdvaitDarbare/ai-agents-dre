import React, { useState, useEffect } from "react";
import { Radar, RadarChart, PolarGrid, PolarAngleAxis, PolarRadiusAxis, ResponsiveContainer, Tooltip } from "recharts";
import { Loader2, Hexagon, AlertCircle, ChevronDown, ChevronRight } from "lucide-react";
import axios from "axios";

const getStatusColor = (status) => {
  switch (status) {
    case "PASS": return "text-emerald-600";
    case "WARN": return "text-amber-600";
    case "FAIL": return "text-rose-600";
    default: return "text-slate-600";
  }
};

const getStatusBgColor = (status) => {
  switch (status) {
    case "PASS": return "bg-emerald-50";
    case "WARN": return "bg-amber-50";
    case "FAIL": return "bg-rose-50";
    default: return "bg-slate-50";
  }
};

const QualityRadarChart = ({ datasetName }) => {
  const [dimensionData, setDimensionData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [selectedDimension, setSelectedDimension] = useState(null);

  useEffect(() => {
    const fetchDimensions = async () => {
      setLoading(true);
      setError(null);
      try {
        const response = await axios.get(`http://localhost:8000/quality-dimensions/${datasetName}`);
        setDimensionData(response.data);
      } catch (err) {
        console.error("Failed to load quality dimensions", err);
        setError(err.response?.data?.detail || "Failed to load quality dimensions");
      } finally {
        setLoading(false);
      }
    };

    if (datasetName) {
      fetchDimensions();
    }
  }, [datasetName]);

  if (loading) {
    return (
      <div className="bg-white rounded-xl border border-slate-200 p-12">
        <div className="flex justify-center items-center">
          <Loader2 className="animate-spin text-slate-300" size={32} />
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-white rounded-xl border border-slate-200 p-8">
        <div className="flex flex-col items-center justify-center text-center">
          <AlertCircle className="text-rose-400 mb-3" size={40} />
          <p className="text-sm text-slate-500">{error}</p>
        </div>
      </div>
    );
  }

  if (!dimensionData || !dimensionData.dimensions) {
    return (
      <div className="bg-white rounded-xl border border-slate-200 p-8">
        <div className="text-center text-slate-400 text-base italic">
          No dimension data available.
        </div>
      </div>
    );
  }

  // Transform data for Recharts Radar
  const radarData = dimensionData.dimensions.map(d => ({
    dimension: d.name,
    score: d.score,
    fullMark: 100,
    status: d.status,
    weight: d.weight,
    checkCount: d.check_count,
    violations: d.violations
  }));

  const handleDimensionClick = (dimension) => {
    setSelectedDimension(selectedDimension?.dimension === dimension.dimension ? null : dimension);
  };

  return (
    <div className="bg-white rounded-xl border border-slate-200 p-8 shadow-sm">
      {/* Header */}
      <div className="flex items-center justify-between mb-8">
        <div className="flex items-center gap-4">
          <div className="p-2 bg-primary/10 text-primary rounded-lg">
            <Hexagon size={20} />
          </div>
          <div>
            <h3 className="text-base font-black uppercase text-slate-700 tracking-wider">
              6-Dimensional Quality Framework
            </h3>
            <p className="text-sm text-slate-500 mt-0.5">
              Industry-standard data quality assessment
            </p>
          </div>
        </div>
        <div className="flex items-center gap-3">
          <div className="text-right">
            <div className="text-sm font-semibold text-slate-500 uppercase tracking-wide">Overall Score</div>
            <div className="text-3xl font-black text-slate-700 mt-1">
              {dimensionData.overall_score.toFixed(1)}%
            </div>
          </div>
          <div className={`px-4 py-2 rounded-lg font-bold text-base ${dimensionData.overall_score >= 95 ? "bg-emerald-50 text-emerald-600" :
              dimensionData.overall_score >= 80 ? "bg-amber-50 text-amber-600" :
                "bg-rose-50 text-rose-600"
            }`}>
            {dimensionData.overall_score >= 95 ? "EXCELLENT" :
              dimensionData.overall_score >= 80 ? "GOOD" :
                dimensionData.overall_score >= 60 ? "FAIR" : "POOR"}
          </div>
        </div>
      </div>

      <div className="grid grid-cols-5 gap-8">
        {/* Left: Radar Chart - Takes 3 columns */}
        <div className="col-span-3 flex items-center justify-center bg-slate-50 rounded-xl p-8">
          <ResponsiveContainer width="100%" height={450}>
            <RadarChart data={radarData}>
              <PolarGrid stroke="#cbd5e1" strokeWidth={1.5} />
              <PolarAngleAxis
                dataKey="dimension"
                tick={{ fill: '#475569', fontSize: 13, fontWeight: 700 }}
              />
              <PolarRadiusAxis
                angle={90}
                domain={[0, 100]}
                tick={{ fill: '#94a3b8', fontSize: 11, fontWeight: 600 }}
                tickCount={6}
              />
              <Radar
                name="Quality Score"
                dataKey="score"
                stroke="#6366f1"
                fill="#6366f1"
                fillOpacity={0.4}
                strokeWidth={3}
              />
              <Tooltip
                content={({ payload }) => {
                  if (!payload || !payload[0]) return null;
                  const data = payload[0].payload;
                  return (
                    <div className="bg-white border-2 border-slate-200 rounded-lg p-4 shadow-xl">
                      <div className="text-base font-black text-slate-800 mb-2">
                        {data.dimension}
                      </div>
                      <div className="space-y-1">
                        <div className="text-sm text-slate-600">
                          Score: <span className="font-bold text-slate-800">{data.score.toFixed(1)}%</span>
                        </div>
                        <div className="text-sm text-slate-600">
                          Weight: <span className="font-bold text-slate-800">{(data.weight * 100).toFixed(0)}%</span>
                        </div>
                        <div className="text-sm text-slate-600">
                          Status: <span className={`font-bold ${getStatusColor(data.status)}`}>
                            {data.status}
                          </span>
                        </div>
                        <div className="text-sm text-slate-600">
                          Checks: <span className="font-bold text-slate-800">
                            {data.checkCount.passed}/{data.checkCount.total} passed
                          </span>
                        </div>
                      </div>
                    </div>
                  );
                }}
              />
            </RadarChart>
          </ResponsiveContainer>
        </div>

        {/* Right: Dimension Details - Takes 2 columns */}
        <div className="col-span-2 space-y-3">
          {dimensionData.dimensions.map((dim, idx) => (
            <div
              key={idx}
              onClick={() => handleDimensionClick(radarData[idx])}
              className={`p-4 rounded-lg border-2 cursor-pointer transition-all ${selectedDimension?.dimension === dim.name
                  ? 'border-primary/50 bg-primary/10 shadow-md'
                  : 'border-slate-200 hover:border-slate-300 bg-white hover:shadow-sm'
                }`}
            >
              <div className="flex items-center justify-between mb-2">
                <div className="flex items-center gap-2">
                  <span className="text-base font-black text-slate-800">
                    {dim.name}
                  </span>
                  <span className={`text-sm font-bold px-3 py-1 rounded ${getStatusBgColor(dim.status)} ${getStatusColor(dim.status)}`}>
                    {dim.status}
                  </span>
                </div>
                <div className="flex items-center gap-2">
                  <span className="text-2xl font-black text-slate-700">
                    {dim.score.toFixed(0)}%
                  </span>
                  {dim.violations && dim.violations.length > 0 ? (
                    selectedDimension?.dimension === dim.name ?
                      <ChevronDown size={18} className="text-slate-400" /> :
                      <ChevronRight size={18} className="text-slate-400" />
                  ) : null}
                </div>
              </div>

              <div className="flex items-center gap-3 text-sm text-slate-500">
                <span className="font-semibold">Weight: {(dim.weight * 100).toFixed(0)}%</span>
                <span>•</span>
                <span className="font-semibold">
                  {dim.check_count.passed}/{dim.check_count.total} checks passed
                </span>
              </div>

              {selectedDimension?.dimension === dim.name && dim.violations && dim.violations.length > 0 && (
                <div className="mt-3 pt-3 border-t-2 border-slate-200">
                  <div className="text-sm font-bold uppercase text-rose-600 mb-2 flex items-center gap-1">
                    <AlertCircle size={16} />
                    Violations ({dim.violations.length})
                  </div>
                  <div className="space-y-1.5">
                    {dim.violations.map((violation, vIdx) => (
                      <div key={vIdx} className="text-sm text-slate-700 bg-rose-50 rounded px-3 py-2 border border-rose-200">
                        • {violation}
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          ))}
        </div>
      </div>

      {/* Remediation Status */}
      {dimensionData.remediation_status !== "NO_ACTION_NEEDED" && (
        <div className="mt-6 pt-6 border-t-2 border-slate-200">
          <div className={`text-base font-bold px-5 py-3 rounded-lg inline-flex items-center gap-2 ${dimensionData.remediation_status === "OPEN_INCIDENT"
              ? "bg-rose-50 text-rose-600 border-2 border-rose-200"
              : "bg-amber-50 text-amber-600 border-2 border-amber-200"
            }`}>
            <AlertCircle size={20} />
            {dimensionData.remediation_status === "OPEN_INCIDENT"
              ? "Open Incident - Remediation Required"
              : "Under Monitoring"}
          </div>
        </div>
      )}
    </div>
  );
};

export default QualityRadarChart;
