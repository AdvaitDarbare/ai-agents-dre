import React, { useState, useEffect } from "react";
import { motion, AnimatePresence } from "framer-motion";
import {
  AlertCircle,
  AlertTriangle,
  ChevronRight,
  ChevronDown,
  Clock,
  Loader2,
  ShieldCheck,
  Filter,
  Database,
} from "lucide-react";
import { getIncidents, getRecentRuns } from "../api";

const severityConfig = {
  CRITICAL: {
    icon: AlertCircle,
    bg: "bg-rose-50",
    border: "border-rose-200",
    badge: "bg-rose-100 text-rose-700",
    dot: "bg-rose-500",
    text: "text-rose-700",
  },
  WARNING: {
    icon: AlertTriangle,
    bg: "bg-amber-50",
    border: "border-amber-200",
    badge: "bg-amber-100 text-amber-700",
    dot: "bg-amber-500",
    text: "text-amber-700",
  },
  PASSED: {
    icon: ShieldCheck,
    bg: "bg-emerald-50",
    border: "border-emerald-200",
    badge: "bg-emerald-100 text-emerald-700",
    dot: "bg-emerald-500",
    text: "text-emerald-700",
  },
};

const ActivityFeed = () => {
  const [viewMode, setViewMode] = useState("incidents"); // 'incidents' | 'history'
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [filter, setFilter] = useState("ALL");
  const [expandedGroups, setExpandedGroups] = useState(new Set());

  // Data fetching based on viewMode
  useEffect(() => {
    const fetch = async () => {
      setLoading(true);
      try {
        let res;
        if (viewMode === "incidents") {
          res = await getIncidents(100);
          const uniqueDatasets = new Set(res.data.map((i) => i.dataset));
          setExpandedGroups(uniqueDatasets);
        } else {
          res = await getRecentRuns(100);
          setExpandedGroups(new Set()); // Reset grouping for linear history
        }
        setData(res.data || []);
      } catch (e) {
        console.error("Failed to load activity data", e);
        setData([]);
      } finally {
        setLoading(false);
      }
    };
    fetch();
  }, [viewMode]);

  const toggleGroup = (dataset) => {
    const next = new Set(expandedGroups);
    if (next.has(dataset)) {
      next.delete(dataset);
    } else {
      next.add(dataset);
    }
    setExpandedGroups(next);
  };

  // --- Filtering Logic ---
  const filteredData =
    filter === "ALL"
      ? data
      : data.filter((item) => {
        if (viewMode === "incidents") return item.severity === filter;
        // For history, map status to filter keys if needed, or disable filter
        // keeping simplifed for now: History usually shows everything, or we can filter by PASS/FAIL
        if (filter === "CRITICAL") return item.status === "BLOCKED" || item.status === "CRITICAL";
        if (filter === "WARNING") return item.status === "WARNING";
        return true;
      });

  // --- Grouping Logic (Incidents Only) ---
  const groupedIncidents = (() => {
    if (viewMode !== "incidents") return [];

    const groups = filteredData.reduce((acc, inc) => {
      if (!acc[inc.dataset]) acc[inc.dataset] = [];
      acc[inc.dataset].push(inc);
      return acc;
    }, {});

    return Object.entries(groups)
      .map(([dataset, items]) => {
        const critCount = items.filter((i) => i.severity === "CRITICAL").length;
        const recentTs = Math.max(...items.map((i) => new Date(i.timestamp).getTime()));
        return { dataset, items, critCount, recentTs };
      })
      .sort((a, b) => {
        if (a.critCount > 0 && b.critCount === 0) return -1;
        if (b.critCount > 0 && a.critCount === 0) return 1;
        return b.recentTs - a.recentTs;
      });
  })();

  const critCount = data.filter((i) => i.severity === "CRITICAL" || i.status === "BLOCKED").length;
  const warnCount = data.filter((i) => i.severity === "WARNING" || i.status === "WARNING").length;

  if (loading)
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="animate-spin text-slate-300" size={24} />
      </div>
    );

  return (
    <div className="h-full flex flex-col">
      {/* Header & Toggle */}
      <div className="flex items-center justify-between mb-5">
        <div className="flex items-center gap-3">
          <div className={`p-2 rounded-xl transition-colors ${viewMode === 'incidents' ? 'bg-rose-100 text-rose-600' : 'bg-slate-100 text-slate-600'}`}>
            {viewMode === 'incidents' ? <AlertCircle size={20} /> : <History size={20} />}
          </div>
          <div>
            <h2 className="text-lg font-black text-slate-800">
              {viewMode === "incidents" ? "Active Incidents" : "Run History"}
            </h2>
            <p className="text-xs text-slate-400 font-bold">
              {viewMode === "incidents"
                ? `${data.length} open issues — ${critCount} critical`
                : "Recent execution logs across all datasets"}
            </p>
          </div>
        </div>

        {/* View Toggle */}
        <div className="flex bg-slate-100 p-1 rounded-lg">
          <button
            onClick={() => setViewMode("incidents")}
            className={`px-3 py-1.5 rounded-md text-xs font-bold transition-all ${viewMode === "incidents" ? "bg-white text-slate-900 shadow-sm" : "text-slate-500 hover:text-slate-700"
              }`}
          >
            Incidents
          </button>
          <button
            onClick={() => setViewMode("history")}
            className={`px-3 py-1.5 rounded-md text-xs font-bold transition-all ${viewMode === "history" ? "bg-white text-slate-900 shadow-sm" : "text-slate-500 hover:text-slate-700"
              }`}
          >
            All History
          </button>
        </div>
      </div>

      {/* Filter Chips (Only for Incident View mostly, but can apply to history if needed) */}
      <div className="flex gap-2 mb-6">
        {[
          { key: "ALL", label: "All", count: data.length },
          { key: "CRITICAL", label: "Critical/Blocked", count: critCount },
          { key: "WARNING", label: "Warning", count: warnCount },
        ].map((f) => (
          <button
            key={f.key}
            onClick={() => setFilter(f.key)}
            className={`px-3 py-1.5 rounded-lg text-[11px] font-bold transition-colors ${filter === f.key
              ? "bg-slate-900 text-white"
              : "bg-slate-100 text-slate-500 hover:bg-slate-200"
              }`}
          >
            {f.label}{" "}
            {viewMode === "incidents" && <span className="opacity-60">({f.count})</span>}
          </button>
        ))}
      </div>

      {/* Content Area */}
      <div className="flex-1 overflow-y-auto space-y-6 pr-1 custom-scrollbar">

        {/* VIEW: INCIDENTS (Grouped) */}
        {viewMode === "incidents" && (
          sortedGroups.length === 0 ? (
            <div className="flex flex-col items-center justify-center py-16 text-slate-300">
              <ShieldCheck size={40} className="mb-3" />
              <p className="font-bold text-sm">No incidents found</p>
            </div>
          ) : (
            sortedGroups.map((group) => (
              <div key={group.dataset} className="animate-in fade-in slide-in-from-bottom-2 duration-500">
                {/* ... Existing Group Header Code ... */}
                <button
                  onClick={() => toggleGroup(group.dataset)}
                  className="w-full flex items-center justify-between group mb-3 hover:bg-slate-50 p-2 rounded-lg transition-colors -mx-2"
                >
                  <div className="flex items-center gap-3">
                    <div className={`p-1.5 rounded-lg ${group.critCount > 0 ? "bg-rose-100 text-rose-600" : "bg-slate-100 text-slate-500"}`}>
                      <Database size={16} />
                    </div>
                    <div className="text-left">
                      <h3 className="font-bold text-slate-800 text-sm">{group.dataset}</h3>
                      <p className="text-[11px] text-slate-400 font-bold">
                        {group.items.length} Issue{group.items.length !== 1 ? "s" : ""}
                      </p>
                    </div>
                  </div>
                  <div className="flex items-center gap-2">
                    {group.critCount > 0 && (
                      <span className="px-2 py-0.5 bg-rose-100 text-rose-700 text-[10px] font-black rounded-full uppercase tracking-wider">
                        {group.critCount} Critical
                      </span>
                    )}
                    <ChevronDown
                      size={16}
                      className={`text-slate-300 transition-transform duration-200 ${expandedGroups.has(group.dataset) ? "rotate-180" : ""
                        }`}
                    />
                  </div>
                </button>

                <AnimatePresence>
                  {expandedGroups.has(group.dataset) && (
                    <motion.div
                      initial={{ height: 0, opacity: 0 }}
                      animate={{ height: "auto", opacity: 1 }}
                      exit={{ height: 0, opacity: 0 }}
                      className="space-y-2 pl-4 border-l-2 border-slate-100 ml-3"
                    >
                      {group.items.map((inc) => {
                        const cfg = severityConfig[inc.severity] || severityConfig.WARNING;

                        return (
                          <div
                            key={inc.run_id}
                            className={`rounded-xl border ${cfg.border} ${cfg.bg} p-4 transition-all hover:shadow-sm`}
                          >
                            {/* ... Existing Card Code ... */}
                            <div className="flex items-start gap-3">
                              <div className={`w-2 h-2 rounded-full mt-1.5 ${cfg.dot}`} />
                              <div className="flex-1 min-w-0">
                                <div className="flex items-center gap-2 mb-1">
                                  <span className={`text-[10px] font-black uppercase px-2 py-0.5 rounded-full ${cfg.badge}`}>
                                    {inc.severity}
                                  </span>
                                  <span className="ml-auto text-[10px] text-slate-400 flex items-center gap-1">
                                    <Clock size={10} />
                                    {inc.timestamp
                                      ? new Date(inc.timestamp).toLocaleString("en-US", {
                                        month: "short",
                                        day: "numeric",
                                        hour: "2-digit",
                                        minute: "2-digit",
                                      })
                                      : "Unknown"}
                                  </span>
                                </div>
                                <p className="text-xs text-slate-700 font-medium leading-relaxed">
                                  {inc.reason}
                                </p>

                                {/* Stats Grid */}
                                <div className="grid grid-cols-3 gap-2 mt-3 pt-3 border-t border-black/5">
                                  <div>
                                    <div className="text-[9px] font-black uppercase text-slate-400 mb-0.5">Score</div>
                                    <div className="font-bold text-slate-700 text-xs">{inc.quality_score?.toFixed(1) ?? "N/A"}%</div>
                                  </div>
                                  <div>
                                    <div className="text-[9px] font-black uppercase text-slate-400 mb-0.5">Anomalies</div>
                                    <div className="font-bold text-slate-700 text-xs">{inc.anomaly_count ?? 0}</div>
                                  </div>
                                  <div>
                                    <div className="text-[9px] font-black uppercase text-slate-400 mb-0.5">Max Z</div>
                                    <div className="font-bold text-slate-700 text-xs">{inc.z_score_max?.toFixed(2) ?? "N/A"}</div>
                                  </div>
                                </div>
                              </div>
                            </div>
                          </div>
                        );
                      })}
                    </motion.div>
                  )}
                </AnimatePresence>
              </div>
            ))
          )
        )}

        {/* VIEW: HISTORY (Linear) */}
        {viewMode === "history" && (
          filteredData.length === 0 ? (
            <div className="text-center py-10 text-slate-400 text-sm">No history found.</div>
          ) : (
            <div className="space-y-3">
              {filteredData.map((run) => {
                const status = run.status || "UNKNOWN";
                // Map run status to config keys
                let severityKey = "WARNING";
                if (status === "PASSED") severityKey = "PASSED";
                if (status === "BLOCKED" || status === "CRITICAL") severityKey = "CRITICAL";

                const cfg = severityConfig[severityKey] || severityConfig.WARNING;

                return (
                  <div key={run.id || run.run_id} className="p-4 bg-white border border-slate-200 rounded-xl hover:shadow-sm transition-all flex items-center gap-4">
                    <div className={`w-2 h-2 rounded-full ${cfg.dot}`} />
                    <div className="flex-1">
                      <div className="flex items-center gap-2 mb-1">
                        <span className="font-bold text-slate-800 text-sm">{run.dataset}</span>
                        <span className={`text-[10px] font-black uppercase px-2 py-0.5 rounded-full ${cfg.badge}`}>
                          {status}
                        </span>
                      </div>
                      <p className="text-xs text-slate-500 truncate">{run.reason || "No details provided"}</p>
                    </div>
                    <div className="text-right">
                      <div className="text-[10px] text-slate-400 font-mono mb-0.5">
                        {run.date === "Today" ? run.time : run.date}
                      </div>
                      <div className={`text-xs font-bold ${run.quality_score >= 90 ? "text-emerald-600" : "text-amber-500"}`}>
                        {run.quality_score ? `${run.quality_score.toFixed(1)}%` : "-"}
                      </div>
                    </div>
                  </div>
                )
              })}
            </div>
          )
        )}
      </div>
    </div>
  );
};

export default ActivityFeed;
