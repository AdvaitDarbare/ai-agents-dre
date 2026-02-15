import React, { useState, useEffect } from "react";
import { motion, AnimatePresence } from "framer-motion";
import {
  AlertCircle,
  AlertTriangle,
  ChevronRight,
  Clock,
  Loader2,
  ShieldCheck,
  Filter,
  Database,
} from "lucide-react";
import { getIncidents } from "../api";

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
};

const IncidentFeed = () => {
  const [incidents, setIncidents] = useState([]);
  const [loading, setLoading] = useState(true);
  const [filter, setFilter] = useState("ALL");
  const [expandedId, setExpandedId] = useState(null);

  useEffect(() => {
    const fetch = async () => {
      setLoading(true);
      try {
        const res = await getIncidents(100);
        setIncidents(res.data);
      } catch (e) {
        console.error("Failed to load incidents", e);
      } finally {
        setLoading(false);
      }
    };
    fetch();
  }, []);

  const filtered =
    filter === "ALL"
      ? incidents
      : incidents.filter((inc) => inc.severity === filter);

  const critCount = incidents.filter((i) => i.severity === "CRITICAL").length;
  const warnCount = incidents.filter((i) => i.severity === "WARNING").length;

  if (loading)
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="animate-spin text-slate-300" size={24} />
      </div>
    );

  return (
    <div className="h-full flex flex-col">
      {/* Header */}
      <div className="flex items-center justify-between mb-5">
        <div className="flex items-center gap-3">
          <div className="p-2 bg-rose-100 text-rose-600 rounded-xl">
            <AlertCircle size={20} />
          </div>
          <div>
            <h2 className="text-lg font-black text-slate-800">Incidents</h2>
            <p className="text-xs text-slate-400 font-bold">
              {incidents.length} total — {critCount} critical, {warnCount} warnings
            </p>
          </div>
        </div>
      </div>

      {/* Filter Chips */}
      <div className="flex gap-2 mb-4">
        {[
          { key: "ALL", label: "All", count: incidents.length },
          { key: "CRITICAL", label: "Critical", count: critCount },
          { key: "WARNING", label: "Warning", count: warnCount },
        ].map((f) => (
          <button
            key={f.key}
            onClick={() => setFilter(f.key)}
            className={`px-3 py-1.5 rounded-lg text-[11px] font-bold transition-colors ${filter === f.key
                ? "bg-primary text-white"
                : "bg-slate-100 text-slate-500 hover:bg-slate-200"
              }`}
          >
            {f.label}{" "}
            <span className="opacity-60">({f.count})</span>
          </button>
        ))}
      </div>

      {/* Incident List */}
      <div className="flex-1 overflow-y-auto space-y-2 pr-1">
        {filtered.length === 0 ? (
          <div className="flex flex-col items-center justify-center py-16 text-slate-300">
            <ShieldCheck size={40} className="mb-3" />
            <p className="font-bold text-sm">No incidents</p>
            <p className="text-xs">All systems are running clean.</p>
          </div>
        ) : (
          <AnimatePresence mode="popLayout">
            {filtered.map((inc) => {
              const cfg = severityConfig[inc.severity] || severityConfig.WARNING;
              const Icon = cfg.icon;
              const isExpanded = expandedId === inc.run_id;

              return (
                <motion.div
                  key={inc.run_id}
                  layout
                  initial={{ opacity: 0, y: 10 }}
                  animate={{ opacity: 1, y: 0 }}
                  exit={{ opacity: 0, y: -10 }}
                  className={`rounded-xl border ${cfg.border} ${cfg.bg} overflow-hidden cursor-pointer transition-colors`}
                  onClick={() => setExpandedId(isExpanded ? null : inc.run_id)}
                >
                  <div className="p-4 flex items-start gap-3">
                    <div className={`w-2 h-2 rounded-full mt-1.5 ${cfg.dot}`} />
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center gap-2 mb-1">
                        <span className={`text-[10px] font-black uppercase px-2 py-0.5 rounded-full ${cfg.badge}`}>
                          {inc.severity}
                        </span>
                        <span className="text-[11px] font-bold text-slate-500 flex items-center gap-1">
                          <Database size={10} /> {inc.dataset}
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
                      <p className="text-xs text-slate-600 font-medium truncate">
                        {inc.reason}
                      </p>

                      <AnimatePresence>
                        {isExpanded && (
                          <motion.div
                            initial={{ height: 0, opacity: 0 }}
                            animate={{ height: "auto", opacity: 1 }}
                            exit={{ height: 0, opacity: 0 }}
                            className="mt-3 pt-3 border-t border-slate-200/50"
                          >
                            <div className="grid grid-cols-3 gap-3 text-xs">
                              <div>
                                <div className="text-[9px] font-black uppercase text-slate-400 mb-0.5">
                                  Quality Score
                                </div>
                                <div className="font-black text-slate-700">
                                  {inc.quality_score?.toFixed(1) ?? "N/A"}%
                                </div>
                              </div>
                              <div>
                                <div className="text-[9px] font-black uppercase text-slate-400 mb-0.5">
                                  Anomalies
                                </div>
                                <div className="font-black text-slate-700">
                                  {inc.anomaly_count ?? 0}
                                </div>
                              </div>
                              <div>
                                <div className="text-[9px] font-black uppercase text-slate-400 mb-0.5">
                                  Max Z-Score
                                </div>
                                <div className="font-black text-slate-700">
                                  {inc.z_score_max?.toFixed(2) ?? "N/A"}
                                </div>
                              </div>
                            </div>
                          </motion.div>
                        )}
                      </AnimatePresence>
                    </div>
                    <ChevronRight
                      size={14}
                      className={`text-slate-400 transition-transform mt-1 ${isExpanded ? "rotate-90" : ""
                        }`}
                    />
                  </div>
                </motion.div>
              );
            })}
          </AnimatePresence>
        )}
      </div>
    </div>
  );
};

export default IncidentFeed;
