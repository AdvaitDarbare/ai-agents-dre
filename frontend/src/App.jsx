import React, { useState, useEffect, useRef } from "react";
import {
  LayoutDashboard,
  Database,
  Sparkles,
  Activity,
  History,
  Settings,
  MessageSquare,
  ShieldCheck,
  AlertCircle,
  Clock,
  Zap,
  Send,
  Loader2,
  Link,
  ChevronRight,
  Filter,
  Download,
  Search,
  CheckCircle2,
  XCircle,
  AlertTriangle,
  Code,
  Table,
  Users,
  Share2,
  ArrowRight,
  FileText,
  Stethoscope,
  Microscope,
  ChevronLeft,
  Network,
  Plus,
  Server,
  Sun,
  Moon,
} from "lucide-react";
import { motion, AnimatePresence } from "framer-motion";
import {
  LineChart,
  Line,
  ResponsiveContainer,
  YAxis,
  Tooltip,
  AreaChart,
  Area,
  XAxis,
  CartesianGrid,
} from "recharts";
import {
  getPulse,
  evaluateDataset,
  chatWithCopilot,
  getDatasets,
  getLineage,
  getSystemHealth,
  getDatasetProfile,
  getRemediationPlan,
  getRecentRuns,
  getGlobalStats,
  getDatasetMetrics,
  getHistory,
  applyRemediation,
  getGovernanceHistory,
  getHistoricalFile,
  rollbackSchema,
  getContract,
  proposeContract,
  saveContract,
  getIncidents,
  getMetricTimeseries,
  getBaselines,
} from "./api";

// Chart Components
import VolumeAnomalyChart from "./components/charts/VolumeAnomalyChart";
import DriftChart from "./components/charts/DriftChart";
import ColumnQualityBars from "./components/charts/ColumnQualityBars";
import NullRateHeatmap from "./components/charts/NullRateHeatmap";
import QualityScoreTrend from "./components/charts/QualityScoreTrend";
import SchemaValidationTable from "./components/charts/SchemaValidationTable";
import QualityRadarChart from "./components/charts/QualityRadarChart";
import ConstraintViolations from "./components/charts/ConstraintViolations";
import IncidentFeed from "./components/IncidentFeed";
import ContractGovernance from "./components/ContractGovernance";

// --- Modal: Data Profile ---
const ProfileModal = ({ isOpen, onClose, data }) => {
  if (!isOpen || !data) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-background/80 backdrop-blur-sm">
      <motion.div
        initial={{ opacity: 0, scale: 0.95 }}
        animate={{ opacity: 1, scale: 1 }}
        exit={{ opacity: 0, scale: 0.95 }}
        className="bg-card rounded-2xl shadow-2xl w-full max-w-5xl max-h-[90vh] flex flex-col overflow-hidden"
      >
        <div className="p-6 border-b border-border flex justify-between items-center bg-muted/50">
          <div className="flex items-center gap-3">
            <div className="p-2 bg-primary/10 text-primary rounded-lg">
              <Microscope size={24} />
            </div>
            <div>
              <h3 className="text-lg font-black text-foreground">
                Deep Data Profile
              </h3>
              <p className="text-xs text-muted-foreground font-bold">
                Comprehensive analysis for {data.dataset_name}
              </p>
            </div>
          </div>
          <button
            onClick={onClose}
            className="p-2 hover:bg-muted rounded-full text-muted-foreground/80"
          >
            <XCircle size={24} />
          </button>
        </div>

        <div className="flex-1 overflow-y-auto p-8 custom-scrollbar">
          {/* Summary Stats */}
          <div className="grid grid-cols-4 gap-4 mb-8">
            <div className="p-4 bg-muted/50 rounded-xl border border-border">
              <div className="text-xs font-bold text-muted-foreground/80 uppercase tracking-wider mb-1">
                Rows Scanned
              </div>
              <div className="text-2xl font-black text-foreground">
                {data.total_rows?.toLocaleString()}
              </div>
            </div>
            <div className="p-4 bg-muted/50 rounded-xl border border-border">
              <div className="text-xs font-bold text-muted-foreground/80 uppercase tracking-wider mb-1">
                Columns
              </div>
              <div className="text-2xl font-black text-foreground">
                {Object.keys(data.columns || {}).length}
              </div>
            </div>
            <div className="p-4 bg-muted/50 rounded-xl border border-border">
              <div className="text-xs font-bold text-muted-foreground/80 uppercase tracking-wider mb-1">
                Overall Quality
              </div>
              <div
                className={`text-2xl font-black ${data.overall_quality_score >= 90 ? "text-green-500" : "text-amber-500"} `}
              >
                {data.overall_quality_score?.toFixed(1)}%
              </div>
            </div>
            <div className="p-4 bg-muted/50 rounded-xl border border-border">
              <div className="text-xs font-bold text-muted-foreground/80 uppercase tracking-wider mb-1">
                Memory Usage
              </div>
              <div className="text-2xl font-black text-foreground">
                {data.memory_usage_mb?.toFixed(2)} MB
              </div>
            </div>
          </div>

          <h4 className="text-sm font-black uppercase text-muted-foreground/80 tracking-widest mb-4">
            Column Analysis
          </h4>
          <div className="space-y-3">
            {Object.entries(data.columns || {}).map(([colName, stats]) => (
              <div
                key={colName}
                className="p-4 rounded-xl border border-border hover:border-primary/20 transition-colors bg-card shadow-sm flex items-center justify-between"
              >
                <div className="flex items-center gap-4">
                  <div className="p-2 bg-muted rounded-lg text-muted-foreground font-mono text-xs font-bold">
                    {stats.type}
                  </div>
                  <div>
                    <div className="font-bold text-foreground">{colName}</div>
                    <div className="flex gap-3 text-xs text-muted-foreground mt-1 font-medium">
                      <span>Nulls: {stats.null_count}</span>
                      <span>•</span>
                      <span>Unique: {stats.unique_count}</span>
                    </div>
                  </div>
                </div>

                <div className="flex items-center gap-6">
                  <div className="text-right">
                    <div className="text-[10px] font-bold text-muted-foreground/80 uppercase">
                      Quality
                    </div>
                    <div
                      className={`font-black ${stats.quality_score >= 90 ? "text-green-500" : "text-rose-500"} `}
                    >
                      {stats.quality_score?.toFixed(0)}%
                    </div>
                  </div>
                  <div className="w-32 h-2 bg-muted rounded-full overflow-hidden">
                    <div
                      className={`h-full rounded-full ${stats.quality_score >= 90 ? "bg-green-500" : "bg-rose-500"} `}
                      style={{ width: `${stats.quality_score}% ` }}
                    />
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      </motion.div>
    </div>
  );
};

// --- Shared Components ---
const StatCard = ({ title, value, subtext, icon: Icon, color }) => (
  <div className="bg-card p-8 rounded-2xl border border-border shadow-soft hover:shadow-md transition-all group flex flex-col justify-between">
    <div className="flex justify-between items-start">
      <div>
        <p className="text-sm font-black text-muted-foreground uppercase tracking-widest">
          {title}
        </p>
        <h3 className="text-4xl font-black mt-3 text-foreground tracking-tight">
          {value}
        </h3>
      </div>
      <div
        className={`p-4 rounded-xl ${color} bg-opacity-10 group-hover: scale-110 transition-transform duration-300`}
      >
        <Icon className={`w-7 h-7 ${color.replace("bg-", "text-")} `} />
      </div>
    </div>
    <div className="mt-5 flex items-center gap-2">
      <div className="w-2 h-2 rounded-full bg-green-500" />
      <p className="text-sm font-bold text-muted-foreground">{subtext}</p>
    </div>
  </div>
);

// --- Modal: Raw JSON Viewer ---
const JsonViewerModal = ({ isOpen, onClose, data, title }) => {
  if (!isOpen) return null;
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-background/80 backdrop-blur-sm">
      <div className="bg-card rounded-xl shadow-2xl w-full max-w-4xl max-h-[80vh] flex flex-col overflow-hidden">
        <div className="p-4 border-b border-border flex justify-between items-center bg-muted/50">
          <h3 className="font-bold text-foreground/90">
            {title || "Raw JSON Data"}
          </h3>
          <button onClick={onClose}>
            <XCircle
              size={20}
              className="text-muted-foreground/80 hover:text-muted-foreground"
            />
          </button>
        </div>
        <div className="flex-1 overflow-auto p-4 bg-zinc-950/90">
          <pre className="text-xs font-mono text-emerald-400 whitespace-pre-wrap">
            {JSON.stringify(data, null, 2)}
          </pre>
        </div>
      </div>
    </div>
  );
};

// --- Component: Run Timeline ---
const RunTimeline = ({ history }) => {
  const data = [...history].reverse();
  return (
    <div className="h-48 mb-6 bg-card rounded-2xl border border-border p-4">
      <h4 className="text-xs font-black text-muted-foreground/80 uppercase tracking-widest mb-4">
        Run Quality Timeline
      </h4>
      <ResponsiveContainer width="100%" height="100%">
        <AreaChart data={data}>
          <defs>
            <linearGradient id="colorQuality" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor="#10b981" stopOpacity={0.2} />
              <stop offset="95%" stopColor="#10b981" stopOpacity={0} />
            </linearGradient>
            <linearGradient id="colorQualityFail" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor="#f43f5e" stopOpacity={0.2} />
              <stop offset="95%" stopColor="#f43f5e" stopOpacity={0} />
            </linearGradient>
          </defs>
          <CartesianGrid
            strokeDasharray="3 3"
            vertical={false}
            stroke="hsl(var(--border))"
          />
          <XAxis
            dataKey="timestamp"
            tickFormatter={(t) =>
              new Date(t).toLocaleTimeString([], {
                hour: "2-digit",
                minute: "2-digit",
              })
            }
            stroke="hsl(var(--muted-foreground))"
            fontSize={10}
            tickLine={false}
            axisLine={false}
          />
          <YAxis domain={[0, 100]} hide />
          <Tooltip
            contentStyle={{
              borderRadius: "12px",
              border: "none",
              boxShadow: "0 4px 6px -1px rgb(0 0 0 / 0.1)",
            }}
            labelFormatter={(t) => new Date(t).toLocaleString()}
          />
          <Area
            type="monotone"
            dataKey="quality_score"
            stroke="#10b981"
            fillOpacity={1}
            fill="url(#colorQuality)"
            strokeWidth={3}
          />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
};

// --- Tab: History Log ---
const HistoryTab = ({ datasets, history = [] }) => {
  const [selectedRun, setSelectedRun] = useState(null);
  const [jsonModalOpen, setJsonModalOpen] = useState(false);

  // If no history, show empty state
  if (!history || history.length === 0) {
    return (
      <div className="h-full flex flex-col items-center justify-center text-center p-20 bg-card rounded-3xl border border-dashed border-border">
        <History size={48} className="text-muted-foreground/40 mb-4" />
        <h4 className="font-bold text-muted-foreground">No Run History Found</h4>
        <p className="text-xs text-muted-foreground/80 mt-1">
          Run a scan to generate history logs.
        </p>
      </div>
    );
  }

  return (
    <div className="flex flex-col h-full">
      <RunTimeline history={history} />
      <div className="flex gap-8 flex-1 overflow-hidden">
        <JsonViewerModal
          isOpen={jsonModalOpen}
          onClose={() => setJsonModalOpen(false)}
          data={selectedRun}
          title="Run Verification Result"
        />

        <div className="w-1/3 border-r pr-6 space-y-3 overflow-y-auto custom-scrollbar">
          {history.map((run, idx) => (
            <div
              key={idx}
              onClick={() => setSelectedRun(run)}
              className={`p-4 rounded-2xl cursor-pointer transition-all border ${selectedRun === run
                ? "bg-primary/10 border-primary"
                : "bg-card border-border hover:border-primary/50"
                }`}
            >
              <div className="flex justify-between items-center mb-2">
                <span
                  className={`px-2 py-0.5 rounded-full text-[10px] font-black uppercase tracking-wider ${run.status === "PASSED"
                    ? "bg-emerald-100 text-emerald-700"
                    : "bg-rose-100 text-rose-700"
                    }`}
                >
                  {run.status}
                </span>
                <span className="text-[10px] font-mono text-muted-foreground/80">
                  {new Date(run.timestamp).toLocaleTimeString()}
                </span>
              </div>
              <div className="flex justify-between items-center">
                <p className="text-xs font-medium text-muted-foreground line-clamp-1">
                  {run.reason}
                </p>
                {run.quality_score && (
                  <span
                    className={`text-xs font-bold ${run.quality_score >= 90 ? "text-emerald-600" : "text-amber-500"}`}
                  >
                    {run.quality_score.toFixed(1)}%
                  </span>
                )}
              </div>
            </div>
          ))}
        </div>

        <div className="flex-1 space-y-6 overflow-y-auto max-h-[600px]">
          {selectedRun ? (
            <div className="animate-in fade-in slide-in-from-right-4 duration-500">
              <div className="flex items-center justify-between gap-3 mb-6">
                <div className="flex items-center gap-3">
                  <div
                    className={`p-3 rounded-xl ${selectedRun.status === "PASSED" ? "bg-emerald-100 text-emerald-600" : "bg-rose-100 text-rose-600"} `}
                  >
                    {selectedRun.status === "PASSED" ? (
                      <CheckCircle2 size={24} />
                    ) : (
                      <AlertTriangle size={24} />
                    )}
                  </div>
                  <div>
                    <h3 className="text-lg font-bold text-foreground">
                      Run Details
                    </h3>
                    <p className="text-xs text-muted-foreground font-mono">
                      {selectedRun.timestamp}
                    </p>
                  </div>
                </div>
                <button
                  onClick={() => setJsonModalOpen(true)}
                  className="text-xs font-bold text-primary bg-primary/10 hover:bg-primary/20 px-3 py-1.5 rounded-lg border border-primary/20"
                >
                  {"{ }"} View JSON
                </button>
              </div>

              <div className="bg-muted/50 rounded-2xl p-6 border border-border">
                <h4 className="text-xs font-black text-muted-foreground/80 uppercase tracking-widest mb-3">
                  Verdict Reasoning
                </h4>
                <p className="text-sm text-foreground/90 leading-relaxed font-medium">
                  {selectedRun.reason}
                </p>

                {selectedRun.anomalies && selectedRun.anomalies.length > 0 && (
                  <div className="mt-6 pt-6 border-t border-border">
                    <h4 className="text-xs font-black text-rose-400 uppercase tracking-widest mb-3 flex items-center gap-2">
                      <AlertCircle size={14} /> Detected Anomalies
                    </h4>
                    <div className="space-y-2">
                      {selectedRun.anomalies.map((anom, i) => (
                        <div
                          key={i}
                          className="bg-card p-3 rounded-lg border border-rose-100 text-xs text-rose-700 font-mono"
                        >
                          {anom}
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            </div>
          ) : (
            <div className="h-full flex flex-col items-center justify-center text-center text-muted-foreground/80">
              <Filter size={48} className="mb-4 opacity-20" />
              <p>Select a run to view details</p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

// --- Modal: Propose Contract (HITL) ---
// --- Modal: Contract Wizard (Profile -> Propose -> Approve) ---
const ContractWizardModal = ({ isOpen, onClose, datasetName, dataset, onSave }) => {
  const [step, setStep] = useState(1); // 1: Profile, 2: Propose, 3: Approve
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  // Step 1 Data
  const [profileData, setProfileData] = useState(null);
  const [sampleData, setSampleData] = useState(null);

  // Step 2 Data
  const [proposedYaml, setProposedYaml] = useState("");
  const [aiAnalysis, setAiAnalysis] = useState(null);

  // Reset on open
  useEffect(() => {
    if (isOpen && datasetName) {
      setStep(1);
      setLoading(true);
      setError(null);
      setProfileData(null);
      setSampleData(null);
      setProposedYaml("");
      setAiAnalysis(null);

      // Step 1: Fetch Profile & Sample
      Promise.all([
        import("./api").then(api => api.getDatasetProfile(datasetName).catch(e => ({ data: { total_rows: "Unknown", columns: {} } }))),
        import("./api").then(api => api.getDatasetData(datasetName, 5).catch(e => ({ data: { data: [] } })))
      ])
        .then(([profileRes, sampleRes]) => {
          setProfileData(profileRes.data);
          setSampleData(sampleRes.data);
          setLoading(false);
        })
        .catch(err => {
          console.error("Profile fetch failed:", err);
          setError("Failed to profile dataset. " + (err.message || ""));
          setLoading(false);
        });
    }
  }, [isOpen, datasetName]);

  const handleGenerateProposal = async () => {
    setStep(2);
    setLoading(true);
    try {
      const api = await import("./api");
      const res = await api.proposeContract(datasetName);
      setProposedYaml(res.data.proposed_yaml);
      setAiAnalysis(res.data.generation);
      setLoading(false);
    } catch (err) {
      setError("Failed to generate contract proposal.");
      setLoading(false);
    }
  };

  const handleApprove = async () => {
    setLoading(true);
    try {
      await onSave({
        dataset_name: datasetName,
        yaml_content: proposedYaml,
        summary: "Wizard: User approved AI proposal"
      });
      setStep(3); // Success/Done state
      setLoading(false);
    } catch (err) {
      setError("Failed to save contract.");
      setLoading(false);
    }
  };

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-background/80 backdrop-blur-sm">
      <motion.div
        initial={{ opacity: 0, scale: 0.95 }}
        animate={{ opacity: 1, scale: 1 }}
        className="bg-card rounded-2xl shadow-2xl w-full max-w-5xl h-[80vh] flex flex-col overflow-hidden"
      >
        {/* Header with Steps */}
        <div className="p-6 border-b border-border bg-muted/50 flex justify-between items-center">
          <div>
            <h3 className="text-xl font-black text-foreground tracking-tight">Contract Wizard</h3>
            <p className="text-xs text-muted-foreground font-bold uppercase tracking-wider mt-1">{datasetName}</p>
          </div>

          <div className="flex items-center gap-4">
            {[1, 2, 3].map(s => (
              <div key={s} className={`flex items-center gap-2 ${step >= s ? "opacity-100" : "opacity-40"}`}>
                <div className={`w-8 h-8 rounded-full flex items-center justify-center font-bold text-sm 
                    ${step === s ? "bg-primary text-white shadow-lg shadow-primary/20" :
                    step > s ? "bg-emerald-500 text-white" : "bg-muted/80 text-muted-foreground"}`}>
                  {step > s ? <CheckCircle2 size={16} /> : s}
                </div>
                <span className={`text-xs font-bold uppercase ${step === s ? "text-primary" : "text-muted-foreground"}`}>
                  {s === 1 ? "Profile" : s === 2 ? "Propose" : "Approve"}
                </span>
                {s < 3 && <div className="w-8 h-px bg-muted/80 mx-2" />}
              </div>
            ))}
          </div>

          <button onClick={onClose} className="p-2 hover:bg-muted/80 rounded-full text-muted-foreground/80"
          >
            <XCircle size={24} />
          </button>
        </div>

        {/* Content Area */}
        <div className="flex-1 overflow-hidden relative bg-card">
          {loading && (
            <div className="absolute inset-0 z-10 bg-card/80 flex flex-col items-center justify-center gap-4">
              <Loader2 size={48} className="animate-spin text-primary" />
              <p className="font-bold text-muted-foreground animate-pulse">Processing...</p>
            </div>
          )}

          {error && (
            <div className="flex flex-col items-center justify-center h-full text-center p-10 space-y-4">
              <div className="p-4 bg-rose-100 text-rose-600 rounded-full"><AlertTriangle size={40} /></div>
              <h3 className="text-lg font-bold text-foreground/90">Something went wrong</h3>
              <p className="text-rose-500 max-w-md">{error}</p>
              <button
                onClick={onClose}
                className="px-6 py-2 bg-muted font-bold text-muted-foreground rounded-lg hover:bg-muted/80"
              >
                Close
              </button>
            </div>
          )}

          {!loading && !error && step === 1 && (
            <div className="h-full flex flex-col p-8 overflow-y-auto">
              <div className="text-center mb-8">
                <h2 className="text-2xl font-black text-foreground">Profiling Analysis</h2>
                <p className="text-muted-foreground mt-2">We've analyzed <b>{profileData?.total_rows?.toLocaleString() || "N/A"} rows</b> to detect schema & distribution.</p>
              </div>

              <div className="grid grid-cols-4 gap-4 mb-8">
                <div className="p-4 bg-muted/50 border border-border rounded-xl text-center">
                  <div className="text-xs font-bold text-muted-foreground/80 uppercase">Columns</div>
                  <div className="text-2xl font-black text-foreground">{Object.keys(profileData?.column_profiles || {}).length}</div>
                </div>
                <div className="p-4 bg-muted/50 border border-border rounded-xl text-center">
                  <div className="text-xs font-bold text-muted-foreground/80 uppercase">Est. Memory</div>
                  <div className="text-2xl font-black text-foreground">{profileData?.memory_usage_mb?.toFixed(2) || "0.00"} MB</div>
                </div>
                <div className="p-4 bg-muted/50 border border-border rounded-xl text-center">
                  <div className="text-xs font-bold text-muted-foreground/80 uppercase">Infer Quality</div>
                  <div className="text-2xl font-black text-emerald-500">{profileData?.overall_quality_score?.toFixed(0) || "100"}%</div>
                </div>
                <div className="p-4 bg-muted/50 border border-border rounded-xl text-center">
                  <div className="text-xs font-bold text-muted-foreground/80 uppercase">Source Type</div>
                  <div className="text-2xl font-black text-primary">
                    {dataset?.data_file?.split('.').pop()?.toUpperCase() || 'UNKNOWN'}
                  </div>
                </div>
              </div>

              <div className="border rounded-xl overflow-hidden shadow-sm">
                <table className="w-full text-xs text-left">
                  <thead className="bg-muted/50 border-b border-border">
                    <tr>
                      <th className="p-3 font-bold text-muted-foreground">Column Name</th>
                      <th className="p-3 font-bold text-muted-foreground">Type</th>
                      <th className="p-3 font-bold text-muted-foreground">Nulls</th>
                      <th className="p-3 font-bold text-muted-foreground">Unique</th>
                      <th className="p-3 font-bold text-muted-foreground">Sample Value</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-border">
                    {Object.entries(profileData?.column_profiles || {}).map(([col, stats], idx) => {
                      const sampleVal = sampleData?.data?.[0]?.[col];
                      return (
                        <tr key={col} className="hover:bg-muted/50">
                          <td className="p-3 font-bold text-foreground/90">{col}</td>
                          <td className="p-3 font-mono text-muted-foreground">{stats.type}</td>
                          <td className="p-3 text-muted-foreground">{stats.null_count} ({profileData?.total_rows ? ((stats.null_count / profileData.total_rows) * 100).toFixed(1) : 0}%)</td>
                          <td className="p-3 text-muted-foreground">{stats.unique_count}</td>
                          <td className="p-3 font-mono text-muted-foreground/80 truncate max-w-[150px]" title={String(sampleVal)}>{String(sampleVal).substring(0, 30)}</td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {!loading && !error && step === 2 && (
            <div className="h-full flex flex-col">
              <div className="flex-1 flex overflow-hidden">
                <div className="w-1/3 border-r border-border bg-muted/50 p-6 overflow-y-auto">
                  <h4 className="font-bold text-foreground/90 mb-4 flex items-center gap-2">
                    <Sparkles size={16} className="text-primary" /> AI Suggestions
                  </h4>
                  <ul className="space-y-4 text-xs">
                    <li className="p-3 bg-card rounded-lg border border-border shadow-sm">
                      <strong className="block text-foreground mb-1">Detected Schema</strong>
                      <p className="text-muted-foreground">Inferred types for {Object.keys(profileData?.column_profiles || {}).length} columns based on sampling.</p>
                    </li>
                    <li className="p-3 bg-card rounded-lg border border-border shadow-sm">
                      <strong className="block text-foreground mb-1">Quality Rules</strong>
                      <p className="text-muted-foreground">Auto-generated non-null and uniqueness checks for primary keys.</p>
                    </li>
                    {aiAnalysis?.warnings?.map((w, i) => (
                      <li key={i} className="p-3 bg-amber-50 rounded-lg border border-amber-100 shadow-sm">
                        <strong className="block text-amber-800 mb-1">Warning</strong>
                        <p className="text-amber-700">{w}</p>
                      </li>
                    ))}
                  </ul>
                </div>
                <div className="w-2/3 flex flex-col bg-zinc-950">
                  <div className="p-2 border-b border-border bg-muted/50 text-xs font-mono text-muted-foreground/80 flex justify-between">
                    <span>contract.yaml</span>
                    <span>Editable</span>
                  </div>
                  <textarea
                    className="flex-1 bg-zinc-950 text-green-400 font-mono text-xs p-4 resize-none focus:outline-none"
                    value={proposedYaml}
                    onChange={(e) => setProposedYaml(e.target.value)}
                    spellCheck="false"
                  />
                </div>
              </div>
            </div>
          )}

          {!loading && !error && step === 3 && (
            <div className="h-full flex flex-col items-center justify-center text-center p-10 animate-in zoom-in-50 duration-300">
              <div className="w-24 h-24 bg-emerald-100 text-emerald-600 rounded-full flex items-center justify-center mb-6 shadow-xl shadow-emerald-100">
                <CheckCircle2 size={48} />
              </div>
              <h2 className="text-3xl font-black text-foreground mb-2">Contract Activated!</h2>
              <p className="text-muted-foreground text-lg max-w-md mb-8">
                <b>{datasetName}</b> is now a managed asset. Quality checks will run automatically on the next scan.
              </p>
              <div className="flex gap-4">
                <button
                  onClick={onClose}
                  className="px-8 py-3 bg-secondary text-secondary-foreground shadow-lg hover:shadow-xl hover:-translate-y-1 transition-all"
                >
                  Return to Dashboard
                </button>
              </div>
            </div>
          )}

        </div>

        {/* Footer Actions */}
        {!loading && !error && step < 3 && (
          <div className="p-6 border-t border-border bg-muted/50 flex justify-end gap-3">
            {step === 1 && (
              <button
                onClick={handleGenerateProposal}
                className="px-6 py-2.5 bg-primary hover:bg-primary/90 text-white font-bold rounded-xl shadow-lg shadow-primary/20 hover:-translate-y-0.5 transition-all flex items-center gap-2"
              >
                <Sparkles size={16} /> Generate Proposal (AI)
              </button>
            )}
            {step === 2 && (
              <>
                <button
                  onClick={() => setStep(1)}
                  className="px-6 py-2.5 text-muted-foreground font-bold hover:bg-muted/80 rounded-xl transition-colors"
                >
                  Back
                </button>
                <button
                  onClick={handleApprove}
                  className="px-6 py-2.5 bg-green-600 hover:bg-green-700 text-white font-bold rounded-xl shadow-lg shadow-green-200 hover:-translate-y-0.5 transition-all flex items-center gap-2"
                >
                  <CheckCircle2 size={16} /> Approve & Save
                </button>
              </>
            )}
          </div>
        )}
      </motion.div>
    </div>
  );
};

// --- Tab: Contract Governance ---
const GovernanceTab = ({ datasetName, lastRun }) => {
  const [history, setHistory] = useState([]);
  const [selectedVersion, setSelectedVersion] = useState(null);
  const [originalContent, setOriginalContent] = useState("");
  const [historicalContent, setHistoricalContent] = useState("");
  const [loading, setLoading] = useState(false);
  const [isProposeOpen, setIsProposeOpen] = useState(false);
  const [remediation, setRemediation] = useState(null);
  const [remediationView, setRemediationView] = useState("llm");

  // Fetch history list
  const fetchHistory = () => {
    getGovernanceHistory(datasetName).then((res) => {
      setHistory(res.data);
      if (res.data.length > 0) setSelectedVersion(res.data[0]);
    });
  };

  useEffect(() => {
    fetchHistory();
    // Fetch Current Active Content
    getRemediationPlan(datasetName).then((res) => {
      if (res.data.original_yaml) setOriginalContent(res.data.original_yaml);
      setRemediation(res.data || null);
    });
  }, [datasetName]);

  // Fetch content when version selected
  useEffect(() => {
    if (selectedVersion) {
      setLoading(true);
      getHistoricalFile(selectedVersion.filename).then((res) => {
        setHistoricalContent(res.data.content);
        setLoading(false);
      });
    }
  }, [selectedVersion]);

  const handleRollback = async () => {
    if (
      !window.confirm(
        `Are you sure you want to revert ${datasetName} to version from ${selectedVersion.timestamp}?`,
      )
    )
      return;

    try {
      await rollbackSchema({
        dataset_name: datasetName,
        filename: selectedVersion.filename,
      });
      alert("Rollback successful!");
      window.location.reload(); // Simple refresh to see changes
    } catch (err) {
      alert("Rollback failed: " + err.message);
    }
  };

  const handleSaveContract = async (data) => {
    await saveContract(data);
    alert("New contract saved successfully!");
    fetchHistory(); // Refresh list
    // Refresh current view
    getRemediationPlan(datasetName).then((res) => {
      if (res.data.original_yaml) setOriginalContent(res.data.original_yaml);
      setRemediation(res.data || null);
    });
  };

  const handleApplyRemediation = async (yamlContent) => {
    if (!yamlContent) return;
    try {
      await applyRemediation({
        dataset_name: datasetName,
        proposed_yaml: yamlContent,
        error_context: remediation?.error || "Manual remediation apply",
      });
      alert("Remediation applied. Re-scan to validate.");
      fetchHistory();
      getRemediationPlan(datasetName).then((res) => {
        if (res.data.original_yaml) setOriginalContent(res.data.original_yaml);
        setRemediation(res.data || null);
      });
    } catch (err) {
      alert("Remediation apply failed: " + err.message);
    }
  };

  return (
    <div className="flex h-full gap-6 relative">
      <ContractWizardModal
        isOpen={isProposeOpen}
        onClose={() => setIsProposeOpen(false)}
        datasetName={datasetName}
        onSave={handleSaveContract}
      />

      <div className="w-1/4 space-y-4">
        <h4 className="font-bold text-foreground/90 flex items-center gap-2">
          <History size={16} /> Version History
        </h4>
        <div className="space-y-2 max-h-[500px] overflow-y-auto">
          {history.map((ver, idx) => (
            <div
              key={idx}
              onClick={() => setSelectedVersion(ver)}
              className={`p-3 rounded-lg border text-sm cursor-pointer transition-colors ${selectedVersion?.filename === ver.filename
                ? "border-orange-300 bg-orange-50 shadow-sm"
                : "border-border hover:bg-muted/50"
                } `}
            >
              <div className="flex justify-between items-center mb-1">
                <span className="font-mono text-xs text-muted-foreground">
                  {ver.timestamp
                    ? new Date(ver.timestamp).toLocaleString()
                    : "Unknown"}
                </span>
                {idx === 0 && (
                  <span className="bg-green-100 text-green-700 text-[10px] px-1.5 py-0.5 rounded-full">
                    Newest
                  </span>
                )}
              </div>
              <p className="font-medium text-foreground line-clamp-2">
                {ver.summary}
              </p>
            </div>
          ))}
          {history.length === 0 && (
            <p className="text-sm text-muted-foreground/80 italic">
              No history versions found.
            </p>
          )}
        </div>
      </div>

      <div className="flex-1 flex flex-col h-full">
        <div className="flex justify-between items-center mb-4">
          <h4 className="font-bold text-foreground/90">Schema Comparison</h4>
          <div className="flex gap-2">
            <button
              onClick={() => setIsProposeOpen(true)}
              className="bg-orange-600 hover:bg-orange-700 text-white text-xs font-bold px-4 py-2 rounded-lg flex items-center gap-2 transition-all shadow-sm hover:shadow-md"
            >
              <Zap size={14} /> Propose New Contract
            </button>
            {selectedVersion && (
              <button
                onClick={handleRollback}
                className="bg-orange-600 hover:bg-orange-700 text-white text-xs font-bold px-4 py-2 rounded-lg flex items-center gap-2"
              >
                <Users size={14} /> Revert to this Version
              </button>
            )}
          </div>
        </div>

        {remediation?.status === "remediation_available" && (
          <div className="mb-4 bg-card border border-border rounded-xl p-4">
            <div className="flex items-center justify-between">
              <div>
                <h4 className="text-sm font-bold text-foreground/90">
                  Remediation Proposal
                </h4>
                <p className="text-xs text-muted-foreground">
                  {remediation?.generation?.engine
                    ? `Source: ${remediation.generation.engine} `
                    : "Source: hybrid"}
                </p>
              </div>
              <div className="flex gap-2">
                <button
                  onClick={() => setRemediationView("llm")}
                  className={`px-3 py-1.5 text-xs font-bold rounded-lg border ${remediationView === "llm" ? "bg-orange-600 text-white border-orange-600" : "bg-card text-muted-foreground border-border"} `}
                >
                  LLM Proposal
                </button>
                <button
                  onClick={() => setRemediationView("deterministic")}
                  className={`px-3 py-1.5 text-xs font-bold rounded-lg border ${remediationView === "deterministic" ? "bg-emerald-600 text-white border-emerald-600" : "bg-card text-muted-foreground border-border"} `}
                >
                  Deterministic Merge
                </button>
                <button
                  onClick={() => setRemediationView("observed")}
                  className={`px-3 py-1.5 text-xs font-bold rounded-lg border ${remediationView === "observed" ? "bg-secondary text-secondary-foreground border-border/50" : "bg-card text-muted-foreground border-border"} `}
                >
                  Observed Schema
                </button>
              </div>
            </div>

            {remediation?.merge_summary && (
              <div className="mt-3 text-xs text-muted-foreground">
                <span className="font-bold">Merge Summary:</span>{" "}
                {remediation.merge_summary.added_columns?.length || 0} added,
                {` ${remediation.merge_summary.updated_types?.length || 0} `}{" "}
                type updates,
                {` ${remediation.merge_summary.filled_fields?.length || 0} `}{" "}
                filled fields
              </div>
            )}

            {remediation?.generation?.warnings?.length > 0 && (
              <div className="mt-2 text-xs text-amber-600">
                {remediation.generation.warnings.join(" | ")}
              </div>
            )}

            <div className="mt-3 flex gap-2">
              <button
                onClick={() =>
                  handleApplyRemediation(
                    remediation?.deterministic_yaml ||
                    remediation?.proposed_yaml,
                  )
                }
                className="px-4 py-2 text-xs font-bold rounded-lg bg-emerald-600 text-white"
              >
                Apply Deterministic
              </button>
              <button
                onClick={() =>
                  handleApplyRemediation(remediation?.proposed_yaml)
                }
                className="px-4 py-2 text-xs font-bold rounded-lg bg-orange-600 text-white"
                disabled={!remediation?.proposed_yaml}
              >
                Apply LLM
              </button>
            </div>

            <div className="mt-3 bg-zinc-950 rounded-lg p-3 overflow-auto max-h-[220px] border border-border">
              <pre className="text-xs font-mono text-green-400 whitespace-pre-wrap">
                {remediationView === "observed"
                  ? remediation?.observed_yaml
                  : remediationView === "deterministic"
                    ? remediation?.deterministic_yaml
                    : remediation?.proposed_yaml}
              </pre>
            </div>
          </div>
        )}

        <div className="flex-1 grid grid-cols-2 gap-4 h-0 min-h-[400px]">
          {/* Current Active */}
          <div className="flex flex-col">
            <span className="text-xs font-bold text-muted-foreground mb-2 uppercase">
              Current Active Schema
            </span>
            <div className="flex-1 bg-zinc-950/90 rounded-lg p-4 overflow-auto border border-border relative">
              <pre className="text-xs font-mono text-green-400">
                {originalContent || "Loading current schema..."}
              </pre>
            </div>
          </div>

          {/* Historical Selection */}
          <div className="flex flex-col">
            <span className="text-xs font-bold text-muted-foreground mb-2 uppercase flex items-center gap-2">
              {selectedVersion?.timestamp
                ? `Version: ${new Date(selectedVersion.timestamp).toLocaleString()} `
                : "Selected Version"}
              {loading && <Loader2 size={12} className="animate-spin" />}
            </span>
            <div className="flex-1 bg-zinc-950/90 rounded-lg p-4 overflow-auto border border-border">
              <pre className="text-xs font-mono text-muted-foreground">
                {historicalContent || "Select a version to view content..."}
              </pre>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

// --- Tab: Connections ---
const ConnectionsTab = () => {
  const connections = [
    {
      id: "snowflake",
      name: "Snowflake",
      type: "Warehouse",
      status: "CONNECTED",
      datasets: 4,
      lastSync: "2m ago",
      icon: Database,
    },
    {
      id: "kafka",
      name: "Kafka Stream",
      type: "Event Bus",
      status: "STREAMING",
      datasets: 12,
      lastSync: "Live",
      icon: Activity,
    },
    {
      id: "bigquery",
      name: "BigQuery Analytics",
      type: "Warehouse",
      status: "CONNECTED",
      datasets: 8,
      lastSync: "5m ago",
      icon: Database,
    },
    {
      id: "postgres",
      name: "Operational DB",
      type: "Database",
      status: "CONNECTED",
      datasets: 24,
      lastSync: "1h ago",
      icon: Server,
    },
  ];

  return (
    <div className="space-y-6 max-w-7xl mx-auto">
      <div className="flex items-center justify-between">
        <div>
          <h3 className="text-lg font-black text-foreground">
            Active Data Connections
          </h3>
          <p className="text-muted-foreground text-sm">
            Manage source integrations and ingestion pipelines.
          </p>
        </div>
        <button className="bg-secondary text-secondary-foreground hover:bg-muted transition-colors flex items-center gap-2">
          <Plus size={14} /> Add Connection
        </button>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        {connections.map((conn) => (
          <div
            key={conn.id}
            className="bg-card p-6 rounded-2xl border border-border shadow-sm hover:shadow-md transition-shadow group"
          >
            <div className="flex items-start justify-between mb-4">
              <div className="p-3 bg-muted/50 rounded-xl group-hover:bg-orange-50 transition-colors">
                <conn.icon
                  size={24}
                  className="text-muted-foreground/80 group-hover:text-orange-600 transition-colors"
                />
              </div>
              <div
                className={`px-2 py-1 rounded text-[10px] font-black uppercase tracking-wider ${conn.status === "CONNECTED"
                  ? "bg-green-100 text-green-600"
                  : conn.status === "STREAMING"
                    ? "bg-orange-100 text-orange-600"
                    : "bg-muted text-muted-foreground"
                  } `}
              >
                {conn.status}
              </div>
            </div>

            <h4 className="font-bold text-foreground mb-1">{conn.name}</h4>
            <p className="text-xs text-muted-foreground/80 font-bold uppercase tracking-wider mb-4">
              {conn.type}
            </p>

            <div className="flex items-center justify-between text-xs text-muted-foreground border-t border-border pt-4">
              <div className="flex items-center gap-1">
                <Database size={12} />
                <span className="font-bold">{conn.datasets}</span> Datasets
              </div>
              <div className="flex items-center gap-1">
                <Clock size={12} />
                {conn.lastSync}
              </div>
            </div>
          </div>
        ))}

        <button className="border-2 border-dashed border-border rounded-2xl p-6 flex flex-col items-center justify-center gap-3 text-muted-foreground/80 hover:border-orange-300 hover:text-orange-600 hover:bg-orange-50/50 transition-all group">
          <div className="w-12 h-12 rounded-full bg-muted/50 flex items-center justify-center group-hover:bg-card transition-colors">
            <Plus size={24} />
          </div>
          <span className="text-xs font-bold uppercase tracking-wider">
            Connect New Source
          </span>
        </button>
      </div>
    </div>
  );
};

// --- Modal: Data Preview ---
const DataPreviewModal = ({ isOpen, onClose, datasetName }) => {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (isOpen && datasetName) {
      setLoading(true);
      import("./api").then((api) => {
        api.getDatasetData(datasetName)
          .then((res) => {
            setData(res.data);
            setLoading(false);
          })
          .catch((err) => {
            console.error("Failed to load data", err);
            setError("Failed to load dataset preview.");
            setLoading(false);
          });
      });
    } else {
      setData(null);
      setError(null);
    }
  }, [isOpen, datasetName]);

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-background/80 backdrop-blur-sm">
      <div className="bg-card rounded-2xl shadow-2xl w-full max-w-6xl max-h-[90vh] flex flex-col overflow-hidden">
        <div className="p-4 border-b border-border flex justify-between items-center bg-muted/80">
          <div className="flex items-center gap-3">
            <div className="p-2 bg-orange-50 text-orange-600 rounded-lg">
              <Database size={20} />
            </div>
            <div>
              <h3 className="font-bold text-foreground text-lg">
                Data Preview: {datasetName}
              </h3>
              <p className="text-xs text-muted-foreground font-medium">
                Showing first {data?.preview_limit || 100} rows • {data?.total_rows} total rows
              </p>
            </div>
          </div>
          <button onClick={onClose} className="p-2 hover:bg-muted/80 rounded-full text-muted-foreground/80 transition-colors">
            <XCircle size={24} />
          </button>
        </div>

        <div className="flex-1 overflow-auto bg-card custom-scrollbar relative">
          {loading ? (
            <div className="absolute inset-0 flex flex-col items-center justify-center gap-3">
              <Loader2 size={40} className="animate-spin text-orange-600" />
              <p className="text-sm font-bold text-muted-foreground/80 animate-pulse">Fetching dataset content...</p>
            </div>
          ) : error ? (
            <div className="p-10 flex flex-col items-center justify-center text-rose-500 gap-2">
              <AlertTriangle size={32} />
              <p className="font-bold">{error}</p>
            </div>
          ) : data && data.data.length > 0 ? (
            <table className="w-full text-left border-collapse text-sm">
              <thead className="sticky top-0 bg-muted/50 shadow-sm z-10">
                <tr>
                  <th className="p-3 pl-6 border-b border-border bg-muted/50 w-16 text-center text-xs font-black text-muted-foreground/80 uppercase">#</th>
                  {data.columns.map((col) => (
                    <th key={col} className="p-3 border-b border-border text-xs font-black text-muted-foreground uppercase tracking-wide whitespace-nowrap">
                      {col}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-border">
                {data.data.map((row, idx) => (
                  <tr key={idx} className="hover:bg-orange-50/30 transition-colors group">
                    <td className="p-3 pl-6 border-r border-border/50 font-mono text-xs text-muted-foreground/80 text-center select-none group-hover:text-orange-400">
                      {idx + 1}
                    </td>
                    {data.columns.map((col) => (
                      <td key={`${idx}-${col}`} className="p-3 text-foreground/90 whitespace-nowrap truncate max-w-[300px]" title={String(row[col])}>
                        {row[col] === null ? <span className="text-muted-foreground/40 italic">null</span> : String(row[col])}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          ) : (
            <div className="p-20 text-center text-muted-foreground/80 italic">Dataset is empty.</div>
          )}
        </div>
        <div className="p-3 border-t border-border bg-muted/50 text-xs text-center text-muted-foreground/80">
          Read-only preview mode. Large files are truncated for performance.
        </div>
      </div>
    </div>
  );
};

// --- Tab: Datasets Overview ---
const DatasetsTab = ({ datasets, onProfile, onGenerateContract, previewDataset, setPreviewDataset }) => {
  return (
    <div className="space-y-8 h-full">
      <DataPreviewModal
        isOpen={!!previewDataset}
        onClose={() => setPreviewDataset(null)}
        datasetName={previewDataset}
      />

      <div className="flex items-center justify-between">
        <div className="flex items-center gap-4 bg-card border border-border px-4 py-2 rounded-2xl shadow-sm w-96">
          <Search size={18} className="text-muted-foreground/80" />
          <input
            type="text"
            placeholder="Search datasets, owners, tags..."
            className="bg-transparent border-none text-sm focus:outline-none w-full text-foreground"
          />
        </div>
        <button className="bg-secondary text-secondary-foreground hover:bg-muted transition-all">
          <Database size={16} /> Register New Dataset
        </button>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        {datasets.map((ds) => {
          if (ds.lifecycle === "unconfigured") {
            return (
              <motion.div
                whileHover={{ y: -5 }}
                key={ds.name}
                className="bg-muted/50 rounded-3xl border-2 border-dashed border-border shadow-sm flex flex-col h-full text-foreground"
              >
                <div className="p-6 border-b border-dashed border-border flex justify-between items-start">
                  <div className="flex items-center gap-3">
                    <div className="w-10 h-10 rounded-xl bg-orange-50 flex items-center justify-center text-orange-400">
                      <FileText size={20} />
                    </div>
                    <div>
                      <h4 className="font-bold text-md tracking-tight text-muted-foreground">
                        {ds.name}
                      </h4>
                      <span className="text-[10px] font-bold text-orange-600 uppercase tracking-widest flex items-center gap-1">
                        <Sparkles size={10} /> Discovered Source
                      </span>
                    </div>
                  </div>
                  <div className="p-1.5 rounded-md bg-muted text-muted-foreground/80">
                    <AlertCircle size={14} />
                  </div>
                </div>

                <div className="p-6 flex-1 flex flex-col items-center justify-center text-center space-y-3">
                  <p className="text-xs text-muted-foreground font-medium">
                    Raw file detected in landing zone. No data contract exists yet.
                  </p>
                  <div className="w-full bg-muted/80 h-px" />
                  <div className="grid grid-cols-2 w-full gap-4 text-left">
                    <div>
                      <p className="text-[10px] font-bold text-muted-foreground/80 uppercase">Format</p>
                      <p className="text-xs font-bold text-foreground/90">{ds.data_file.split('.').pop().toUpperCase()}</p>
                    </div>
                    <div>
                      <p className="text-[10px] font-bold text-muted-foreground/90 uppercase">Status</p>
                      <p className="text-xs font-bold text-foreground/90">Unmanaged</p>
                    </div>
                  </div>
                </div>

                <div className="p-4 bg-muted/50 border-t border-dashed border-border px-6 flex justify-between gap-2">
                  <button
                    onClick={() => setPreviewDataset(ds.name)}
                    className="flex-1 bg-card hover:bg-orange-50 text-muted-foreground hover:text-orange-600 text-xs font-bold py-2.5 rounded-xl shadow-sm border border-border transition-all flex items-center justify-center gap-2"
                  >
                    <Table size={14} /> View Data
                  </button>
                  <button
                    onClick={() => onGenerateContract(ds.name)}
                    className="flex-1 bg-orange-600 hover:bg-orange-700 text-white text-xs font-bold py-2.5 rounded-xl shadow-sm transition-all flex items-center justify-center gap-2"
                  >
                    <Zap size={14} /> Generate Contract
                  </button>
                </div>
              </motion.div>
            );
          }

          return (
            <motion.div
              whileHover={{ y: -5 }}
              key={ds.name}
              className="bg-card rounded-3xl border border-border shadow-soft flex flex-col h-full text-foreground"
            >
              <div className="p-6 border-b border-border flex justify-between items-start">
                <div className="flex items-center gap-3">
                  <div className="w-10 h-10 rounded-xl bg-primary/10 flex items-center justify-center text-primary">
                    <Table size={20} />
                  </div>
                  <div>
                    <h4 className="font-bold text-md tracking-tight">
                      {ds.name}
                    </h4>
                    <span className="text-[10px] font-bold text-muted-foreground/80 uppercase tracking-widest">
                      {ds.domain || "LAKEHOUSE"}
                    </span>
                  </div>
                </div>
                <div
                  className={`p-1.5 rounded-md ${ds.status === "PASSED" ? "bg-green-100" : "bg-rose-100"} `}
                >
                  {ds.status === "PASSED" ? (
                    <ShieldCheck size={14} className="text-green-600" />
                  ) : (
                    <ShieldCheck size={14} className="text-rose-600" />
                  )}
                </div>
              </div>

              <div className="p-6 flex-1 space-y-6">
                <div className="flex items-center justify-between">
                  <div className="space-y-1">
                    <p className="text-[10px] font-black text-muted-foreground/80 uppercase tracking-widest">
                      Columns
                    </p>
                    <p className="text-sm font-bold text-foreground/90">
                      {ds.column_count || 6}
                    </p>
                  </div>
                  <div className="space-y-1 text-right">
                    <p className="text-[10px] font-black text-muted-foreground/80 uppercase tracking-widest">
                      Quality Rules
                    </p>
                    <p className="text-sm font-bold text-foreground/90">
                      {ds.has_quality_rules ? "12 Active" : "None"}
                    </p>
                  </div>
                </div>

                <div className="space-y-2">
                  <p className="text-[10px] font-black text-muted-foreground/80 uppercase tracking-widest">
                    Lineage Tags
                  </p>
                  <div className="flex flex-wrap gap-2 text-foreground">
                    <span className="px-2 py-1 bg-muted text-[10px] font-bold text-muted-foreground rounded-md">
                      S3_LANDING
                    </span>
                    <span className="px-2 py-1 bg-muted text-[10px] font-bold text-muted-foreground rounded-md">
                      PII_SENSITIVE
                    </span>
                    <span className="px-2 py-1 bg-muted text-[10px] font-bold text-muted-foreground rounded-md">
                      TIER_1
                    </span>
                  </div>
                </div>
              </div>

              <div className="p-4 bg-muted/50 border-t border-border px-6 space-y-3">
                <div className="flex justify-between items-center">
                  <div className="flex items-center gap-2">
                    <div className="w-5 h-5 rounded-full bg-muted/80" />
                    <span className="text-[10px] font-bold text-muted-foreground/80 uppercase tracking-tight">
                      Owner: <span className="text-muted-foreground underline cursor-pointer">{ds.owner}</span>
                    </span>
                  </div>
                  <div className="flex gap-1">
                    <button
                      onClick={() => onProfile(ds.name)}
                      className="text-muted-foreground/80 hover:text-primary hover:bg-orange-50 p-2 rounded-lg transition-all"
                      title="Deep Profile"
                    >
                      <Microscope size={14} />
                    </button>
                    <button
                      className="text-muted-foreground/80 hover:text-primary hover:bg-orange-50 p-2 rounded-lg transition-all"
                      title="View Schema"
                    >
                      <Code size={14} />
                    </button>
                  </div>
                </div>

                <button
                  onClick={() => setPreviewDataset(ds.name)}
                  className="w-full bg-card hover:bg-muted/50 text-foreground/90 text-xs font-bold py-2.5 rounded-xl border border-border shadow-sm transition-all flex items-center justify-center gap-2 group"
                >
                  <Table size={14} className="group-hover:text-primary transition-colors" />
                  View Data
                </button>
              </div>
            </motion.div>
          );
        })}
      </div>
    </div>
  );
};

// --- Tab: Lineage View ---
const LineageTab = ({ pulseData, lineageGraph, embedded = false }) => {
  if (!lineageGraph) {
    return (
      <div
        className={`bg-card rounded-[40px] border border-border shadow-soft h-full flex items-center justify-center p-20 text-slate - 400 ${embedded ? "border-0 shadow-none rounded-none p-10" : ""} `}
      >
        <div className="flex flex-col items-center gap-4">
          <Loader2 size={48} className="animate-spin text-primary/30" />
          <p className="font-bold text-sm tracking-widest uppercase">
            Tracing Data Lineage...
          </p>
        </div>
      </div>
    );
  }

  return (
    <div
      className={`bg-card rounded-[40px] border border-border shadow-soft h-full flex flex-col relative overflow-hidden text-slate - 800 ${embedded ? "border-0 shadow-none rounded-none h-auto" : ""} `}
    >
      {!embedded && (
        <div className="absolute inset-0 bg-[radial-gradient(#f1f5f9_1px,transparent_1px)] [background-size:32px_32px]" />
      )}

      <div
        className={`p-10 relative z-10 flex flex-col h-full overflow-y - auto custom - scrollbar ${embedded ? "p-0 overflow-visible" : ""} `}
      >
        {!embedded && (
          <div className="flex justify-between items-start mb-12 shrink-0">
            <div>
              <h3 className="text-2xl font-black text-foreground tracking-tighter">
                Lakehouse Lineage Mapper
              </h3>
              <p className="text-sm text-muted-foreground font-medium">
                Visualizing data dependencies & blast radius across active
                environments
              </p>
            </div>
            <div className="flex gap-3">
              <div className="bg-card p-2 rounded-xl border border-border shadow-sm flex items-center gap-4 px-4">
                <span className="text-[10px] font-black uppercase text-muted-foreground/80">
                  Node Legend
                </span>
                <div className="flex items-center gap-2">
                  <div className="w-2 h-2 rounded-full bg-orange-500" />
                  <span className="text-[10px] font-bold text-muted-foreground">
                    Dataset
                  </span>
                </div>
                <div className="flex items-center gap-2">
                  <div className="w-2 h-2 rounded-full bg-orange-400" />
                  <span className="text-[10px] font-bold text-muted-foreground">
                    Service
                  </span>
                </div>
                <div className="flex items-center gap-2">
                  <div className="w-2 h-2 rounded-full bg-rose-400" />
                  <span className="text-[10px] font-bold text-muted-foreground">
                    Critical App
                  </span>
                </div>
              </div>
            </div>
          </div>
        )}

        <div className="space-y-8 pb-10">
          {Object.entries(lineageGraph.datasets || {}).map(([dsName, data]) => (
            <div
              key={dsName}
              className="bg-card rounded-3xl border border-border p-8 shadow-sm relative group hover:shadow-md transition-shadow"
            >
              <div className="absolute top-8 right-8 text-muted-foreground/40 group-hover:text-primary/20 transition-colors">
                <Database size={64} strokeWidth={1} />
              </div>

              <div className="flex items-center gap-4 mb-8">
                <div className="p-3 bg-orange-50 text-orange-600 rounded-xl border border-orange-200">
                  <Database size={24} />
                </div>
                <div>
                  <h3 className="text-xl font-black text-foreground">
                    {dsName}
                  </h3>
                  <div className="flex items-center gap-2 mt-1">
                    <span className="text-[10px] font-bold bg-muted text-muted-foreground px-2 py-0.5 rounded-md uppercase tracking-wider">
                      Owner: {data.owner}
                    </span>
                    {pulseData.find((p) => p.name === dsName)?.status ===
                      "BLOCKED" && (
                        <span className="text-[10px] font-bold bg-rose-100 text-rose-600 px-2 py-0.5 rounded-md uppercase tracking-wider flex items-center gap-1">
                          <AlertTriangle size={10} /> Remediation Needed
                        </span>
                      )}
                  </div>
                </div>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-2 gap-12 relative">
                {/* Connecting Line (Visual only for desktop) */}
                <div className="hidden md:block absolute left-1/2 top-10 bottom-10 w-px bg-muted -translate-x-1/2" />
                <div className="hidden md:flex absolute left-1/2 top-1/2 -translate-y-1/2 -translate-x-1/2 w-8 h-8 bg-muted/50 border border-border rounded-full items-center justify-center text-muted-foreground/40 z-10 shadow-sm">
                  <ArrowRight size={14} />
                </div>

                {/* Upstream */}
                <div>
                  <h4 className="flex items-center gap-2 text-xs font-black uppercase text-muted-foreground/80 mb-4 tracking-widest">
                    <Share2 size={12} className="rotate-180" /> Upstream Sources
                  </h4>
                  <div className="space-y-3">
                    {data.upstream?.map((src, i) => (
                      <div
                        key={i}
                        className="p-4 rounded-xl border border-border bg-muted/50 flex items-center gap-3"
                      >
                        <div className="w-2 h-2 rounded-full bg-orange-400 shrink-0" />
                        <div className="overflow-hidden">
                          <div className="font-bold text-foreground/90 text-sm truncate">
                            {src.name}
                          </div>
                          <div
                            className="text-[10px] text-muted-foreground/80 font-bold uppercase truncate"
                            title={src.endpoint}
                          >
                            {src.type} •{" "}
                            {src.endpoint?.split("/")[2] || "External"}
                          </div>
                        </div>
                      </div>
                    ))}
                    {(!data.upstream || data.upstream.length === 0) && (
                      <div className="p-4 rounded-xl border border-dashed border-border text-muted-foreground/80 text-xs font-bold italic text-center">
                        No upstream sources defined
                      </div>
                    )}
                  </div>
                </div>

                {/* Consumers */}
                <div>
                  <h4 className="flex items-center gap-2 text-xs font-black uppercase text-muted-foreground/80 mb-4 tracking-widest">
                    <Users size={12} /> Downstream Consumers
                  </h4>
                  <div className="space-y-3">
                    {data.consumers?.map((con, i) => (
                      <div
                        key={i}
                        className="p-4 rounded-xl border border-border bg-muted/50 flex items-center justify-between group/item hover:border-border transition-colors"
                      >
                        <div className="flex items-center gap-3 overflow-hidden">
                          <div
                            className={`w-2 h-2 rounded-full shrink-0 ${con.criticality === "HIGH" ? "bg-rose-400 shadow-[0_0_8px_rgba(251,113,133,0.4)]" : "bg-orange-400"} `}
                          />
                          <div className="overflow-hidden">
                            <div className="font-bold text-foreground/90 text-sm truncate">
                              {con.name}
                            </div>
                            <div className="text-[10px] text-muted-foreground/80 font-bold uppercase truncate">
                              {con.type} • {con.owner}
                            </div>
                          </div>
                        </div>
                        <span
                          className={`text-[10px] font-black px-2 py-0.5 rounded shrink-0 ${con.criticality === "HIGH"
                            ? "bg-rose-100 text-rose-600"
                            : "bg-muted text-muted-foreground"
                            } `}
                        >
                          {con.criticality}
                        </span>
                      </div>
                    ))}
                    {(!data.consumers || data.consumers.length === 0) && (
                      <div className="p-4 rounded-xl border border-dashed border-border text-muted-foreground/80 text-xs font-bold italic text-center">
                        No downstream consumers
                      </div>
                    )}
                  </div>
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
};

// --- New Component: Global Stats Ribbon ---
const StatsRibbon = ({ stats }) => {
  if (!stats) return null;
  return (
    <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
      <div className="bg-card rounded-2xl border border-border p-5 flex items-center gap-4 shadow-sm hover:shadow-md transition-shadow">
        <div className="p-3 bg-orange-50 text-orange-600 rounded-xl">
          <Activity size={24} />
        </div>
        <div>
          <div className="text-xs font-bold text-muted-foreground/80 uppercase tracking-wider">
            Total Runs (Today)
          </div>
          <div className="text-2xl font-black text-foreground">
            {stats.total_runs_today}
          </div>
        </div>
      </div>
      <div className="bg-card rounded-2xl border border-border p-5 flex items-center gap-4 shadow-sm hover:shadow-md transition-shadow">
        <div
          className={`p-3 rounded-xl ${stats.pass_rate_today >= 90 ? "bg-green-50 text-green-600" : "bg-amber-50 text-amber-600"} `}
        >
          <CheckCircle2 size={24} />
        </div>
        <div>
          <div className="text-xs font-bold text-muted-foreground/80 uppercase tracking-wider">
            Pass Rate
          </div>
          <div
            className={`text-2xl font-black ${stats.pass_rate_today >= 90 ? "text-green-600" : "text-amber-600"} `}
          >
            {stats.pass_rate_today}%
          </div>
        </div>
      </div>
      <div className="bg-card rounded-2xl border border-border p-5 flex items-center gap-4 shadow-sm hover:shadow-md transition-shadow">
        <div className="p-3 bg-orange-50 text-orange-600 rounded-xl">
          <Clock size={24} />
        </div>
        <div>
          <div className="text-xs font-bold text-muted-foreground/80 uppercase tracking-wider">
            Avg Duration
          </div>
          <div className="text-2xl font-black text-foreground">
            {stats.avg_duration} ms
          </div>
        </div>
      </div>
    </div>
  );
};

// --- New Component: Expanded Row Detail ---
const ExpandedRowDetail = ({ datasetName, pulseData }) => {
  const [activeTab, setActiveTab] = useState("quality");
  const [metrics, setMetrics] = useState(null);
  const [history, setHistory] = useState([]);
  const [lineage, setLineage] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchData = async () => {
      setLoading(true);
      try {
        const [mRes, hRes, lRes] = await Promise.all([
          getDatasetMetrics(datasetName),
          getHistory(datasetName),
          getLineage(datasetName),
        ]);
        setMetrics(mRes.data);
        setHistory(hRes.data);
        setLineage(lRes.data);
      } catch (e) {
        console.error("Failed to load details", e);
      } finally {
        setLoading(false);
      }
    };
    fetchData();
  }, [datasetName]);

  if (loading)
    return (
      <div className="p-8 flex justify-center">
        <Loader2 className="animate-spin text-muted-foreground/40" />
      </div>
    );

  return (
    <div className="bg-card rounded-xl border border-border shadow-sm overflow-hidden">
      {/* Tabs */}
      <div className="flex border-b border-border bg-muted/50">
        {[
          { id: "quality", label: "Data Quality", icon: Microscope },
          { id: "anomaly", label: "Anomalies & Violations", icon: Activity },
          { id: "governance", label: "Governance & History", icon: FileText },
          { id: "lineage", label: "Impact Lineage", icon: Share2 },
        ].map((tab) => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
            className={`px-6 py-3 flex items-center gap-2 text-xs font-bold uppercase tracking-wider transition-colors border-b-2 ${activeTab === tab.id
              ? "border-primary text-primary bg-card"
              : "border-transparent text-muted-foreground/80 hover:text-muted-foreground hover:bg-muted/50"
              } `}
          >
            <tab.icon size={14} /> {tab.label}
          </button>
        ))}
      </div>

      {/* Content */}
      <div className="p-6 min-h-[300px]">
        {activeTab === "quality" && (
          <div className="space-y-5">
            <div className="flex items-center gap-3 mb-2">
              <div className="text-xs font-bold text-muted-foreground/80 uppercase bg-muted px-3 py-1 rounded-full">
                Last Scanned:{" "}
                {metrics?.run_timestamp
                  ? new Date(metrics.run_timestamp).toLocaleString()
                  : "N/A"}
              </div>
            </div>

            {/* 6-Dimensional Quality Radar Chart */}
            <QualityRadarChart datasetName={datasetName} />

            {/* Schema Validation with Expandable Violations */}
            <SchemaValidationTable datasetName={datasetName} />

            <div className="grid grid-cols-1 lg:grid-cols-2 gap-5">
              <ColumnQualityBars datasetName={datasetName} />
              <QualityScoreTrend datasetName={datasetName} />
            </div>
            <NullRateHeatmap datasetName={datasetName} />
          </div>
        )}

        {activeTab === "anomaly" && (
          <div className="space-y-5">
            <ConstraintViolations datasetName={datasetName} />
            <VolumeAnomalyChart datasetName={datasetName} />
            <DriftChart datasetName={datasetName} metricName="mean_amount" />
          </div>
        )}

        {activeTab === "governance" && (
          <div className="space-y-6">
            {/* Contract Governance - YAML Editor with AI Chat */}
            <ContractGovernance datasetName={datasetName} />

            {/* Scan History */}
            <div className="bg-white rounded-xl border border-slate-200 overflow-hidden">
              <div className="flex items-center gap-2 p-5 pb-4 bg-muted/50 border-b border-border">
                <div className="p-1.5 bg-primary/10 text-primary rounded-lg">
                  <History size={14} />
                </div>
                <h4 className="text-sm font-black uppercase text-slate-600 tracking-wider">
                  Scan History
                </h4>
                <span className="ml-auto text-[10px] font-bold text-slate-400 bg-slate-100 px-2 py-0.5 rounded-full">
                  Last 10 runs
                </span>
              </div>
              <div className="overflow-x-auto">
                <table className="w-full">
                  <thead className="bg-slate-50 text-left text-xs font-black uppercase text-slate-500">
                    <tr>
                      <th className="p-3 pl-6">Run Timestamp</th>
                      <th className="p-3">Status</th>
                      <th className="p-3">Quality Score</th>
                      <th className="p-3">Anomalies</th>
                      <th className="p-3">Reason</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-100 bg-white">
                    {history.slice(0, 10).map((run, i) => (
                      <tr
                        key={i}
                        className="hover:bg-slate-50/50 transition-colors"
                      >
                        <td className="p-3 pl-6 font-medium text-slate-700 text-sm">
                          {new Date(run.timestamp).toLocaleString()}
                        </td>
                        <td className="p-3">
                          <span
                            className={`px-2.5 py-1 rounded text-xs font-black uppercase ${run.status === "PASSED"
                              ? "bg-emerald-100 text-emerald-700 border border-emerald-200"
                              : "bg-rose-100 text-rose-700 border border-rose-200"
                              } `}
                          >
                            {run.status}
                          </span>
                        </td>
                        <td className="p-3 font-bold text-slate-700 text-sm">
                          {run.quality_score
                            ? `${run.quality_score.toFixed(1)}%`
                            : "-"}
                        </td>
                        <td className="p-3 text-slate-600 text-sm font-medium">
                          {run.anomaly_count || 0}
                        </td>
                        <td
                          className="p-3 text-slate-600 truncate max-w-xs text-sm"
                          title={run.reason}
                        >
                          {run.reason || "-"}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        )}

        {activeTab === "lineage" && (
          <div className="h-[400px] border border-border rounded-xl overflow-hidden">
            <LineageTab
              pulseData={pulseData}
              lineageGraph={lineage}
              embedded={true}
            />
          </div>
        )}
      </div>
    </div>
  );
};

const App = () => {
  const [activeTab, setActiveTab] = useState("health");
  const [isCopilotOpen, setIsCopilotOpen] = useState(false);
  const [pulseData, setPulseData] = useState([]);
  const [allDatasets, setAllDatasets] = useState([]);
  const [loading, setLoading] = useState(true);

  // New State
  const [isProposeModalOpen, setIsProposeModalOpen] = useState(false);
  const [selectedDataset, setSelectedDataset] = useState(null);
  const [jsonViewerData, setJsonViewerData] = useState({
    isOpen: false,
    data: null,
    title: "",
  });

  const [isProfileOpen, setIsProfileOpen] = useState(false);
  const [profileData, setProfileData] = useState(null);
  const [systemHealth, setSystemHealth] = useState([]);
  const [lineageGraph, setLineageGraph] = useState(null);
  const [historyData, setHistoryData] = useState([]);

  // Phase 8 State
  const [globalStats, setGlobalStats] = useState(null);
  const [expandedRows, setExpandedRows] = useState(new Set());

  // Phase 9 State
  const [isSidebarCollapsed, setIsSidebarCollapsed] = useState(true);
  const [scanningDatasets, setScanningDatasets] = useState(new Set());
  const [previewDataset, setPreviewDataset] = useState(null);

  const [isDarkMode, setIsDarkMode] = useState(() => {
    const saved = localStorage.getItem("theme");
    return saved === "dark" || (!saved && window.matchMedia("(prefers-color-scheme: dark)").matches);
  });

  useEffect(() => {
    if (isDarkMode) {
      document.documentElement.classList.add("dark");
      localStorage.setItem("theme", "dark");
    } else {
      document.documentElement.classList.remove("dark");
      localStorage.setItem("theme", "light");
    }
  }, [isDarkMode]);

  const [chatMessages, setChatMessages] = useState([
    {
      role: "assistant",
      content:
        "Hello! I am monitoring the pipeline. Ask me anything about the recent runs.",
    },
  ]);
  const [userInput, setUserInput] = useState("");
  const [isChatting, setIsChatting] = useState(false);
  const chatEndRef = useRef(null);

  useEffect(() => {
    fetchInitialData();
    const interval = setInterval(fetchPulse, 10000); // refresh every 10s
    return () => clearInterval(interval);
  }, []);

  useEffect(() => {
    chatEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [chatMessages]);

  const fetchInitialData = async () => {
    setLoading(true);
    try {
      const [pulseRes, dsRes, healthRes, lineageRes, runsRes, statsRes] =
        await Promise.all([
          getPulse(),
          getDatasets(),
          getSystemHealth(),
          getLineage(),
          getRecentRuns(),
          getGlobalStats(),
        ]);
      setPulseData(pulseRes.data);
      setAllDatasets(dsRes.data);
      setSystemHealth(healthRes.data);
      setLineageGraph(lineageRes.data);
      setHistoryData(runsRes.data);
      setGlobalStats(statsRes.data);
    } catch (err) {
      console.error("Initial fetch failed", err);
    } finally {
      setLoading(false);
    }
  };

  const fetchPulse = async () => {
    try {
      const [pulseRes, runsRes, statsRes] = await Promise.all([
        getPulse(),
        getRecentRuns(),
        getGlobalStats(),
      ]);
      setPulseData(pulseRes.data);
      setHistoryData(runsRes.data);
      setGlobalStats(statsRes.data);
    } catch (err) {
      console.error("Failed to fetch pulse", err);
    }
  };

  const toggleExpand = (name) => {
    const newSet = new Set(expandedRows);
    if (newSet.has(name)) newSet.delete(name);
    else newSet.add(name);
    setExpandedRows(newSet);
  };

  const handleProposeSave = async (data) => {
    try {
      await saveContract(data); // Assuming saveContract is imported or we use endpoint
      // Actually we need to call the API directly or use a wrapper.
      // Let's use the API function directly if imported, or fetch.
      // We need to import saveContract first.
      // Wait, let's just implement the fetch here to be safe or assuming onSave does it in the modal?
      // The modal calls onSave. We need to pass a handler that calls the API.

      const response = await fetch("http://localhost:8000/contracts/save", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(data),
      });
      if (!response.ok) throw new Error("Failed to save");

      alert("Contract saved successfully! Re-scanning...");
      handleRunCheck(data.dataset_name);
    } catch (err) {
      console.error("Failed to save contract", err);
      alert("Failed to save: " + err.message);
      throw err; // Propagate to modal
    }
  };

  const handleOpenProfile = async (datasetName) => {
    try {
      // In a real app we might show loading state specific to this action
      const res = await getDatasetProfile(datasetName);
      setProfileData(res.data);
      setIsProfileOpen(true);
    } catch (err) {
      console.error("Failed to get profile", err);
      alert("Failed to fetch profile data. Ensure dataset file exists.");
    }
  };

  const handleRunCheck = async (name) => {
    setScanningDatasets((prev) => new Set(prev).add(name));
    try {
      await evaluateDataset(name);
      await fetchPulse();
    } catch (err) {
      console.error("Evaluation failed", err);
    } finally {
      setScanningDatasets((prev) => {
        const next = new Set(prev);
        next.delete(name);
        return next;
      });
    }
  };

  const handleSmartScan = async () => {
    setLoading(true);
    try {
      // Run checks for all datasets
      // In a real production app, this should be a single backend async job
      const promises = allDatasets.map((ds) => evaluateDataset(ds.name));
      await Promise.all(promises);
      await fetchPulse();
    } catch (err) {
      console.error("Smart Scan failed", err);
      alert("Smart Scan encountered errors. Check console for details.");
    } finally {
      setLoading(false);
    }
  };

  const handleSendMessage = async () => {
    if (!userInput.trim() || isChatting) return;

    const query = userInput;
    setUserInput("");
    setChatMessages((prev) => [...prev, { role: "user", content: query }]);
    setIsChatting(true);

    try {
      const res = await chatWithCopilot(query);
      setChatMessages((prev) => [
        ...prev,
        { role: "assistant", content: res.data.response },
      ]);
    } catch (err) {
      setChatMessages((prev) => [
        ...prev,
        {
          role: "assistant",
          content: "Sorry, I encountered an error processing your request.",
        },
      ]);
    } finally {
      setIsChatting(false);
    }
  };

  const trustScore =
    pulseData.length > 0
      ? (
        (pulseData.filter((d) => d.status === "PASSED").length /
          pulseData.length) *
        100
      ).toFixed(1)
      : "N/A";

  const getFreshness = () => {
    if (!pulseData.length) return "N/A";
    const scans = pulseData
      .map((d) => (d.last_scanned ? new Date(d.last_scanned).getTime() : 0))
      .filter((t) => t > 0);
    if (!scans.length) return "N/A";
    const diff = Date.now() - Math.max(...scans);
    const mins = Math.floor(diff / 60000);
    if (mins < 1) return "< 1m";
    if (mins < 60) return `${mins}m`;
    return `${Math.floor(mins / 60)}h`;
  };

  return (
    <div className="flex h-screen bg-background overflow-hidden font-sans text-foreground">
      {/* Sidebar */}
      <aside
        className={`${isSidebarCollapsed ? "w-20" : "w-64"} bg-slate-950 border-r border-slate-900 flex flex-col z-20 transition-all duration-300 ease-in-out relative`}
      >
        {/* Collapse Toggle */}
        <button
          onClick={() => setIsSidebarCollapsed(!isSidebarCollapsed)}
          className="absolute -right-3 top-9 bg-card border border-border rounded-full p-1 text-muted-foreground hover:text-primary hover:border-primary shadow-sm z-50"
        >
          {isSidebarCollapsed ? (
            <ChevronRight size={14} />
          ) : (
            <ChevronLeft size={14} />
          )}
        </button>

        <div
          className={`p-6 ${isSidebarCollapsed ? "flex justify-center" : ""} `}
        >
          <div className="flex items-center gap-3 text-white font-black text-2xl tracking-tighter">
            <div className="w-10 h-10 bg-slate-900 rounded-xl flex items-center justify-center text-primary border border-slate-800 shadow-lg shadow-black/20 flex-shrink-0">
              <ShieldCheck size={24} />
            </div>
            {!isSidebarCollapsed && <span>DRE.ai</span>}
          </div>
        </div>

        <nav className="flex-1 px-4 space-y-2 mt-4">
          {[
            { id: "health", icon: Activity, label: "Schema Health" },
            { id: "datasets", icon: Database, label: "Datasets" },
            { id: "incidents", icon: AlertCircle, label: "Incidents" },
            { id: "history", icon: History, label: "Run History" },
            { id: "lineage", icon: Link, label: "Data Lineage" },
            { id: "connections", icon: Network, label: "Connections" },
            { id: "settings", icon: Settings, label: "Settings" },
          ].map((item) => (
            <button
              key={item.id}
              onClick={() => setActiveTab(item.id)}
              className={`w-full flex items-center gap-3 px-3 py-3 rounded-xl transition-all duration-200 group ${activeTab === item.id
                ? "bg-white/10 text-white font-bold shadow-sm"
                : "text-slate-400 hover:bg-white/5 hover:text-white"
                } ${isSidebarCollapsed ? "justify-center" : ""} `}
              title={isSidebarCollapsed ? item.label : ""}
            >
              <item.icon
                size={20}
                className={`${activeTab === item.id ? "stroke-[3px]" : "group-hover:text-primary transition-colors"} flex-shrink-0`}
              />
              {!isSidebarCollapsed && (
                <span className="text-sm">{item.label}</span>
              )}
              {!isSidebarCollapsed && activeTab === item.id && (
                <ChevronRight size={14} className="ml-auto" />
              )}
            </button>
          ))}
        </nav>

        <div className="p-6">
          {!isSidebarCollapsed ? (
            <div className="bg-secondary text-secondary-foreground p-5 rounded-2xl shadow-xl relative overflow-hidden group">
              <div
                className={`absolute - right-4 - top-4 w-20 h-20 rounded-full blur-2xl transition-all duration-500 ${systemHealth.every((s) => s.upstream === "UP")
                  ? "bg-green-500/20 group-hover:bg-green-500/40"
                  : "bg-rose-500/20 group-hover:bg-rose-500/40"
                  } `}
              />
              <div className="relative z-10">
                <div className="flex items-center gap-2 mb-3">
                  <Zap
                    size={14}
                    className={
                      systemHealth.every((s) => s.upstream === "UP")
                        ? "text-green-400 fill-green-400/80"
                        : "text-rose-400 fill-rose-400/80"
                    }
                  />
                  <span className="text-[10px] font-black uppercase tracking-[0.2em]">
                    System Status
                  </span>
                </div>
                <div className="flex items-center gap-3">
                  <div
                    className={`w-2.5 h-2.5 rounded-full shadow-[0_0_10px_rgba(74, 222, 128, 0.5)] animate-pulse ${systemHealth.every((s) => s.upstream === "UP")
                      ? "bg-green-400"
                      : "bg-rose-500"
                      } `}
                  />
                  <span className="text-sm font-bold tracking-tight">
                    {systemHealth.every((s) => s.upstream === "UP")
                      ? "All Operations Up"
                      : "System Degradation"}
                  </span>
                </div>
              </div>
            </div>
          ) : (
            <div className="flex justify-center">
              <div
                className={`w-3 h-3 rounded-full animate-pulse ${systemHealth.every((s) => s.upstream === "UP") ? "bg-green-500 shadow-[0_0_10px_rgba(34,197,94,0.5)]" : "bg-rose-500 shadow-[0_0_10px_rgba(244,63,94,0.5)]"} `}
                title="System Status"
              />
            </div>
          )}
        </div>
      </aside>

      {/* Main Content */}
      <main className="flex-1 flex flex-col overflow-hidden relative">
        <ContractWizardModal
          isOpen={isProposeModalOpen}
          onClose={() => setIsProposeModalOpen(false)}
          datasetName={selectedDataset}
          dataset={allDatasets.find(d => d.name === selectedDataset)}
          onSave={handleProposeSave}
        />

        <JsonViewerModal
          isOpen={jsonViewerData.isOpen}
          onClose={() =>
            setJsonViewerData({ ...jsonViewerData, isOpen: false })
          }
          data={jsonViewerData.data}
          title={jsonViewerData.title}
        />

        <DataPreviewModal
          isOpen={!!previewDataset}
          onClose={() => setPreviewDataset(null)}
          datasetName={previewDataset}
        />

        <ProfileModal
          isOpen={isProfileOpen}
          onClose={() => setIsProfileOpen(false)}
          data={profileData}
        />

        <header className="h-20 border-b border-border bg-background/80 backdrop-blur-md flex items-center justify-between px-10 sticky top-0 z-10">
          <div>
            <h2 className="text-xl font-black text-foreground tracking-tight">
              {activeTab === "health" && "Schema Health Pulse"}
              {activeTab === "datasets" && "Datasets Overview"}
              {activeTab === "incidents" && "Incident Feed"}
              {activeTab === "history" && "System Run History"}
              {activeTab === "lineage" && "Data Lineage Graph"}
              {activeTab === "connections" && "Source Integrations"}
            </h2>
            <p className="text-[10px] text-muted-foreground font-bold uppercase tracking-widest mt-1">
              Active Environment: Production-Lakehouse
            </p>
          </div>

          <div className="flex items-center gap-4">
            {loading && (
              <div className="flex items-center gap-2 text-xs text-primary font-bold animate-pulse">
                <Loader2 size={14} className="animate-spin" />
                Agent Executing...
              </div>
            )}
            <button
              onClick={handleSmartScan}
              disabled={loading}
              className="bg-slate-900 text-white px-6 py-2.5 rounded-xl text-sm font-black hover:bg-slate-800 transition-all shadow-lg shadow-black/20 flex items-center gap-2 active:scale-95 disabled:opacity-70 disabled:pointer-events-none border border-slate-700/50"
            >
              <Zap size={16} className="text-primary fill-primary" />
              Smart Scan All
            </button>
            <button
              onClick={() => setIsDarkMode(!isDarkMode)}
              className="p-3 rounded-xl border border-border bg-card text-foreground hover:border-primary/50 transition-all duration-300 shadow-sm"
              title={isDarkMode ? "Switch to light mode" : "Switch to dark mode"}
            >
              {isDarkMode ? <Sun size={20} className="text-amber-500" /> : <Moon size={20} className="text-muted-foreground" />}
            </button>
            <div className="w-px h-8 bg-border mx-2" />
            <button
              onClick={() => setIsCopilotOpen(!isCopilotOpen)}
              className={`p-3 rounded-xl border transition-all duration-300 relative group ${isCopilotOpen
                ? "bg-primary/10 border-primary text-primary"
                : "bg-card border-border text-muted-foreground hover:border-primary/50"
                } `}
            >
              <MessageSquare
                size={20}
                className={isCopilotOpen ? "fill-primary/20" : ""}
              />
              {!isCopilotOpen && (
                <div className="absolute -top-1 -right-1 w-3 h-3 bg-primary rounded-full border-2 border-white" />
              )}
            </button>
          </div>
        </header>

        <div className="flex-1 overflow-y-auto p-10 space-y-10 custom-scrollbar">
          {activeTab === "health" && (
            <>
              {/* Global Run Stats */}
              <StatsRibbon stats={globalStats} />

              {/* KPI Cards */}
              <div className="grid grid-cols-1 md:grid-cols-3 gap-8">
                <StatCard
                  title="Pipeline Trust Score"
                  value={`${trustScore}% `}
                  subtext="Dynamic accuracy based on active runs"
                  icon={ShieldCheck}
                  color="bg-teal-500"
                />
                <StatCard
                  title="Data Freshness"
                  value={getFreshness()}
                  subtext="SLA Threshold: 30m"
                  icon={Clock}
                  color="bg-primary"
                />
                <StatCard
                  title="Active Anomalies"
                  value={pulseData
                    .filter((d) => d.status !== "PASSED")
                    .length.toString()}
                  subtext="Critical incidents requiring attention"
                  icon={AlertCircle}
                  color="bg-rose-500"
                />
              </div>

              {/* Dataset Pulse Table */}
              <div className="bg-card rounded-3xl border border-border shadow-soft overflow-hidden">
                <div className="px-8 py-6 border-b border-border bg-muted/30 flex justify-between items-center text-foreground">
                  <div className="flex items-center gap-3">
                    <div className="p-2 bg-primary/10 rounded-lg">
                      <Activity size={20} className="text-primary" />
                    </div>
                    <h3 className="font-extrabold text-lg tracking-tight text-foreground">
                      Active Dataset Pulse
                    </h3>
                  </div>
                </div>

                <div className="overflow-x-auto text-foreground">
                  <table className="w-full text-left border-collapse">
                    <thead>
                      <tr className="text-muted-foreground/80 text-[11px] uppercase tracking-[0.15em] font-black border-b border-border">
                        <th className="px-8 py-5 text-muted-foreground/80">
                          Dataset Name
                        </th>
                        <th className="px-8 py-5 text-muted-foreground/80">Lifecycle</th>
                        <th className="px-8 py-5 text-muted-foreground/80">Quality</th>
                        <th className="px-8 py-5 text-muted-foreground/80">
                          Health Verdict
                        </th>
                        <th className="px-8 py-5 text-muted-foreground/80">
                          Criticality
                        </th>
                        <th className="px-8 py-5 text-muted-foreground/80">
                          Quality Trend
                        </th>
                        <th className="px-8 py-5 text-right text-muted-foreground/80">
                          Actions
                        </th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-border/50 text-foreground">
                      {pulseData.length > 0 ? (
                        pulseData.map((row, i) => (
                          <React.Fragment key={row.name}>
                            <motion.tr
                              initial={{ opacity: 0, y: 10 }}
                              animate={{ opacity: 1, y: 0 }}
                              transition={{ delay: i * 0.1 }}
                              onClick={() => toggleExpand(row.name)}
                              className={`hover: bg-slate - 50 / 80 transition-all group cursor-pointer ${expandedRows.has(row.name) ? "bg-muted/50" : ""} `}
                            >
                              <td className="px-8 py-6">
                                <div className="flex items-center gap-3">
                                  <div
                                    className={`transition-transform duration-200 ${expandedRows.has(row.name) ? "rotate-90" : ""} `}
                                  >
                                    <ChevronRight
                                      size={16}
                                      className="text-muted-foreground/80"
                                    />
                                  </div>
                                  <div
                                    className={`w-2 h-2 rounded-full ${row.status === "PASSED" ? "bg-green-500" : row.status === "BLOCKED" ? "bg-rose-500" : "bg-amber-500"} `}
                                  />
                                  <span className="font-bold text-sm tracking-tight text-foreground/90">
                                    {row.name}
                                  </span>
                                </div>
                              </td>
                              <td className="px-8 py-6">
                                <span className="text-xs font-bold text-muted-foreground bg-muted px-2 py-0.5 rounded-md">
                                  {row.lifecycle}
                                </span>
                              </td>
                              <td className="px-8 py-6">
                                <span
                                  className={`font-black ${row.quality_score >= 90 ? "text-green-500" : row.quality_score >= 70 ? "text-amber-500" : "text-rose-500"} `}
                                >
                                  {row.quality_score?.toFixed(1) || "100.0"}%
                                </span>
                              </td>
                              <td className="px-8 py-6">
                                <span
                                  className={`px-3 py-1 rounded-full text-[10px] font-black uppercase tracking-wider ${row.status === "PASSED"
                                    ? "bg-green-100 text-green-700 shadow-sm border border-green-200"
                                    : row.status === "BLOCKED"
                                      ? "bg-rose-100 text-rose-700 shadow-sm border border-rose-200"
                                      : "bg-amber-100 text-amber-700 shadow-sm border border-amber-200"
                                    } `}
                                >
                                  {row.status}
                                </span>
                              </td>
                              <td className="px-8 py-6">
                                <span className="text-[11px] font-black text-muted-foreground">
                                  {row.criticality}
                                </span>
                              </td>
                              <td className="px-8 py-6 w-48">
                                {row.history && row.history.length > 0 && (
                                  <div className="h-8 w-full">
                                    <ResponsiveContainer
                                      width="100%"
                                      height="100%"
                                    >
                                      <LineChart
                                        data={row.history.map((h, idx) => ({
                                          h,
                                          idx,
                                        }))}
                                      >
                                        <Line
                                          type="monotone"
                                          dataKey="h"
                                          stroke={
                                            row.status === "PASSED"
                                              ? "#10b981"
                                              : "#f43f5e"
                                          }
                                          strokeWidth={3}
                                          dot={false}
                                        />
                                      </LineChart>
                                    </ResponsiveContainer>
                                  </div>
                                )}
                              </td>
                              <td className="px-8 py-6 text-right">
                                <div className="flex items-center justify-end gap-2">
                                  {row.status === "BLOCKED" && (
                                    <button
                                      onClick={(e) => {
                                        e.stopPropagation();
                                        setSelectedDataset(row.name);
                                        setIsProposeModalOpen(true);
                                      }}
                                      className="flex items-center gap-1.5 px-3 py-1.5 bg-rose-50 text-rose-600 rounded-lg text-xs font-bold hover:bg-rose-100 transition-colors border border-rose-100"
                                    >
                                      <Stethoscope size={14} />
                                      Propose Fix
                                    </button>
                                  )}
                                  <button
                                    onClick={(e) => {
                                      e.stopPropagation();
                                      setPreviewDataset(row.name);
                                    }}
                                    className="text-muted-foreground/80 hover:text-primary p-2 hover:bg-primary/5 rounded-lg transition-colors"
                                    title="View Data Preview"
                                  >
                                    <Table size={16} />
                                  </button>
                                  <button
                                    onClick={(e) => {
                                      e.stopPropagation();
                                      setJsonViewerData({
                                        isOpen: true,
                                        data: row,
                                        title: `Monitor Output: ${row.name}`,
                                      });
                                    }}
                                    className="text-muted-foreground/80 hover:text-primary p-2 hover:bg-primary/5 rounded-lg transition-colors"
                                    title="View Raw Monitor JSON"
                                  >
                                    <Code size={16} />
                                  </button>
                                  <button
                                    onClick={(e) => {
                                      e.stopPropagation();
                                      handleRunCheck(row.name);
                                    }}
                                    disabled={scanningDatasets.has(row.name)}
                                    className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-bold transition-all border group / btn ${scanningDatasets.has(row.name)
                                      ? "bg-primary/5 text-primary border-primary/20 cursor-wait"
                                      : "bg-muted/50 text-muted-foreground border-border hover:bg-card hover:text-primary hover:shadow-sm"
                                      } `}
                                  >
                                    {scanningDatasets.has(row.name) ? (
                                      <>
                                        <Loader2
                                          size={14}
                                          className="animate-spin"
                                        />
                                        Scanning...
                                      </>
                                    ) : (
                                      <>
                                        <Zap
                                          size={14}
                                          className="fill-current group-hover/btn:text-primary"
                                        />
                                        Run Scan
                                      </>
                                    )}
                                  </button>
                                </div>
                              </td>
                            </motion.tr>
                            <AnimatePresence>
                              {expandedRows.has(row.name) && (
                                <motion.tr
                                  initial={{ opacity: 0 }}
                                  animate={{ opacity: 1 }}
                                  exit={{ opacity: 0 }}
                                  key={`${row.name} -detail`}
                                >
                                  <td
                                    colSpan="7"
                                    className="p-0 border-b border-border bg-muted/30"
                                  >
                                    <div className="p-4 pl-12 border-t border-dashed border-border">
                                      <ExpandedRowDetail
                                        datasetName={row.name}
                                        pulseData={pulseData}
                                      />
                                    </div>
                                  </td>
                                </motion.tr>
                              )}
                            </AnimatePresence>
                          </React.Fragment>
                        ))
                      ) : loading ? (
                        <tr>
                          <td colSpan="7" className="px-8 py-20 text-center">
                            <div className="flex flex-col items-center gap-4 text-muted-foreground">
                              <Loader2
                                size={32}
                                className="animate-spin text-primary"
                              />
                              <p className="font-bold text-sm">
                                Discovering Datasets & Analyzing Health...
                              </p>
                            </div>
                          </td>
                        </tr>
                      ) : (
                        <tr>
                          <td colSpan="7" className="px-8 py-20 text-center">
                            <div className="flex flex-col items-center gap-4 text-muted-foreground">
                              <div className="p-4 bg-muted/50 rounded-full">
                                <Search size={24} className="text-muted-foreground/40" />
                              </div>
                              <p className="font-bold text-sm">
                                No active monitored datasets found.
                              </p>
                              <button
                                onClick={() => setActiveTab("datasets")}
                                className="text-xs text-primary font-bold hover:underline"
                              >
                                Go to Datasets to configure sources
                              </button>
                            </div>
                          </td>
                        </tr>
                      )}
                    </tbody>
                  </table>
                </div>
              </div>
            </>
          )}

          {activeTab === "datasets" && (
            <DatasetsTab
              datasets={allDatasets}
              onProfile={handleOpenProfile}
              onGenerateContract={(name) => {
                setSelectedDataset(name);
                setIsProposeModalOpen(true);
              }}
              previewDataset={previewDataset}
              setPreviewDataset={setPreviewDataset}
            />
          )}
          {activeTab === "incidents" && (
            <div className="bg-card border border-border rounded-3xl p-6 min-h-[500px]">
              <IncidentFeed />
            </div>
          )}
          {activeTab === "history" && (
            <HistoryTab datasets={allDatasets} history={historyData} />
          )}
          {activeTab === "lineage" && (
            <LineageTab pulseData={pulseData} lineageGraph={lineageGraph} />
          )}
          {activeTab === "connections" && <ConnectionsTab />}
          {activeTab === "settings" && (
            <div className="flex flex-col items-center justify-center p-20 bg-card border border-border rounded-3xl text-foreground">
              <Settings size={64} className="text-muted-foreground/20 mb-6" />
              <h3 className="text-xl font-bold">Platform Configuration</h3>
              <p className="text-muted-foreground mt-2">
                Manage API keys, alerting thresholds, and model selection.
              </p>
              <button className="mt-8 px-6 py-2 bg-secondary text-secondary-foreground rounded-xl font-bold">
                Edit System YAML
              </button>
            </div>
          )}
        </div>
      </main>

      {/* Copilot Sidebar */}
      <AnimatePresence>
        {isCopilotOpen && (
          <motion.aside
            initial={{ x: 400, opacity: 0 }}
            animate={{ x: 0, opacity: 1 }}
            exit={{ x: 400, opacity: 0 }}
            transition={{ type: "spring", damping: 25, stiffness: 200 }}
            className="w-[400px] bg-card border-l border-border flex flex-col shadow-[-20px_0_40px_rgba(0,0,0,0.02)] z-30"
          >
            <div className="p-8 border-b border-border bg-muted/50 text-foreground">
              <div className="flex items-center justify-between mb-6">
                <div className="flex items-center gap-3">
                  <div className="w-10 h-10 rounded-2xl bg-slate-900 shadow-lg shadow-black/10 flex items-center justify-center text-primary border border-slate-800">
                    <Zap size={20} className="fill-primary" />
                  </div>
                  <div>
                    <h3 className="font-black text-foreground text-sm tracking-tight">
                      DRE Copilot
                    </h3>
                    <div className="flex items-center gap-1.5 mt-0.5">
                      <div className="w-1.5 h-1.5 rounded-full bg-green-500 shadow-[0_0_5px_rgba(34,197,94,0.5)]" />
                      <span className="text-[10px] text-muted-foreground font-bold uppercase tracking-widest">
                        Active reasoning
                      </span>
                    </div>
                  </div>
                </div>
                <button
                  onClick={() => setIsCopilotOpen(false)}
                  className="p-2 hover:bg-muted/80 rounded-lg transition-colors text-muted-foreground/80"
                >
                  <ChevronRight size={20} />
                </button>
              </div>

              <div className="bg-card p-4 rounded-2xl border border-border shadow-sm">
                <p className="text-[10px] font-black text-muted-foreground/80 uppercase tracking-widest mb-2">
                  Capabilities
                </p>
                <div className="grid grid-cols-2 gap-2">
                  {["Root Cause", "Lineage", "Remediation", "Drift"].map(
                    (c) => (
                      <div
                        key={c}
                        className="bg-muted/50 px-2.5 py-1.5 rounded-lg text-[10px] font-bold text-muted-foreground border border-border flex items-center gap-1.5"
                      >
                        <div className="w-1 h-1 rounded-full bg-primary" />
                        {c}
                      </div>
                    ),
                  )}
                </div>
              </div>
            </div>

            <div className="flex-1 p-8 overflow-y-auto space-y-6 scrollbar-hide text-foreground">
              {chatMessages.map((msg, idx) => (
                <div
                  key={idx}
                  className={`flex ${msg.role === "user" ? "justify-end" : "justify-start"} `}
                >
                  <div
                    className={`max-w-[85 %] p-4 rounded-2xl text-sm leading-relaxed shadow-sm ${msg.role === "user"
                      ? "bg-primary text-white font-medium rounded-tr-none"
                      : "bg-muted text-foreground/90 font-normal rounded-tl-none border border-border/50"
                      } `}
                  >
                    {msg.content}
                  </div>
                </div>
              ))}
              {isChatting && (
                <div className="flex justify-start">
                  <div className="bg-muted p-4 rounded-2xl rounded-tl-none border border-border/50 flex gap-1">
                    <div className="w-1 h-1 bg-muted rounded-full animate-bounce [animation-delay:-0.3s]" />
                    <div className="w-1 h-1 bg-muted rounded-full animate-bounce [animation-delay:-0.15s]" />
                    <div className="w-1 h-1 bg-muted rounded-full animate-bounce" />
                  </div>
                </div>
              )}
              <div ref={chatEndRef} />
            </div>

            <div className="p-8 bg-card border-t border-border">
              <div className="relative group">
                <textarea
                  rows="1"
                  value={userInput}
                  onChange={(e) => setUserInput(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === "Enter" && !e.shiftKey) {
                      e.preventDefault();
                      handleSendMessage();
                    }
                  }}
                  placeholder="Ask about data health or lineage..."
                  className="w-full bg-muted/50 border border-border rounded-2xl pl-5 pr-12 py-4 text-sm focus:outline-none focus:ring-4 focus:ring-primary/10 focus:border-primary transition-all text-foreground placeholder:text-muted-foreground/80 resize-none font-medium"
                />
                <button
                  onClick={handleSendMessage}
                  disabled={!userInput.trim() || isChatting}
                  className="absolute right-3 top-3 p-2 bg-primary text-white rounded-xl shadow-lg shadow-primary/30 hover:scale-105 active:scale-95 transition-all disabled:opacity-50 disabled:shadow-none"
                >
                  <Send size={18} fill="currentColor" />
                </button>
              </div>
            </div>
          </motion.aside>
        )}
      </AnimatePresence>
    </div>
  );
};

export default App;
