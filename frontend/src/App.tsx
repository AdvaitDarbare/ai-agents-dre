import { useEffect, useState } from 'react';
import axios from 'axios';
import {
  Card,
  Metric,
  Text,
  Badge,
  Flex,
  Title,
  Tracker,
  Table,
  TableHead,
  TableRow,
  TableHeaderCell,
  TableBody,
  TableCell,
  ProgressBar,
  Grid,
  Button,
  Callout,
  List,
  ListItem,
  type Color
} from '@tremor/react';
import {
  ExclamationTriangleIcon,
  DocumentIcon,
  TableCellsIcon,
  ChevronDownIcon,
  ChartBarIcon,
  ShieldCheckIcon,
  ListBulletIcon,
  ArrowTrendingUpIcon,
  CheckCircleIcon,
  TagIcon
} from '@heroicons/react/24/solid';

// --- Types ---
interface Violation {
  column: string;
  issue: string;
  severity: string;
  expected: string;
  actual: string;
}

interface StatsProfile {
  mean?: number;
  min?: number;
  max?: number;
  std?: number;
  null_pct?: number;
  unique_pct?: number;
}

interface QualityMetric {
  score: number;
  status: string;
}

interface ColumnHealth {
  health_score: number;
  issues: string[];
  dtype: string;
}

interface QualityMetrics {
  overall_health_score: number;
  metrics: {
    freshness?: QualityMetric;
    completeness?: QualityMetric;
    validity?: QualityMetric;
    uniqueness?: QualityMetric;
  };
  column_health?: Record<string, ColumnHealth>;
}

interface HealthIndicator {
  status: string;
  score: number;
  risk_assessment: string;
  recommendations: string[];
}

interface DriftResult {
  status: string;
  drift_warnings: string[];
  baseline_period: string;
  summary: string;
}

interface ExecutionStep {
  tool: string;
  result: {
    status: string;
    violations?: Violation[];
    rows_loaded?: number;
    columns?: string[];
    preview?: any[];
    drift_warnings?: string[]; // For Drift Tool
    summary?: string;
    baseline_period?: string;
  };
  decision: string;
  timestamp: string;
}

interface TablePriority {
  priority_tier: string;
  context: string;
}

interface Report {
  file: string;
  report_filename: string;
  status: string;
  timestamp: string;
  execution_log: ExecutionStep[];
  critical_errors?: string[];
  stats_summary?: Record<string, StatsProfile>;
  quality_metrics?: QualityMetrics;
  health_indicator?: HealthIndicator;
  table_priority?: TablePriority;
  table_name: string;
  inferred_contract?: string;
  active_contract?: string;
}

function App() {
  const [report, setReport] = useState<Report | null>(null);
  const [reportsList, setReportsList] = useState<string[]>([]);
  const [selectedReport, setSelectedReport] = useState<string>('');
  const [error, setError] = useState<string>('');

  // Full Data State
  const [fullData, setFullData] = useState<any[]>([]);
  const [isLoadingMore, setIsLoadingMore] = useState(false);
  const [fullDataError, setFullDataError] = useState('');

  // Contract Approval State
  const [isApproving, setIsApproving] = useState(false);
  const [editedContract, setEditedContract] = useState('');

  const handleApproveContract = async () => {
    if (!report || !editedContract) return;
    setIsApproving(true);
    try {
      // 1. Save Contract
      await axios.post('http://localhost:8000/api/contracts', {
        table_name: report.table_name,
        yaml_content: editedContract
      });

      // 2. Trigger Pipeline Re-Run
      // Note: This calls the CLI via backend subprocess
      const runRes = await axios.post('http://localhost:8000/api/pipeline/run', {
        file_path: report.file,
        table_name: report.table_name
      });

      if (runRes.data.status === 'SUCCESS') {
        alert("✅ Contract Saved & Validation Passed! Refreshing report...");
        // Force refresh of report list to pick up the new run
        const listRes = await axios.get('http://localhost:8000/api/reports');
        setReportsList(listRes.data);
        if (listRes.data.length > 0) setSelectedReport(listRes.data[0]);
      } else {
        // Even if it failed validation (e.g. strict mode or data errors), a report was generated.
        // We should refresh anyway to show the failure details.
        alert("⚠️ Validation ran with errors. Loading report...");
        const listRes = await axios.get('http://localhost:8000/api/reports');
        setReportsList(listRes.data);
        if (listRes.data.length > 0) setSelectedReport(listRes.data[0]);
      }

    } catch (e: any) {
      alert("Error processing request: " + e.message);
    } finally {
      setIsApproving(false);
    }
  };

  // Sync edited contract when report changes
  useEffect(() => {
    if (report?.status === 'CONTRACT_MISSING' && report.inferred_contract) {
      setEditedContract(report.inferred_contract);
    } else if (report?.active_contract) {
      setEditedContract(report.active_contract);
    }
  }, [report]);

  const [contractHistory, setContractHistory] = useState<any[]>([]);
  // Fetch history when report table name availability
  useEffect(() => {
    if (report?.table_name) {
      axios.get(`http://localhost:8000/api/contracts/${report.table_name}/history`)
        .then(res => setContractHistory(res.data))
        .catch(_ => setContractHistory([]));
    }
  }, [report]);

  const handleRestoreVersion = async (filename: string, version: string) => {
    if (!confirm(`Restore contract to version ${version}? Current changes will be overwritten in the editor.`)) return;

    try {
      const res = await axios.get(`http://localhost:8000/api/contracts/archive/${filename}`);
      if (res.data.status === 'SUCCESS') {
        setEditedContract(res.data.content);
        // alert(`Version ${version} loaded into editor. Click 'Update Contract' to save and apply.`);
      } else {
        alert("Error: " + res.data.message);
      }
    } catch (e: any) {
      alert("Error restoring version: " + e.message);
    }
  };

  // 1. Fetch List
  useEffect(() => {
    axios.get('http://localhost:8000/api/reports')
      .then(res => {
        setReportsList(res.data);
        if (res.data.length > 0) {
          setSelectedReport(res.data[0]);
        } else {
          // Stop loading state if no data
          setReport({ status: 'NO_DATA' } as any);
        }
      })
      .catch(err => setError(err.message));
  }, []);

  // 2. Fetch Report
  useEffect(() => {
    if (!selectedReport) return;
    setFullData([]);
    setFullDataError('');

    axios.get(`http://localhost:8000/api/health?report=${selectedReport}`)
      .then(res => {
        setReport(res.data);
        setError('');
      })
      .catch(err => {
        console.error(err);
        setError(err.message);
      });
  }, [selectedReport]);

  // 3. Load Full Data
  const loadFullData = () => {
    if (!report?.file) return;
    setIsLoadingMore(true);
    setFullDataError('');

    axios.get(`http://localhost:8000/api/file-content?path=${report.file}`)
      .then(res => {
        if (res.data.status === 'OK') {
          setFullData(res.data.data);
        } else {
          setFullDataError(res.data.message);
        }
      })
      .catch(err => setFullDataError(err.message))
      .finally(() => setIsLoadingMore(false));
  };

  if (error) return <div className="p-10 text-red-500">Error: {error}</div>;
  if (!report) return <div className="p-10 flex justify-center text-slate-500">Loading...</div>;

  // --- Derived Data ---
  const isPass = report.status === 'PASS';
  const color = isPass ? 'emerald' : 'rose';

  const loaderStep = report.execution_log?.find(s => s.tool === 'DataLoaderTool');
  const schemaStep = report.execution_log?.find(s => s.tool === 'SchemaValidatorTool');
  const driftStep = report.execution_log?.find(s => s.tool === 'DriftCheckTool');

  const rows = loaderStep?.result?.rows_loaded ?? 'N/A';
  const cols = Array.isArray(loaderStep?.result?.columns) ? loaderStep.result.columns.length : 'N/A';
  const violations = schemaStep?.result?.violations || [];

  const showFullData = fullData.length > 0;
  const displayData = showFullData ? fullData : (loaderStep?.result?.preview || []);
  const previewColumns = displayData.length > 0 ? Object.keys(displayData[0]) : [];

  const healthScore = report.health_indicator?.score ?? report.quality_metrics?.overall_health_score;
  const metrics = report.quality_metrics?.metrics;
  const stats = report.stats_summary || {};
  const colHealth = report.quality_metrics?.column_health || {};

  // Tracker Logic
  // Tacker Logic - Build Full Pipeline View
  const allTools = [
    'FileMetadataTool',
    'DataLoaderTool',
    'SchemaValidatorTool',
    'ConsistencyCheckTool',
    'StatsAnalysisTool',
    'DriftCheckTool',
    'QualityMetricsTool',
    'HealthIndicator'
  ];

  const allTrackerData = allTools.map((toolName) => {
    const step = report.execution_log?.find(s => s.tool === toolName);
    if (step) {
      let c: Color = 'emerald';
      if (step.result?.status === 'FAIL' || step.decision === 'CRITICAL_STOP') c = 'rose';
      return { color: c, tooltip: `${toolName}: ${step.decision}` };
    }

    // Implicit Execution Check
    if (toolName === 'QualityMetricsTool' && report.quality_metrics) {
      return { color: 'emerald', tooltip: `${toolName}: Completed` };
    }
    if (toolName === 'HealthIndicator' && report.health_indicator) {
      return { color: 'emerald', tooltip: `${toolName}: Completed` };
    }

    return { color: 'slate' as Color, tooltip: `${toolName}: Skipped` };
  });

  // Synthesize Recommendations if missing (FAIL scenario)
  let finalRecommendations: string[] = [];
  if (report.health_indicator?.recommendations && report.health_indicator.recommendations.length > 0) {
    finalRecommendations = report.health_indicator.recommendations;
  } else if (report.status === 'FAIL' && report.critical_errors) {
    finalRecommendations.push("CRITICAL: Pipeline stopped due to validation errors.");
    report.critical_errors.forEach(err => finalRecommendations.push(`Action Required: ${err}`));
    if (violations.length > 0) {
      finalRecommendations.push(`Review the ${violations.length} schema violations below.`);
    }
  }

  return (
    <main className="p-10 bg-slate-50 min-h-screen font-sans">

      {/* HEADER */}
      <Flex className="mb-8" justifyContent="between" alignItems="start">
        <div>
          <Title className="text-3xl font-bold text-slate-800 tracking-tight">Data Reliability Monitor</Title>
          <Flex justifyContent="start" className="mt-2 space-x-4 text-slate-500 items-center">
            <Flex justifyContent="start" className="space-x-2">
              <DocumentIcon className="w-4 h-4" />
              <Text className="font-mono text-sm font-medium text-slate-600">{report.file}</Text>
            </Flex>
            {report.table_priority && (
              <Badge size="xs" color="gray" icon={TagIcon}>
                {report.table_priority.priority_tier} Priority
              </Badge>
            )}
          </Flex>
        </div>
        <div className="text-right space-y-2">
          <div className="relative inline-block text-left w-64">
            <select
              value={selectedReport}
              onChange={(e) => setSelectedReport(e.target.value)}
              className="block w-full rounded-md border-0 py-1.5 pl-3 pr-10 text-slate-900 ring-1 ring-inset ring-slate-300 focus:ring-2 focus:ring-blue-600 sm:text-sm sm:leading-6 bg-white shadow-sm cursor-pointer outline-none font-medium"
            >
              {reportsList.map((r) => <option key={r} value={r}>{r}</option>)}
            </select>
            <div className="pointer-events-none absolute inset-y-0 right-0 flex items-center px-2 text-slate-500">
              <ChevronDownIcon className="h-4 w-4" />
            </div>
          </div>
          <Flex justifyContent="end" className="space-x-3">
            <Badge size="md" color={color}>{report.status}</Badge>
            <Text className="text-xs text-slate-400 font-mono">{new Date(report.timestamp).toLocaleString()}</Text>
          </Flex>
        </div>
      </Flex>

      {/* KPI GRID */}
      <Grid numItems={1} numItemsSm={2} numItemsLg={4} className="gap-6 mb-8">
        <Card decoration="top" decorationColor="blue">
          <Text>Total Rows</Text>
          <Metric>{rows}</Metric>
        </Card>
        <Card decoration="top" decorationColor="indigo">
          <Text>Total Columns</Text>
          <Metric>{cols}</Metric>
        </Card>

        {healthScore !== undefined && (
          <Card decoration="top" decorationColor={healthScore > 80 ? 'emerald' : 'orange'}>
            <Flex justifyContent="start" alignItems="center" className="space-x-2">
              <Text>Health Score</Text>
              <ShieldCheckIcon className="w-4 h-4 text-slate-400" />
            </Flex>
            <Metric>{healthScore.toFixed(1)}</Metric>
          </Card>
        )}

        {report.health_indicator?.risk_assessment && (
          <Card decoration="top" decorationColor={report.health_indicator.risk_assessment === 'Low' ? 'emerald' : 'amber'}>
            <Text>Risk Level</Text>
            <Metric>{report.health_indicator.risk_assessment}</Metric>
          </Card>
        )}
      </Grid>

      {/* DRAFT CONTRACT UI (New) */}
      {report.status === 'CONTRACT_MISSING' && report.inferred_contract && (
        <Card className="mb-8 border-l-4 border-blue-500 ring-1 ring-slate-200 shadow-sm bg-blue-50/10">
          <Flex justifyContent="between" alignItems="start">
            <div>
              <Title className="text-blue-900 font-bold mb-2 flex items-center gap-2">
                <DocumentIcon className="w-5 h-5" />
                Contract Missing: Draft Generated
              </Title>
              <Text className="text-blue-700 font-medium mb-4">
                No active contract found for table <code>{report.table_name}</code>.
                The AI has inferred a schema based on data profiling.
              </Text>
            </div>
            <Button
              size="md"
              color="blue"
              icon={CheckCircleIcon}
              onClick={handleApproveContract}
              loading={isApproving}
            >
              Approve & Save Contract
            </Button>
          </Flex>

          <textarea
            value={editedContract}
            onChange={(e) => setEditedContract(e.target.value)}
            className="w-full h-96 bg-slate-900 text-emerald-400 font-mono text-xs p-4 rounded-md border border-slate-700 focus:ring-2 focus:ring-blue-500 outline-none mt-4 shadow-inner"
            spellCheck={false}
          />

          <div className="mt-4 p-3 bg-blue-100/50 text-blue-800 text-sm rounded-md border border-blue-200 flex items-center shadow-sm">
            <span className="mr-2 text-xl">💡</span>
            <span>Review the draft above. Click <b>Approve</b> to save <code>contracts/{report.table_name}.yaml</code> and enable validation.</span>
          </div>
        </Card>
      )}

      {/* ACTIVE CONTRACT UI */}
      {report.status !== 'CONTRACT_MISSING' && report.active_contract && (
        <Card className="mb-8 ring-1 ring-slate-200 shadow-sm bg-slate-50/50">
          <Flex justifyContent="between" alignItems="center" className="mb-4">
            <Title className="flex items-center gap-2">
              <ShieldCheckIcon className="w-5 h-5 text-emerald-600" />
              Active Data Contract
              <Badge color="emerald" size="xs">Live</Badge>
              {(() => {
                const m = editedContract.match(/version:\s*([^\s]+)/);
                return m ? <Badge color="blue" size="xs" icon={TagIcon}>v{m[1].trim()}</Badge> : null;
              })()}
            </Title>
            <Button
              size="xs"
              variant="secondary"
              color="gray"
              icon={CheckCircleIcon}
              onClick={handleApproveContract}
              loading={isApproving}
            >
              Update Contract & Re-Validate
            </Button>
          </Flex>

          <div className="grid grid-cols-3 gap-6">
            {/* EDITOR (2/3) */}
            <div className="col-span-2">
              <textarea
                value={editedContract}
                onChange={(e) => setEditedContract(e.target.value)}
                className="w-full h-80 bg-slate-900 text-emerald-400 font-mono text-xs p-4 rounded-md border border-slate-700 focus:ring-2 focus:ring-blue-500 outline-none shadow-inner"
                spellCheck={false}
              />
            </div>

            {/* HISTORY (1/3) */}
            <div className="col-span-1 bg-white rounded-md border border-slate-200 p-4 h-80 overflow-y-auto shadow-inner bg-slate-50/50">
              <Text className="font-bold mb-3 text-slate-700 border-b pb-2 text-sm">Version Lineage</Text>
              {contractHistory.length === 0 ? (
                <div className="flex flex-col items-center justify-center h-40 text-slate-400">
                  <DocumentIcon className="w-8 h-8 mb-2 opacity-20" />
                  <Text className="text-xs italic">No previous versions.</Text>
                </div>
              ) : (
                <div className="space-y-4 pl-1 pt-2">
                  {contractHistory.map((h, idx) => {
                    // Look for version in editor
                    const currentMatch = editedContract.match(/version:\s*([^\s]+)/);
                    const currentVersion = currentMatch ? currentMatch[1].trim() : '';

                    // Simple ordering: Newest (idx=0) is Version N. Oldest (idx=len-1) is Version 1.
                    const simpleVersion = contractHistory.length - idx;
                    const isCurrent = h.version === currentVersion;

                    return (
                      <div
                        key={idx}
                        className={`border-l-2 pl-3 relative group cursor-pointer transition-all py-3 pr-2 rounded-r-md mb-2 ${isCurrent ? 'border-blue-500 bg-blue-50' : 'border-slate-200 hover:border-blue-300 hover:bg-slate-50'}`}
                        onClick={() => handleRestoreVersion(h.filename, h.version)}
                        title={isCurrent ? "Currently Active" : "Restore this version"}
                      >
                        <div className={`absolute -left-[5px] top-4 w-2 h-2 rounded-full ring-2 ring-white ${isCurrent ? 'bg-blue-500' : 'bg-slate-300'}`} />

                        <Flex justifyContent="between" alignItems="center">
                          <Text className={`text-sm font-bold ${isCurrent ? 'text-blue-700' : 'text-slate-700'}`}>Version {simpleVersion}</Text>
                          {isCurrent && <Badge size="xs" color="blue">Active</Badge>}
                        </Flex>

                        <Text className="text-[10px] text-slate-400 font-mono mt-1">
                          {h.timestamp.replace('_', ' ')} • v{h.version}
                        </Text>
                      </div>
                    );
                  })}
                </div>
              )}
            </div>
          </div>
        </Card>
      )
      }

      {/* RECOMMENDATIONS (Inferred or Explicit) */}
      {
        finalRecommendations.length > 0 && (
          <Callout title="Agent Recommendations" color={isPass ? "blue" : "rose"} icon={isPass ? CheckCircleIcon : ExclamationTriangleIcon} className="mb-8">
            <List className="mt-2 text-slate-700">
              {finalRecommendations.map((rec, idx) => (
                <ListItem key={idx}>• {rec}</ListItem>
              ))}
            </List>
          </Callout>
        )
      }

      {/* DRIFT ANALYSIS (If Tool Ran) */}
      {
        driftStep && (
          <Card className="mb-8 ring-1 ring-slate-200">
            <Flex justifyContent="start" className="space-x-2 mb-2">
              <ArrowTrendingUpIcon className="w-5 h-5 text-slate-500" />
              <Title>Drift Analysis</Title>
            </Flex>
            <Text className="text-slate-500 mb-4 text-sm">Baseline: {driftStep.result?.baseline_period}</Text>

            {driftStep.result?.drift_warnings && driftStep.result.drift_warnings.length > 0 ? (
              <Callout title="Drift Detected" color="amber" icon={ExclamationTriangleIcon}>
                <List>
                  {driftStep.result.drift_warnings.map((w, idx) => <ListItem key={idx}>{w}</ListItem>)}
                </List>
              </Callout>
            ) : (
              <Flex justifyContent="start" className="space-x-2 text-emerald-600 bg-emerald-50 p-3 rounded-md">
                <CheckCircleIcon className="w-5 h-5" />
                <Text className="text-emerald-700 font-medium">No significant distribution drift detected.</Text>
              </Flex>
            )}
          </Card>
        )
      }

      {/* QUALITY METRICS (Conditional) */}
      {
        metrics && (
          <Card className="mb-8 p-6 ring-1 ring-slate-200">
            <Title className="mb-4">Quality Dimensions</Title>
            <Grid numItems={1} numItemsSm={2} numItemsLg={4} className="gap-6">
              {['freshness', 'completeness', 'validity', 'uniqueness'].map((key) => {
                const m = metrics[key as keyof typeof metrics];
                if (!m) return null;
                return (
                  <div key={key}>
                    <Flex justifyContent="between" className="mb-2">
                      <Text className="capitalize">{key}</Text>
                      <Text className="font-bold">{m.score.toFixed(1)}%</Text>
                    </Flex>
                    <ProgressBar value={m.score} color={m.score > 90 ? 'emerald' : 'yellow'} />
                  </div>
                );
              })}
            </Grid>
          </Card>
        )
      }

      {/* STATS SUMMARY (Conditional) */}
      {
        Object.keys(stats).length > 0 && (
          <Card className="mb-8 ring-1 ring-slate-200">
            <Flex justifyContent="start" className="space-x-2 mb-4">
              <ChartBarIcon className="w-5 h-5 text-slate-400" />
              <Title>Statistical Profile</Title>
            </Flex>
            <div className="max-h-96 overflow-y-auto">
              <Table>
                <TableHead>
                  <TableRow className="bg-slate-50 sticky top-0 z-10 shadow-sm">
                    <TableHeaderCell>Column</TableHeaderCell>
                    <TableHeaderCell>Health</TableHeaderCell>
                    <TableHeaderCell>Mean</TableHeaderCell>
                    <TableHeaderCell>Min</TableHeaderCell>
                    <TableHeaderCell>Max</TableHeaderCell>
                    <TableHeaderCell>Std Dev</TableHeaderCell>
                    <TableHeaderCell>Null %</TableHeaderCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {Object.entries(stats).map(([col, data]) => (
                    <TableRow key={col} className="hover:bg-slate-50">
                      <TableCell className="font-medium text-slate-700">{col}</TableCell>
                      <TableCell>
                        {colHealth[col] ? (
                          <Badge size="xs" color={colHealth[col].health_score > 90 ? 'emerald' : 'amber'}>
                            {colHealth[col].health_score.toFixed(0)}
                          </Badge>
                        ) : '-'}
                      </TableCell>
                      <TableCell>{data.mean?.toFixed(2) ?? '-'}</TableCell>
                      <TableCell>{data.min?.toFixed(2) ?? '-'}</TableCell>
                      <TableCell>{data.max?.toFixed(2) ?? '-'}</TableCell>
                      <TableCell>{data.std?.toFixed(2) ?? '-'}</TableCell>
                      <TableCell>{data.null_pct?.toFixed(1)}%</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          </Card>
        )
      }

      {/* VIOLATIONS (Conditional) */}
      {
        violations.length > 0 && (
          <Card className="mb-8 p-0 overflow-hidden ring-1 ring-rose-200">
            <div className="p-4 border-b border-rose-100 bg-rose-50/50">
              <Flex justifyContent="start" alignItems="center" className="space-x-2">
                <ExclamationTriangleIcon className="w-5 h-5 text-rose-500" />
                <Title className="text-rose-900 font-semibold">Schema Violations Detected</Title>
                <Badge color="rose" size="xs">{violations.length} Issues</Badge>
              </Flex>
            </div>
            <Table className="mt-0">
              <TableHead>
                <TableRow className="bg-rose-50/20">
                  <TableHeaderCell>Column</TableHeaderCell>
                  <TableHeaderCell>Issue</TableHeaderCell>
                  <TableHeaderCell>Severity</TableHeaderCell>
                  <TableHeaderCell>Expected</TableHeaderCell>
                  <TableHeaderCell>Actual</TableHeaderCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {violations.map((v, idx) => (
                  <TableRow key={idx} className="hover:bg-slate-50 transition-colors">
                    <TableCell className="font-mono text-slate-700 font-medium">{v.column}</TableCell>
                    <TableCell>{v.issue}</TableCell>
                    <TableCell>
                      <Badge color={v.severity === 'CRITICAL' ? 'rose' : 'amber'} size="xs">
                        {v.severity}
                      </Badge>
                    </TableCell>
                    <TableCell className="text-slate-500 text-xs">{v.expected}</TableCell>
                    <TableCell className="text-slate-700 font-mono text-xs">{v.actual}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </Card>
        )
      }

      {/* DATA CONTENT */}
      {
        displayData.length > 0 && (
          <Card className="mb-8 ring-1 ring-slate-200 shadow-sm">
            <Flex justifyContent="between" alignItems="center" className="mb-4">
              <Flex justifyContent="start" className="space-x-2">
                <TableCellsIcon className="w-5 h-5 text-slate-400" />
                <Title>Dataset Content</Title>
                <Badge size="xs" color="gray">
                  {showFullData ? `All ${fullData.length} Rows (Up to 2000)` : `First ${displayData.length} Rows`}
                </Badge>
              </Flex>
              {!showFullData && (
                <Button
                  size="xs"
                  variant="secondary"
                  icon={ListBulletIcon}
                  onClick={loadFullData}
                  loading={isLoadingMore}
                >
                  Load Full Dataset
                </Button>
              )}
            </Flex>
            {fullDataError && (
              <div className="mb-4 p-3 bg-rose-50 text-rose-700 text-sm rounded-md border border-rose-200">
                Failed to load full data: {fullDataError}
              </div>
            )}
            <div className={`overflow-auto border rounded-md border-slate-200 ${showFullData ? 'max-h-96' : ''}`}>
              <Table className="mt-0">
                <TableHead>
                  <TableRow className="bg-slate-50 sticky top-0 z-10 shadow-sm">
                    {previewColumns.map((col) => (
                      <TableHeaderCell key={col} className="bg-slate-50">{col}</TableHeaderCell>
                    ))}
                  </TableRow>
                </TableHead>
                <TableBody>
                  {displayData.map((row, idx) => (
                    <TableRow key={idx} className="hover:bg-slate-50 transition-colors">
                      {previewColumns.map((col) => (
                        <TableCell key={col}>
                          {row[col] === null ? <span className="text-slate-300 italic">null</span> : String(row[col])}
                        </TableCell>
                      ))}
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          </Card>
        )
      }

      {/* PIPELINE TRACKER */}
      <Card className="ring-1 ring-slate-200 shadow-sm">
        <Title>Pipeline Execution Journey</Title>
        <Text className="mb-4 text-slate-500">Visual timeline of the agent's decision process.</Text>
        <Tracker data={allTrackerData} className="w-full" />
        <div className="mt-4 flex flex-wrap gap-4">
          {allTrackerData.map((step, idx) => (
            <div key={idx} className="flex items-center space-x-2 text-xs">
              <div className={`w-2 h-2 rounded-full ${step.color === 'emerald' ? 'bg-emerald-500' : step.color === 'rose' ? 'bg-rose-500' : 'bg-slate-300'}`} />
              <span className={`font-semibold ${step.color === 'slate' ? 'text-slate-400' : 'text-slate-700'}`}>
                {step.tooltip.split(':')[0]}
              </span>
            </div>
          ))}
        </div>
      </Card>

    </main >
  );
}

export default App;
