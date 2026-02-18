'use client';

import { useEffect, useMemo, useState } from 'react';
import { useConfirmDialog } from '@/dre/components/confirm-dialog';
import {
  Activity,
  ChevronRight,
  Database,
  GitBranch,
  History,
  Link as LinkIcon,
  MessageSquare,
  Network,
  RefreshCw,
  Settings,
  ShieldCheck,
  Zap,
} from 'lucide-react';

import CopilotPanel from '@/dre/copilot-panel';
import ConnectionsPanel from '@/dre/components/connections-panel';
import ContractWizardModal from '@/dre/components/contract-wizard-modal';
import IncidentsPanel from '@/dre/components/incidents-panel';
import JobActivity from '@/dre/components/job-activity';
import DatasetsTable from '@/dre/components/datasets-table';
import LineagePanel from '@/dre/components/lineage-panel';
import PendingContractsBar from '@/dre/components/pending-contracts-bar';
import PulseTable from '@/dre/components/pulse-table';
import RunsTable from '@/dre/components/runs-table';
import DatasetPreviewModal from '@/dre/components/dataset-preview-modal';
import ProfileModal from '@/dre/components/profile-modal';
import SettingsPanel from '@/dre/components/settings-panel';
import SummaryCards from '@/dre/components/summary-cards';
import SystemHealthPanel from '@/dre/components/system-health-panel';
import SystemStatsRibbon from '@/dre/components/system-stats-ribbon';
import PlatformOpsPanel from '@/dre/components/platform-ops-panel';
import { buildDeepDiveEnvelope } from '@/dre/copilot-constants';
import {
  ApiError,
  enqueueEvaluateAllJob,
  enqueueDeleteJob,
  enqueueScanJob,
  getBaselines,
  getDatasetMetrics,
  getDatasetPreview,
  getHistory,
  getJob,
  getSloSummary,
  getWorkflowTimeline,
  updateIncident,
} from '@/lib/dre-api';
import { useDashboardData } from '@/dre/data/use-dashboard-data';

type MainTab = 'health' | 'datasets' | 'history' | 'lineage' | 'connections' | 'settings';
type BusyState = Record<string, 'scan' | 'delete'>;

function tabTitle(tab: MainTab): string {
  if (tab === 'health') return 'Schema Health Pulse';
  if (tab === 'datasets') return 'Datasets Overview';
  if (tab === 'history') return 'Run History & Issues';
  if (tab === 'lineage') return 'Data Lineage Graph';
  if (tab === 'connections') return 'Source Integrations';
  return 'Platform Configuration';
}

export default function Dashboard() {
  const [activeTab, setActiveTab] = useState<MainTab>('health');
  const [isSidebarCollapsed, setIsSidebarCollapsed] = useState(false);
  const [selectedDataset, setSelectedDataset] = useState<string | null>(null);
  const [copilotOpen, setCopilotOpen] = useState(false);
  const [copilotInitialMessage, setCopilotInitialMessage] = useState<{ id: string; text: string } | null>(null);
  const [previewOpen, setPreviewOpen] = useState(false);
  const [profileOpen, setProfileOpen] = useState(false);
  const [wizardOpen, setWizardOpen] = useState(false);
  const [wizardDataset, setWizardDataset] = useState<string | null>(null);
  const [wizardFilePath, setWizardFilePath] = useState<string | null>(null);
  const [localBusy, setLocalBusy] = useState<BusyState>({});
  const [manualRefreshing, setManualRefreshing] = useState(false);
  const [refreshToken, setRefreshToken] = useState(0);
  const { dialog: confirmDialog, confirm } = useConfirmDialog();

  const { pulse, datasets, pending, jobs, runs, incidents, lineage, globalStats, systemHealth, busy, loading, refresh } = useDashboardData({
    pollIntervalMs: 15_000,
    jobsLimit: 40,
    runsLimit: 120,
    incidentsLimit: 120,
  });

  useEffect(() => {
    if (selectedDataset) return;
    if (pulse.length === 0) return;
    setSelectedDataset(pulse[0].name);
  }, [pulse, selectedDataset]);

  const contractActionCount = useMemo(() => {
    const datasetNames = new Set(datasets.map((ds) => ds.name));
    const unmanaged = datasets.filter((ds) => ds.lifecycle === 'unconfigured').map((ds) => ds.name);
    const pendingOnly = pending.filter((proposal) => !datasetNames.has(proposal.dataset_name)).map((proposal) => proposal.dataset_name);
    return new Set([...unmanaged, ...pendingOnly]).size;
  }, [datasets, pending]);
  const allSystemsUp = useMemo(() => {
    if (systemHealth.length === 0) return true;
    return systemHealth.every((row) => String((row.upstream || {}).status || '').toUpperCase() === 'UP');
  }, [systemHealth]);

  const mergedBusy = useMemo<BusyState>(() => ({ ...busy, ...localBusy }), [busy, localBusy]);
  const historySummary = useMemo(() => {
    const openIncidents = incidents.filter((item) => String(item.status || '').toUpperCase() === 'OPEN').length;
    const criticalOpen = incidents.filter(
      (item) =>
        String(item.status || '').toUpperCase() === 'OPEN' &&
        String(item.severity || '').toUpperCase() === 'CRITICAL',
    ).length;
    const blockedRuns = runs.filter((run) => String(run.status || '').toUpperCase() === 'BLOCKED').length;
    const warningRuns = runs.filter((run) => String(run.status || '').toUpperCase() === 'WARNING').length;
    return {
      openIncidents,
      criticalOpen,
      blockedRuns,
      warningRuns,
    };
  }, [incidents, runs]);

  const waitForJob = async (jobId: string, timeoutMs = 5 * 60 * 1000, intervalMs = 1200) => {
    const startedAt = Date.now();
    let loops = 0;
    while (Date.now() - startedAt < timeoutMs) {
      const job = await getJob(jobId);
      if (job.status === 'COMPLETED') return job;
      if (job.status === 'FAILED') throw new Error(job.error || 'Background job failed');
      loops += 1;
      if (loops % 4 === 0) await refresh({ silent: true });
      await new Promise((resolve) => setTimeout(resolve, intervalMs));
    }
    throw new Error('Job timed out while waiting for completion');
  };

  const onRunScan = async (datasetName: string, options?: { forceLoad?: boolean }) => {
    setLocalBusy((prev) => ({ ...prev, [datasetName]: 'scan' }));
    try {
      const job = await enqueueScanJob(datasetName, { forceLoad: options?.forceLoad });
      await refresh({ silent: true });
      await waitForJob(job.job_id, 8 * 60 * 1000, 1200);
      await refresh();
    } catch (error) {
      console.error('Scan failed', error);
      alert(`Scan failed: ${error instanceof Error ? error.message : 'unknown error'}`);
    } finally {
      setLocalBusy((prev) => {
        const next = { ...prev };
        delete next[datasetName];
        return next;
      });
    }
  };

  const onDelete = async (datasetName: string) => {
    const { confirmed } = await confirm({
      title: 'Delete Dataset',
      message: `Delete dataset "${datasetName}" permanently? This action cannot be undone.`,
      confirmLabel: 'Delete',
      variant: 'danger',
    });
    if (!confirmed) return;

    setLocalBusy((prev) => ({ ...prev, [datasetName]: 'delete' }));
    try {
      let job = await enqueueDeleteJob(datasetName);
      await waitForJob(job.job_id, 3 * 60 * 1000, 900);
      await refresh();
    } catch (error) {
      if (error instanceof ApiError && error.status === 409) {
        const detail =
          error.body && typeof error.body === 'object' && 'detail' in error.body
            ? (error.body as { detail?: unknown }).detail
            : null;
        const policyMessage =
          detail && typeof detail === 'object' && 'message' in detail
            ? String((detail as { message?: unknown }).message || '')
            : '';
        const needsPolicyApproval = policyMessage.toLowerCase().includes('policy approval required');

        if (needsPolicyApproval) {
          const policyResult = await confirm({
            title: 'Policy Approval Required',
            message: `Deleting "${datasetName}" requires policy approval for a HIGH/CRITICAL dataset.\n\nPlease provide a reason to proceed.`,
            confirmLabel: 'Approve & Delete',
            variant: 'danger',
            requireReason: true,
            reasonLabel: 'Policy approval reason (required)',
            reasonPlaceholder: 'Manual deletion approved by owner for test cleanup',
          });
          if (!policyResult.confirmed || !policyResult.reason) return;

          try {
            const retryJob = await enqueueDeleteJob(datasetName, {
              policyApproved: true,
              policyReason: policyResult.reason,
            });
            await waitForJob(retryJob.job_id, 3 * 60 * 1000, 900);
            await refresh();
            return;
          } catch (retryError) {
            console.error('Delete failed after policy approval', retryError);
            return;
          }
        }
      }

      console.error('Delete failed', error);
    } finally {
      setLocalBusy((prev) => {
        const next = { ...prev };
        delete next[datasetName];
        return next;
      });
    }
  };

  const onScanAll = async () => {
    try {
      const job = await enqueueEvaluateAllJob({ includeUnconfigured: true });
      await refresh({ silent: true });
      await waitForJob(job.job_id, 20 * 60 * 1000, 1500);
      await refresh();
    } catch (error) {
      console.error('Scan all failed', error);
      alert(`Scan all failed: ${error instanceof Error ? error.message : 'unknown error'}`);
    }
  };

  const onAckIncident = async (incidentId: string) => {
    try {
      await updateIncident(incidentId, { status: 'ACK', note: 'Acknowledged from Next UI' });
      await refresh();
    } catch (error) {
      console.error('Acknowledge incident failed', error);
    }
  };

  const onResolveIncident = async (incidentId: string) => {
    try {
      await updateIncident(incidentId, { status: 'RESOLVED', note: 'Resolved from Next UI' });
      await refresh();
    } catch (error) {
      console.error('Resolve incident failed', error);
    }
  };

  const onManualRefresh = async () => {
    setManualRefreshing(true);
    try {
      await refresh();
      setRefreshToken((prev) => prev + 1);
    } finally {
      setManualRefreshing(false);
    }
  };

  const onDeepDive = async (datasetName: string) => {
    setSelectedDataset(datasetName);
    setCopilotOpen(true);

    const [
      historyResult,
      metricsResult,
      baselinesResult,
      sloResult,
      previewResult,
      timelineResult,
    ] = await Promise.allSettled([
      getHistory(datasetName, 8),
      getDatasetMetrics(datasetName),
      getBaselines(datasetName),
      getSloSummary(datasetName, 120),
      getDatasetPreview(datasetName, 5),
      getWorkflowTimeline({ dataset_name: datasetName, limit: 80 }),
    ]);

    const history = historyResult.status === 'fulfilled' ? historyResult.value : [];
    const metrics = metricsResult.status === 'fulfilled' ? metricsResult.value : null;
    const baselines = baselinesResult.status === 'fulfilled' ? baselinesResult.value : [];
    const slo = sloResult.status === 'fulfilled' ? sloResult.value : null;
    const preview = previewResult.status === 'fulfilled' ? previewResult.value : null;
    const timeline = timelineResult.status === 'fulfilled' ? timelineResult.value : null;

    const latestRun = Array.isArray(history) && history.length > 0 ? history[0] : null;
    const metricsList = metrics?.metrics
      ? Object.entries(metrics.metrics)
        .slice(0, 12)
        .map(([name, value]) => ({ name, value }))
      : [];
    const baselineList = Array.isArray(baselines)
      ? baselines.slice(0, 10).map((row) => ({
        metric: row.metric,
        mean: row.mean,
        std: row.std,
        upper_3sigma: row.upper_3sigma,
        lower_3sigma: row.lower_3sigma,
      }))
      : [];
    const toolAndRunEvents = Array.isArray(timeline?.events)
      ? timeline.events
        .filter((event) => {
          const channel = String(event.channel || '').toLowerCase();
          return channel === 'tool' || channel === 'run';
        })
        .slice(0, 12)
        .map((event) => ({
          channel: event.channel,
          event: event.event,
          status: event.status,
          message: event.message,
          timestamp: event.timestamp,
        }))
      : [];

    const deepDiveContext = {
      dataset: datasetName,
      latest_run: latestRun,
      recent_runs: Array.isArray(history) ? history.slice(0, 5) : [],
      metrics: metricsList,
      baselines: baselineList,
      slo_summary: slo,
      sample_data: preview
        ? {
          columns: preview.columns?.slice(0, 20),
          rows: preview.data?.slice(0, 5),
          total_rows: preview.total_rows,
        }
        : null,
      workflow_events: toolAndRunEvents,
      workflow_summary: timeline?.summary || null,
    };

    const prompt = buildDeepDiveEnvelope({
      visible_prompt: `Deep dive analysis for dataset: ${datasetName}`,
      hidden_context: deepDiveContext,
    });

    setCopilotInitialMessage({
      id: `${datasetName}-${Date.now()}`,
      text: prompt,
    });
  };

  return (
    <div className="app-shell flex h-screen bg-background overflow-hidden text-foreground">
      {confirmDialog}
      <div className="flex h-screen w-full">
        <aside
          className={`border-r border-border bg-card flex flex-col overflow-hidden transition-[width] duration-150 ease-out ${isSidebarCollapsed ? 'w-20' : 'w-72'
            }`}
        >
          <div className={`px-4 py-5 ${isSidebarCollapsed ? 'flex justify-center' : ''}`}>
            <div className="flex w-full items-center gap-3 text-xl font-semibold tracking-tight">
              <div className="w-9 h-9 rounded-xl bg-foreground text-background flex items-center justify-center shrink-0">
                <ShieldCheck size={20} />
              </div>
              {!isSidebarCollapsed && <span>DataPulse DRE</span>}
              <button
                onClick={() => setIsSidebarCollapsed((prev) => !prev)}
                className={`rounded-md border border-border p-1 text-muted-foreground hover:bg-muted hover:text-foreground ${isSidebarCollapsed ? '' : 'ml-auto'
                  }`}
                aria-label={isSidebarCollapsed ? 'Expand sidebar' : 'Collapse sidebar'}
                title={isSidebarCollapsed ? 'Expand sidebar' : 'Collapse sidebar'}
              >
                <ChevronRight size={16} className={isSidebarCollapsed ? '' : 'rotate-180'} />
              </button>
            </div>
          </div>

          <nav className="flex-1 p-4 space-y-2">
            {(
              [
                { id: 'health', icon: Activity, label: 'Schema Health' },
                { id: 'datasets', icon: Database, label: 'Datasets' },
                { id: 'history', icon: History, label: 'Run History' },
                { id: 'lineage', icon: LinkIcon, label: 'Data Lineage' },
                { id: 'connections', icon: Network, label: 'Connections' },
                { id: 'settings', icon: Settings, label: 'Settings' },
              ] as const
            ).map((item) => (
              <button
                key={item.id}
                onClick={() => setActiveTab(item.id)}
                className={`w-full flex items-center gap-3 px-3 py-3 rounded-xl transition-colors relative ${activeTab === item.id ? 'bg-foreground text-background' : 'text-muted-foreground hover:bg-muted hover:text-foreground'
                  } ${isSidebarCollapsed ? 'justify-center' : ''}`}
                title={isSidebarCollapsed ? item.label : undefined}
              >
                {item.id === 'datasets' && contractActionCount > 0 && (
                  <span className="absolute -top-1 -right-1 bg-amber-500 text-white text-xs rounded-full w-5 h-5 flex items-center justify-center font-medium">
                    {contractActionCount}
                  </span>
                )}
                <item.icon size={18} />
                {!isSidebarCollapsed && <span className="text-sm font-medium">{item.label}</span>}
                {!isSidebarCollapsed && activeTab === item.id && <ChevronRight size={16} className="ml-auto" />}
              </button>
            ))}
          </nav>

          <div className="p-6">
            {!isSidebarCollapsed ? (
              <div className="rounded-2xl border border-border bg-secondary p-5">
                <div className="text-[10px] font-semibold uppercase tracking-[0.2em] text-muted-foreground">System Status</div>
                <div className="mt-3 flex items-center gap-2 text-sm font-medium">
                  <span className={`h-2.5 w-2.5 rounded-full ${allSystemsUp ? 'bg-emerald-500' : 'bg-rose-500'}`} />
                  <span>{allSystemsUp ? 'All Operations Up' : 'System Degradation'}</span>
                </div>
              </div>
            ) : (
              <div className="flex flex-col items-center gap-3">
                <span
                  className={`h-3 w-3 rounded-full ${allSystemsUp ? 'bg-emerald-500' : 'bg-rose-500'}`}
                  title={allSystemsUp ? 'All Operations Up' : 'System Degradation'}
                />
              </div>
            )}
          </div>
        </aside>

        <main className="flex-1 flex flex-col overflow-hidden relative">
          <header className="h-20 border-b border-border bg-background/80 backdrop-blur-md flex items-center justify-between px-4 md:px-8 xl:px-10 sticky top-0 z-10">
            <div className="min-w-0">
              <h1 className="text-xl font-semibold tracking-tight">{tabTitle(activeTab)}</h1>
              <p className="text-[10px] text-muted-foreground uppercase tracking-widest mt-1">Live environment: local-first</p>
            </div>

            <div className="flex items-center gap-3">
              {loading && <div className="text-xs text-muted-foreground animate-pulse">Agent executing...</div>}
              <button
                onClick={() => void onScanAll()}
                className="inline-flex items-center gap-2 rounded-xl border border-border bg-background px-3 py-2 text-sm font-medium transition-colors hover:bg-accent"
              >
                <Zap size={16} />
                Smart Scan All
              </button>
              <button
                onClick={() => void onManualRefresh()}
                disabled={manualRefreshing}
                className="inline-flex items-center gap-2 rounded-xl border border-border bg-background px-3 py-2 text-sm font-medium transition-colors hover:bg-accent"
              >
                <RefreshCw size={16} className={manualRefreshing ? 'animate-spin' : ''} />
                {manualRefreshing ? 'Refreshing...' : 'Refresh'}
              </button>
              <button
                onClick={() => setCopilotOpen((prev) => !prev)}
                className="inline-flex items-center gap-2 rounded-xl bg-foreground text-background px-3 py-2 text-sm font-medium transition-opacity hover:opacity-90"
              >
                <MessageSquare size={16} />
                Copilot
              </button>
            </div>
          </header>

          <div className="flex-1 overflow-y-auto p-4 md:p-8 xl:p-10 space-y-8">
            {activeTab === 'health' && (
              <div className="space-y-8">
                <SystemStatsRibbon stats={globalStats} />
                <SummaryCards datasets={datasets} pulse={pulse} />
                <PulseTable
                  loading={loading}
                  pulse={pulse}
                  busy={mergedBusy}
                  onDeepDive={(datasetName) => void onDeepDive(datasetName)}
                  refreshToken={refreshToken}
                  selectedDataset={selectedDataset}
                  onSelectDataset={setSelectedDataset}
                  onRunScan={(datasetName) => void onRunScan(datasetName)}
                  onPreviewDataset={(name) => {
                    setSelectedDataset(name);
                    setPreviewOpen(true);
                  }}
                  onProfileDataset={(name) => {
                    setSelectedDataset(name);
                    setProfileOpen(true);
                  }}
                  onForceLoad={async (name) => {
                    const { confirmed } = await confirm({
                      title: 'Force Load Dataset',
                      message: `Are you sure you want to force load "${name}"? This will bypass quality checks and data contracts.`,
                      confirmLabel: 'Force Load',
                      variant: 'danger',
                    });
                    if (confirmed) {
                      void onRunScan(name, { forceLoad: true });
                    }
                  }}
                  onAfterDatasetChange={() => void refresh()}
                />
              </div>
            )}

            {activeTab === 'datasets' && (
              <div className="space-y-8">
                <PendingContractsBar pending={pending} datasets={datasets} />
                <DatasetsTable
                  loading={loading}
                  datasets={datasets}
                  pulse={pulse}
                  pending={pending}
                  busy={mergedBusy}
                  selectedDataset={selectedDataset}
                  onSelectDataset={(name) => {
                    setSelectedDataset(name);
                    setActiveTab('health');
                  }}
                  onPreview={(name) => {
                    setSelectedDataset(name);
                    setPreviewOpen(true);
                  }}
                  onProfile={(name) => {
                    setSelectedDataset(name);
                    setProfileOpen(true);
                  }}
                  onRunScan={(datasetName) => void onRunScan(datasetName)}
                  onDelete={(datasetName) => void onDelete(datasetName)}
                  onForceLoad={async (name) => {
                    const { confirmed } = await confirm({
                      title: 'Force Load Dataset',
                      message: `Are you sure you want to force load "${name}"? This will bypass quality checks and data contracts.`,
                      confirmLabel: 'Force Load',
                      variant: 'danger',
                    });
                    if (confirmed) {
                      void onRunScan(name, { forceLoad: true });
                    }
                  }}
                  onOpenWizard={(datasetName, filePath) => {
                    setWizardDataset(datasetName);
                    setWizardFilePath(filePath || null);
                    setWizardOpen(true);
                  }}
                />
              </div>
            )}

            {activeTab === 'history' && (
              <div className="space-y-5">
                <section className="grid gap-3 md:grid-cols-4">
                  <article className="rounded-xl border border-border bg-card p-4">
                    <div className="text-xs uppercase tracking-wide text-muted-foreground">Open Incidents</div>
                    <div className="mt-1 text-xl font-semibold">{historySummary.openIncidents}</div>
                  </article>
                  <article className="rounded-xl border border-border bg-card p-4">
                    <div className="text-xs uppercase tracking-wide text-muted-foreground">Critical Open</div>
                    <div className="mt-1 text-xl font-semibold">{historySummary.criticalOpen}</div>
                  </article>
                  <article className="rounded-xl border border-border bg-card p-4">
                    <div className="text-xs uppercase tracking-wide text-muted-foreground">Blocked Runs</div>
                    <div className="mt-1 text-xl font-semibold">{historySummary.blockedRuns}</div>
                  </article>
                  <article className="rounded-xl border border-border bg-card p-4">
                    <div className="text-xs uppercase tracking-wide text-muted-foreground">Warning Runs</div>
                    <div className="mt-1 text-xl font-semibold">{historySummary.warningRuns}</div>
                  </article>
                </section>
                <IncidentsPanel
                  incidents={incidents}
                  onAck={(incidentId) => void onAckIncident(incidentId)}
                  onResolve={(incidentId) => void onResolveIncident(incidentId)}
                />
                <RunsTable runs={runs} />
                <div className="grid grid-cols-1 xl:grid-cols-2 gap-5">
                  <SystemHealthPanel rows={systemHealth} />
                  <JobActivity jobs={jobs} />
                </div>
              </div>
            )}

            {activeTab === 'lineage' && <LineagePanel lineage={lineage} />}

            {activeTab === 'connections' && <ConnectionsPanel />}

            {activeTab === 'settings' && (
              <div className="space-y-5">
                <PlatformOpsPanel datasetName={selectedDataset} />
                <SettingsPanel
                  onRuntimeResetComplete={async () => {
                    setSelectedDataset(null);
                    setWizardDataset(null);
                    setWizardFilePath(null);
                    setPreviewOpen(false);
                    setProfileOpen(false);
                    setRefreshToken((prev) => prev + 1);
                    await refresh();
                  }}
                />
              </div>
            )}
          </div>
        </main>

        <CopilotPanel
          open={copilotOpen}
          onClose={() => setCopilotOpen(false)}
          initialMessage={copilotInitialMessage}
        />

        <DatasetPreviewModal
          open={previewOpen}
          datasetName={selectedDataset}
          limit={100}
          onClose={() => setPreviewOpen(false)}
        />
        <ProfileModal open={profileOpen} datasetName={selectedDataset} onClose={() => setProfileOpen(false)} />
        <ContractWizardModal
          open={wizardOpen}
          datasetName={wizardDataset}
          filePath={wizardFilePath}
          onClose={() => setWizardOpen(false)}
          onCompleted={() => void refresh()}
        />
      </div>
    </div>
  );
}
