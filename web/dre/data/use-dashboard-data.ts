import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import {
  getGlobalStats,
  getDatasets,
  getIncidents,
  getLineage,
  getPendingContracts,
  getPulse,
  getRecentRuns,
  getSystemHealth,
  listJobs,
  type AsyncJob,
  type DatasetRow,
  type GlobalStats,
  type IncidentItem,
  type LineageGraph,
  type PendingContract,
  type PulseRow,
  type RunEvent,
  type SystemHealthRow,
} from '@/lib/dre-api';

type BusyState = Record<string, 'scan' | 'delete'>;

type Options = {
  pollIntervalMs?: number;
  jobsLimit?: number;
  runsLimit?: number;
  incidentsLimit?: number;
};

export function useDashboardData(options?: Options) {
  const pollIntervalMs = options?.pollIntervalMs ?? 15000;
  const jobsLimit = options?.jobsLimit ?? 40;
  const runsLimit = options?.runsLimit ?? 120;
  const incidentsLimit = options?.incidentsLimit ?? 120;

  const [pulse, setPulse] = useState<PulseRow[]>([]);
  const [datasets, setDatasets] = useState<DatasetRow[]>([]);
  const [pending, setPending] = useState<PendingContract[]>([]);
  const [jobs, setJobs] = useState<AsyncJob[]>([]);
  const [runs, setRuns] = useState<RunEvent[]>([]);
  const [incidents, setIncidents] = useState<IncidentItem[]>([]);
  const [lineage, setLineage] = useState<LineageGraph | null>(null);
  const [globalStats, setGlobalStats] = useState<GlobalStats | null>(null);
  const [systemHealth, setSystemHealth] = useState<SystemHealthRow[]>([]);
  const [loading, setLoading] = useState(true);
  const aliveRef = useRef(true);
  const hydratedRef = useRef(false);

  useEffect(() => {
    aliveRef.current = true;
    return () => {
      aliveRef.current = false;
    };
  }, []);

  const busy = useMemo<BusyState>(() => {
    const map: BusyState = {};
    for (const job of jobs) {
      if (!(job.status === 'QUEUED' || job.status === 'RUNNING')) continue;
      map[job.dataset_name] = job.action === 'delete' ? 'delete' : 'scan';
    }
    return map;
  }, [jobs]);

  const refresh = useCallback(async (opts?: { silent?: boolean }) => {
    const silent = opts?.silent ?? false;
    const shouldShowLoading = !hydratedRef.current && !silent;
    if (aliveRef.current && shouldShowLoading) setLoading(true);
    try {
      const [pulseData, datasetData, pendingData, jobData, runData, incidentData, lineageData, statsData, healthData] = await Promise.allSettled([
        getPulse(),
        getDatasets(),
        getPendingContracts(),
        listJobs(jobsLimit),
        getRecentRuns(runsLimit),
        getIncidents({ limit: incidentsLimit }),
        getLineage(),
        getGlobalStats(),
        getSystemHealth(),
      ]);
      if (!aliveRef.current) return;
      if (pulseData.status === 'fulfilled') setPulse(pulseData.value);
      if (datasetData.status === 'fulfilled') setDatasets(datasetData.value);
      if (pendingData.status === 'fulfilled') setPending(pendingData.value);
      if (jobData.status === 'fulfilled') setJobs(jobData.value);
      if (runData.status === 'fulfilled') setRuns(runData.value);
      if (incidentData.status === 'fulfilled') setIncidents(incidentData.value);
      if (lineageData.status === 'fulfilled') setLineage(lineageData.value);
      if (statsData.status === 'fulfilled') setGlobalStats(statsData.value);
      if (healthData.status === 'fulfilled') setSystemHealth(healthData.value);
      hydratedRef.current = true;

      const results: Array<[string, PromiseSettledResult<unknown>]> = [
        ['pulse', pulseData],
        ['datasets', datasetData],
        ['pending contracts', pendingData],
        ['jobs', jobData],
        ['runs', runData],
        ['incidents', incidentData],
        ['lineage', lineageData],
        ['global stats', statsData],
        ['system health', healthData],
      ];
      const failures = results.filter(([, result]) => result.status === 'rejected');
      if (failures.length > 0) {
        console.warn(
          'Dashboard refresh completed with partial failures:',
          failures.map(([name, result]) => ({
            name,
            error: result.status === 'rejected' ? String(result.reason) : null,
          })),
        );
      }
    } catch (error) {
      console.error('Failed to refresh dashboard', error);
    } finally {
      if (aliveRef.current && shouldShowLoading) setLoading(false);
    }
  }, [incidentsLimit, jobsLimit, runsLimit]);

  useEffect(() => {
    const run = async (silent = false) => {
      await refresh({ silent });
    };

    void run(false);
    const interval = setInterval(() => {
      if (typeof document !== 'undefined' && document.visibilityState !== 'visible') return;
      void run(true);
    }, pollIntervalMs);

    return () => {
      clearInterval(interval);
    };
  }, [pollIntervalMs, refresh]);

  return {
    pulse,
    datasets,
    pending,
    jobs,
    runs,
    incidents,
    lineage,
    globalStats,
    systemHealth,
    busy,
    loading,
    refresh,
  };
}
