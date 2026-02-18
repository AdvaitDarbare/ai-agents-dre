import { useCallback, useEffect, useState } from 'react';

import { getSloHistory, getSloSummary, type SloCheck, type SloSummary } from '@/lib/dre-api';

type Options = {
  window?: number;
  limit?: number;
};

export function useSloData(datasetName: string | null, options?: Options) {
  const window = options?.window ?? 200;
  const limit = options?.limit ?? 100;

  const [summary, setSummary] = useState<SloSummary | null>(null);
  const [checks, setChecks] = useState<SloCheck[]>([]);

  const refresh = useCallback(async () => {
    if (!datasetName) return;
    try {
      const [nextSummary, nextChecks] = await Promise.all([getSloSummary(datasetName, window), getSloHistory(datasetName, limit)]);
      setSummary(nextSummary);
      setChecks(nextChecks);
    } catch (error) {
      setSummary(null);
      setChecks([]);
      console.error('Failed to load SLO panel', error);
    }
  }, [datasetName, limit, window]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  return { summary, checks, refresh };
}

