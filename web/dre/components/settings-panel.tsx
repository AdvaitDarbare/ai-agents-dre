'use client';

import { useEffect, useMemo, useState } from 'react';
import { ShieldCheck, SlidersHorizontal, Trash2 } from 'lucide-react';

import {
  getPlatformConfig,
  resetRuntimeState,
  type PlatformConfig,
  type RuntimeResetResponse,
} from '@/lib/dre-api';

type SettingsPanelProps = {
  onRuntimeResetComplete?: () => Promise<void> | void;
};

export default function SettingsPanel({ onRuntimeResetComplete }: SettingsPanelProps) {
  const [config, setConfig] = useState<PlatformConfig | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [confirmPhrase, setConfirmPhrase] = useState<string>('');
  const [clearContracts, setClearContracts] = useState<boolean>(true);
  const [clearCheckpoints, setClearCheckpoints] = useState<boolean>(true);
  const [preserveContracts, setPreserveContracts] = useState<string>('');
  const [resetting, setResetting] = useState<boolean>(false);
  const [resetResult, setResetResult] = useState<RuntimeResetResponse | null>(null);

  const loadConfig = async () => {
    try {
      setError(null);
      const payload = await getPlatformConfig();
      setConfig(payload);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load platform configuration');
    }
  };

  useEffect(() => {
    void loadConfig();
  }, []);

  const rows = useMemo(() => {
    const runtime = config?.runtime || {};
    const asyncJobs = (runtime.async_jobs || {}) as Record<string, any>;
    const connectors = Array.isArray(runtime.connectors_enabled) ? runtime.connectors_enabled : [];
    const doris = (runtime.doris || {}) as Record<string, any>;

    return [
      {
        key: 'langgraph_workflow_enabled',
        name: 'LangGraph Workflow',
        description: 'Unified evaluate/HITL workflow runtime in backend.',
        value: runtime.langgraph_workflow_enabled ? 'ENABLED' : 'DISABLED',
      },
      {
        key: 'policy_gates_enabled',
        name: 'Policy Gates',
        description: 'Approval-required enforcement for high-risk actions.',
        value: runtime.policy_gates_enabled ? 'ENFORCED' : 'DISABLED',
      },
      {
        key: 'contract_store_backend',
        name: 'Contract Store',
        description: 'Active contract storage backend.',
        value: String(runtime.contract_store_backend || 'unknown').toUpperCase(),
      },
      {
        key: 'connectors_enabled',
        name: 'Connectors Enabled',
        description: 'Connectors currently loaded in runtime.',
        value: connectors.length ? String(connectors.join(', ')) : 'NONE',
      },
      {
        key: 'async_workers',
        name: 'Async Workers',
        description: 'In-process worker count for queued jobs.',
        value: String(asyncJobs.max_workers ?? 'unknown'),
      },
      {
        key: 'async_queue',
        name: 'Async Queue Capacity',
        description: 'Maximum number of queued/running async jobs.',
        value: String(asyncJobs.max_queued_jobs ?? 'unknown'),
      },
      {
        key: 'watch_dir',
        name: 'Watch Directory',
        description: 'Default event-driven watch path.',
        value: String(runtime.watch_dir || 'data/landing'),
      },
      {
        key: 'doris_mode',
        name: 'Doris Load Mode',
        description: 'Warehouse load mode for Stage C actuator.',
        value: doris.mock_mode ? 'MOCK' : 'LIVE',
      },
    ];
  }, [config]);

  const handleReset = async () => {
    if (confirmPhrase.trim().toUpperCase() !== 'RESET') {
      setError('Type RESET to confirm runtime reset.');
      return;
    }

    try {
      setResetting(true);
      setError(null);
      const preserved = preserveContracts
        .split(',')
        .map((item) => item.trim())
        .filter(Boolean);

      const payload = await resetRuntimeState({
        confirm_phrase: 'RESET',
        clear_generated_contracts: clearContracts,
        preserve_contracts: preserved,
        clear_langgraph_checkpoints: clearCheckpoints,
      });
      setResetResult(payload);
      setConfirmPhrase('');
      await loadConfig();
      if (onRuntimeResetComplete) {
        await onRuntimeResetComplete();
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to reset runtime state');
    } finally {
      setResetting(false);
    }
  };

  return (
    <section className="space-y-4">
      <div className="rounded-xl border border-border bg-card p-4">
        <div className="flex items-center gap-2 text-sm font-medium">
          <ShieldCheck size={16} className="text-emerald-600" />
          Platform Configuration (Runtime)
        </div>
        <p className="mt-1 text-sm text-muted-foreground">
          Read-only values from backend runtime; no hardcoded placeholders.
        </p>
        {error && <p className="mt-2 text-sm text-rose-700">{error}</p>}
      </div>

      <div className="rounded-xl border border-border bg-card overflow-hidden">
        <div className="px-4 py-3 border-b border-border text-sm font-medium flex items-center gap-2">
          <SlidersHorizontal size={14} />
          Platform Settings
        </div>
        <div className="divide-y divide-border">
          {rows.map((item) => (
            <div key={item.key} className="px-4 py-3 flex items-center justify-between gap-4">
              <div>
                <div className="text-sm font-medium">{item.name}</div>
                <div className="text-xs text-muted-foreground mt-1">{item.description}</div>
              </div>
              <div className="text-xs font-semibold uppercase tracking-wide rounded-full bg-muted px-3 py-1">{item.value}</div>
            </div>
          ))}
        </div>
      </div>

      <div className="rounded-xl border border-rose-300 bg-rose-50 overflow-hidden">
        <div className="px-4 py-3 border-b border-rose-200 text-sm font-medium flex items-center gap-2 text-rose-900">
          <Trash2 size={14} />
          Runtime Reset (Danger Zone)
        </div>
        <div className="p-4 space-y-3 text-sm">
          <p className="text-rose-900">
            Clears runtime DB logs/jobs/incidents, staged files, history artifacts, and optional generated contracts.
          </p>
          <label className="inline-flex items-center gap-2">
            <input type="checkbox" checked={clearContracts} onChange={(e) => setClearContracts(e.target.checked)} />
            Clear generated contracts in <code>config/expectations</code>
          </label>
          <label className="inline-flex items-center gap-2">
            <input type="checkbox" checked={clearCheckpoints} onChange={(e) => setClearCheckpoints(e.target.checked)} />
            Clear LangGraph checkpoints
          </label>
          <div>
            <label className="text-xs uppercase tracking-wide text-rose-700">Preserve contracts (comma-separated)</label>
            <input
              value={preserveContracts}
              onChange={(e) => setPreserveContracts(e.target.value)}
              className="mt-1 w-full rounded-md border border-rose-300 bg-white px-3 py-2"
              placeholder="optional: transactions,orders"
            />
          </div>
          <div>
            <label className="text-xs uppercase tracking-wide text-rose-700">Type RESET to confirm</label>
            <input
              value={confirmPhrase}
              onChange={(e) => setConfirmPhrase(e.target.value)}
              className="mt-1 w-full rounded-md border border-rose-300 bg-white px-3 py-2"
              placeholder="RESET"
            />
          </div>
          <button
            onClick={() => void handleReset()}
            disabled={resetting || confirmPhrase.trim().toUpperCase() !== 'RESET'}
            className="inline-flex items-center gap-2 rounded-lg bg-rose-700 text-white px-3 py-2 disabled:opacity-50"
          >
            <Trash2 size={14} />
            {resetting ? 'Resetting...' : 'Reset Runtime State'}
          </button>

          {resetResult && (
            <div className="rounded-lg border border-rose-200 bg-white p-3 text-xs text-rose-900">
              <div className="font-semibold">Reset complete</div>
              <div className="mt-1">Removed files: {resetResult.files?.removed_count ?? 0}</div>
              <div>Contracts removed: {resetResult.contracts?.removed_contract_count ?? 0}</div>
              <div>Contract history removed: {resetResult.contracts?.removed_contract_history_count ?? 0}</div>
              <div>Tables truncated: {Array.isArray(resetResult.db?.truncated_tables) ? resetResult.db?.truncated_tables.length : 0}</div>
            </div>
          )}
        </div>
      </div>
    </section>
  );
}
