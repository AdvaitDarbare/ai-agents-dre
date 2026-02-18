'use client';

import { useEffect, useMemo, useState } from 'react';
import { FileText, History, Loader2, Save } from 'lucide-react';
import { useConfirmDialog } from '@/dre/components/confirm-dialog';

import {
  getContractVersion,
  getContractYaml,
  listContractVersions,
  saveContractVersion,
  type ContractVersionRow,
} from '@/lib/dre-api';

type Props = {
  datasetName: string | null;
  refreshToken?: number;
};

export default function GovernancePanel({ datasetName, refreshToken = 0 }: Props) {
  const [loading, setLoading] = useState(false);
  const [history, setHistory] = useState<ContractVersionRow[]>([]);
  const [selected, setSelected] = useState<ContractVersionRow | null>(null);
  const [activeYaml, setActiveYaml] = useState<string>('');
  const [selectedYaml, setSelectedYaml] = useState<string>('');
  const { dialog: confirmDialog, confirm } = useConfirmDialog();

  const canOperate = Boolean(datasetName);

  const refresh = async () => {
    if (!datasetName) return;
    setLoading(true);
    try {
      const [items, active] = await Promise.all([listContractVersions(datasetName), getContractYaml(datasetName)]);
      setHistory(items);
      const latest = items[0] || null;
      setSelected(latest);
      setSelectedYaml(latest?.yaml_content || '');
      setActiveYaml(active.yaml_content || '');
    } catch (error) {
      console.error('Failed to load governance panel', error);
      setHistory([]);
      setSelected(null);
      setActiveYaml('');
      setSelectedYaml('');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void refresh();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [datasetName, refreshToken]);

  const title = useMemo(() => {
    if (!datasetName) return 'Governance';
    return `Governance · ${datasetName}`;
  }, [datasetName]);

  const onLoadVersion = async (row: ContractVersionRow) => {
    if (!datasetName) return;
    setSelected(row);
    if (row.yaml_content) {
      setSelectedYaml(row.yaml_content);
      return;
    }
    setLoading(true);
    try {
      const loaded = await getContractVersion(datasetName, row.version_id);
      setSelectedYaml(loaded.yaml_content || '');
    } catch (error) {
      console.error('Failed to load contract version content', error);
      setSelectedYaml('');
      alert(`Load version failed: ${error instanceof Error ? error.message : 'unknown error'}`);
    } finally {
      setLoading(false);
    }
  };

  const onRestoreAsActive = async () => {
    if (!datasetName || !selected?.version_id || !selectedYaml.trim()) return;
    const stamp = selected.timestamp ? new Date(selected.timestamp).toLocaleString() : selected.version_id;
    const { confirmed } = await confirm({
      title: 'Restore Contract Version',
      message: `Restore ${datasetName} to version ${selected.version_id} (${stamp}) and save as new active version?`,
      confirmLabel: 'Restore',
      variant: 'warning',
    });
    if (!confirmed) return;
    try {
      await saveContractVersion(datasetName, {
        yaml_content: selectedYaml,
        change_type: 'restore_version',
        changed_by: 'next-ui',
      });
      await refresh();
    } catch (error) {
      console.error('Restore failed', error);
    }
  };

  if (!canOperate) {
    return (
      <section className="rounded-xl border border-border bg-card p-4 text-sm text-muted-foreground">
        Select a dataset to view governance history.
      </section>
    );
  }

  return (
    <>
      {confirmDialog}
      <section className="rounded-xl border border-border bg-card overflow-hidden">
        <div className="px-4 py-3 border-b border-border flex items-center justify-between gap-3">
          <div className="flex items-center gap-2">
            <History size={16} />
            <div className="text-sm font-medium">{title}</div>
          </div>
          <div className="flex items-center gap-2">
            <button
              onClick={() => void refresh()}
              className="rounded-lg border border-border bg-background px-2 py-1 text-xs font-medium hover:bg-accent"
            >
              Refresh
            </button>
            <button
              onClick={onRestoreAsActive}
              disabled={!selected?.version_id || !selectedYaml.trim()}
              className="rounded-lg bg-foreground text-background px-2 py-1 text-xs font-medium hover:opacity-90 disabled:opacity-50"
            >
              <span className="inline-flex items-center gap-1">
                <Save size={12} />
                Restore As Active
              </span>
            </button>
          </div>
        </div>

        {loading ? (
          <div className="p-6 text-sm text-muted-foreground flex items-center gap-2">
            <Loader2 size={16} className="animate-spin" /> Loading governance data...
          </div>
        ) : (
          <div className="grid gap-0 md:grid-cols-3">
            <div className="md:col-span-1 border-b md:border-b-0 md:border-r border-border">
              <div className="p-4 text-xs uppercase tracking-wide text-muted-foreground">Version History</div>
              <div className="max-h-[420px] overflow-y-auto">
                {history.map((row) => (
                  <button
                    key={`${row.version_id}-${row.timestamp}`}
                    onClick={() => void onLoadVersion(row)}
                    className={`w-full text-left px-4 py-3 border-t border-border/60 hover:bg-muted/20 ${selected?.version_id === row.version_id ? 'bg-muted/30' : ''
                      }`}
                  >
                    <div className="text-xs text-muted-foreground">{row.timestamp ? new Date(row.timestamp).toLocaleString() : 'unknown time'}</div>
                    <div className="mt-1 text-sm font-medium text-foreground line-clamp-2">
                      v{row.version_id} · {row.change_type || 'manual_edit'} · {row.changed_by || 'user'}
                    </div>
                  </button>
                ))}
                {history.length === 0 && (
                  <div className="px-4 py-6 text-sm text-muted-foreground">No contract versions found.</div>
                )}
              </div>
            </div>

            <div className="md:col-span-2 p-4 space-y-4">
              <article className="rounded-xl border border-border bg-background overflow-hidden">
                <div className="px-4 py-3 border-b border-border text-sm font-medium flex items-center justify-between">
                  <span className="inline-flex items-center gap-2">
                    <FileText size={16} />
                    Active Contract
                  </span>
                </div>
                <pre className="p-4 text-xs overflow-auto max-h-[220px] whitespace-pre-wrap text-muted-foreground">{activeYaml || '# No contract found'}</pre>
              </article>

              <article className="rounded-xl border border-border bg-background overflow-hidden">
                <div className="px-4 py-3 border-b border-border text-sm font-medium flex items-center justify-between">
                  <span className="inline-flex items-center gap-2">
                    <History size={16} />
                    Selected Version
                  </span>
                  <span className="text-xs text-muted-foreground">
                    {selected?.version_id ? `v${selected.version_id}` : 'No selection'}
                  </span>
                </div>
                <pre className="p-4 text-xs overflow-auto max-h-[220px] whitespace-pre-wrap text-muted-foreground">
                  {selectedYaml || '# Select a version to load YAML content'}
                </pre>
              </article>
            </div>
          </div>
        )}
      </section>
    </>
  );
}
