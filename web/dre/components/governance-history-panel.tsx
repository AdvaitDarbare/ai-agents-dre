'use client';

import { useEffect, useMemo, useState } from 'react';
import { FileText, History, Loader2, MessageSquare, Pencil, Save, X } from 'lucide-react';

import {
  aiModifyContract,
  getContractVersion,
  getContractYaml,
  listContractVersions,
  saveContractVersion,
  type ContractVersionRow,
  type DatasetRunHistoryItem,
} from '@/lib/dre-api';

type ChatMessage = {
  role: 'user' | 'assistant' | 'system';
  content: string;
};

type Props = {
  datasetName: string;
  scanHistory: DatasetRunHistoryItem[];
  onAfterSave?: () => void;
};

function toLocal(ts?: string | null): string {
  if (!ts) return 'Unknown';
  const dt = new Date(ts);
  if (Number.isNaN(dt.getTime())) return 'Unknown';
  return new Intl.DateTimeFormat(undefined, {
    year: 'numeric',
    month: 'numeric',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
    second: '2-digit',
    hour12: true,
    timeZoneName: 'short',
  }).format(dt);
}

export default function GovernanceHistoryPanel({ datasetName, scanHistory, onAfterSave }: Props) {
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [aiLoading, setAiLoading] = useState(false);
  const [versions, setVersions] = useState<ContractVersionRow[]>([]);
  const [selected, setSelected] = useState<ContractVersionRow | null>(null);
  const [activeYaml, setActiveYaml] = useState('');
  const [workingYaml, setWorkingYaml] = useState('');
  const [isEditing, setIsEditing] = useState(false);
  const [aiOpen, setAiOpen] = useState(true);
  const [aiInstruction, setAiInstruction] = useState('');
  const [aiEdited, setAiEdited] = useState(false);
  const [chat, setChat] = useState<ChatMessage[]>([
    {
      role: 'assistant',
      content: 'Ask me to modify this contract. Example: "Set Age nullable false" or "Add pattern to Email".',
    },
  ]);

  const lastScanned = useMemo(() => toLocal(scanHistory[0]?.timestamp), [scanHistory]);

  const refresh = async () => {
    setLoading(true);
    try {
      const [history, active] = await Promise.all([listContractVersions(datasetName), getContractYaml(datasetName)]);
      setVersions(history || []);
      setSelected((history || [])[0] || null);
      setActiveYaml(active.yaml_content || '');
      setWorkingYaml(active.yaml_content || '');
      setIsEditing(false);
      setAiEdited(false);
    } catch (error) {
      console.error('Failed to load governance history panel', error);
      setVersions([]);
      setSelected(null);
      setActiveYaml('');
      setWorkingYaml('');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void refresh();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [datasetName]);

  const onSelectVersion = async (row: ContractVersionRow) => {
    setSelected(row);
    setLoading(true);
    try {
      const yaml = row.yaml_content || (await getContractVersion(datasetName, row.version_id)).yaml_content || '';
      setWorkingYaml(yaml);
      setIsEditing(true);
      setAiEdited(false);
      setChat((prev) => [
        ...prev,
        {
          role: 'system',
          content: `Loaded version ${row.version_id}. Review and click Save Contract to restore.`,
        },
      ]);
    } catch (error) {
      console.error('Failed to load selected version', error);
      alert(`Load version failed: ${error instanceof Error ? error.message : 'unknown error'}`);
    } finally {
      setLoading(false);
    }
  };

  const onSave = async () => {
    if (!workingYaml.trim()) return;
    setSaving(true);
    try {
      const result = await saveContractVersion(datasetName, {
        yaml_content: workingYaml,
        change_type: aiEdited ? 'ai_edit' : 'manual_edit',
        changed_by: 'next-ui',
      });
      await refresh();
      onAfterSave?.();
      if (result?.scan?.enqueued) {
        alert(`Contract version saved. Auto-scan queued (${result.scan.job_id}).`);
      } else if (result?.scan?.error) {
        alert(`Contract version saved. Auto-scan could not start: ${result.scan.error}`);
      } else {
        alert('Contract version saved.');
      }
    } catch (error) {
      alert(`Save failed: ${error instanceof Error ? error.message : 'unknown error'}`);
    } finally {
      setSaving(false);
    }
  };

  const onAiSend = async () => {
    const instruction = aiInstruction.trim();
    if (!instruction) return;
    setAiLoading(true);
    setChat((prev) => [...prev, { role: 'user', content: instruction }]);
    setAiInstruction('');
    try {
      const resp = await aiModifyContract(datasetName, {
        instruction,
        current_yaml: workingYaml || activeYaml,
      });
      const nextYaml = String(resp.modified_yaml || workingYaml || activeYaml);
      setWorkingYaml(nextYaml);
      setIsEditing(true);
      setAiEdited(true);
      setChat((prev) => [
        ...prev,
        { role: 'assistant', content: resp.explanation || 'Applied your requested changes to YAML.' },
      ]);
    } catch (error) {
      const message = error instanceof Error ? error.message : 'unknown error';
      setChat((prev) => [...prev, { role: 'assistant', content: `AI modify failed: ${message}` }]);
    } finally {
      setAiLoading(false);
    }
  };

  return (
    <div className="space-y-4">
      <div className="text-[10px] uppercase tracking-[0.2em] text-muted-foreground rounded-full bg-muted/30 inline-flex px-3 py-1">
        Last Scanned: {lastScanned}
      </div>

      <section className="rounded-xl border border-border bg-card overflow-hidden">
        <div className="grid gap-0 lg:grid-cols-12">
          <div className="lg:col-span-3 border-b lg:border-b-0 lg:border-r border-border">
            <div className="px-4 py-3 border-b border-border text-sm font-medium inline-flex items-center gap-2">
              <History size={15} /> Version History
            </div>
            <div className="max-h-[560px] overflow-y-auto">
              {versions.map((row, idx) => (
                <button
                  key={`${row.version_id}-${row.timestamp}`}
                  onClick={() => void onSelectVersion(row)}
                  className={`w-full text-left px-4 py-3 border-t border-border/60 hover:bg-muted/20 ${
                    selected?.version_id === row.version_id ? 'bg-muted/30' : ''
                  }`}
                >
                  <div className="flex items-center justify-between">
                    <span className="text-xs text-muted-foreground">{toLocal(row.timestamp)}</span>
                    {idx === 0 && <span className="text-[10px] rounded-full px-2 py-0.5 bg-emerald-100 text-emerald-700">Latest</span>}
                  </div>
                  <div className="mt-1 text-xs text-foreground/90">
                    {row.change_type || 'manual_edit'} · {row.changed_by || 'user'}
                  </div>
                </button>
              ))}
              {!versions.length && <div className="px-4 py-6 text-sm text-muted-foreground">No versions found.</div>}
            </div>
          </div>

          <div className="lg:col-span-9 p-4 space-y-4">
            <article className="rounded-xl border border-border bg-background overflow-hidden">
              <div className="px-4 py-3 border-b border-border flex items-center justify-between">
                <div className="inline-flex items-center gap-2 text-sm font-medium">
                  <FileText size={15} /> Data Contract
                </div>
                <div className="flex items-center gap-2">
                  {!isEditing ? (
                    <>
                      <button
                        onClick={() => setAiOpen((prev) => !prev)}
                        className="rounded-lg border border-border bg-background px-2 py-1 text-xs font-medium hover:bg-accent inline-flex items-center gap-1"
                      >
                        <MessageSquare size={12} /> AI Assistant
                      </button>
                      <button
                        onClick={() => setIsEditing(true)}
                        className="rounded-lg bg-foreground text-background px-2 py-1 text-xs font-medium hover:opacity-90 inline-flex items-center gap-1"
                      >
                        <Pencil size={12} /> Edit Contract
                      </button>
                    </>
                  ) : (
                    <>
                      <button
                        onClick={() => {
                          setWorkingYaml(activeYaml);
                          setIsEditing(false);
                          setAiEdited(false);
                        }}
                        className="rounded-lg border border-border bg-background px-2 py-1 text-xs font-medium hover:bg-accent inline-flex items-center gap-1"
                      >
                        <X size={12} /> Cancel
                      </button>
                      <button
                        onClick={() => void onSave()}
                        disabled={saving}
                        className="rounded-lg bg-foreground text-background px-2 py-1 text-xs font-medium hover:opacity-90 disabled:opacity-50 inline-flex items-center gap-1"
                      >
                        {saving ? <Loader2 size={12} className="animate-spin" /> : <Save size={12} />} Save Contract
                      </button>
                    </>
                  )}
                </div>
              </div>
              {isEditing ? (
                <textarea
                  value={workingYaml}
                  onChange={(e) => setWorkingYaml(e.target.value)}
                  className="h-[320px] w-full resize-none bg-background p-4 font-mono text-xs text-foreground outline-none"
                />
              ) : (
                <pre className="h-[320px] overflow-auto p-4 text-xs whitespace-pre-wrap text-muted-foreground">
                  {workingYaml || activeYaml || '# No contract found'}
                </pre>
              )}
            </article>

            {aiOpen && (
              <article className="rounded-xl border border-border bg-background overflow-hidden">
                <div className="px-4 py-3 border-b border-border text-xs uppercase tracking-wide text-muted-foreground">
                  AI Contract Assistant
                </div>
                <div className="p-3 space-y-3">
                  <div className="max-h-40 overflow-auto rounded-lg border border-border bg-card p-3 text-xs space-y-2">
                    {chat.map((msg, idx) => (
                      <div key={`${msg.role}-${idx}`} className={msg.role === 'user' ? 'text-right' : 'text-left'}>
                        <span
                          className={`inline-block rounded-lg px-3 py-1.5 ${
                            msg.role === 'user'
                              ? 'bg-foreground text-background'
                              : msg.role === 'system'
                                ? 'bg-emerald-100 text-emerald-700'
                                : 'bg-muted text-foreground'
                          }`}
                        >
                          {msg.content}
                        </span>
                      </div>
                    ))}
                  </div>
                  <div className="flex items-center gap-2">
                    <input
                      value={aiInstruction}
                      onChange={(e) => setAiInstruction(e.target.value)}
                      onKeyDown={(e) => {
                        if (e.key === 'Enter') void onAiSend();
                      }}
                      placeholder='Type request, e.g. "Add unique constraint to VendorID"'
                      className="flex-1 rounded-lg border border-border bg-background px-3 py-2 text-sm outline-none focus:ring-2 focus:ring-ring/30"
                    />
                    <button
                      onClick={() => void onAiSend()}
                      disabled={aiLoading || !aiInstruction.trim()}
                      className="rounded-lg bg-foreground text-background px-3 py-2 text-xs font-medium hover:opacity-90 disabled:opacity-50"
                    >
                      {aiLoading ? 'Applying...' : 'Send'}
                    </button>
                  </div>
                </div>
              </article>
            )}
          </div>
        </div>
      </section>

      <section className="rounded-xl border border-border bg-card overflow-hidden">
        <div className="px-4 py-3 border-b border-border text-sm font-medium inline-flex items-center gap-2">
          <History size={15} /> Scan History
        </div>
        <div className="overflow-x-auto">
          <table className="min-w-full text-sm">
            <thead className="bg-muted/20 text-left text-xs uppercase tracking-wide text-muted-foreground">
              <tr>
                <th className="px-4 py-3">Run Timestamp</th>
                <th className="px-4 py-3">Status</th>
                <th className="px-4 py-3">Quality Score</th>
                <th className="px-4 py-3">Anomalies</th>
                <th className="px-4 py-3">Reason</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border/60">
              {scanHistory.slice(0, 10).map((run, idx) => (
                <tr key={`scan-${idx}`} className="hover:bg-muted/10">
                  <td className="px-4 py-3 text-muted-foreground">{toLocal(run.timestamp || null)}</td>
                  <td className="px-4 py-3">
                    <span
                      className={`rounded px-2 py-0.5 text-[10px] font-semibold uppercase ${
                        run.status === 'PASSED'
                          ? 'bg-emerald-100 text-emerald-700'
                          : run.status === 'WARNING'
                            ? 'bg-amber-100 text-amber-700'
                            : 'bg-rose-100 text-rose-700'
                      }`}
                    >
                      {run.status || 'UNKNOWN'}
                    </span>
                  </td>
                  <td className="px-4 py-3">
                    {typeof run.quality_score === 'number' ? `${run.quality_score.toFixed(1)}%` : '-'}
                  </td>
                  <td className="px-4 py-3">{run.anomaly_count ?? 0}</td>
                  <td className="px-4 py-3 text-muted-foreground">{run.reason || '-'}</td>
                </tr>
              ))}
              {!scanHistory.length && (
                <tr>
                  <td className="px-4 py-8 text-sm text-muted-foreground" colSpan={5}>
                    No scan history found.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </section>

      {loading && (
        <div className="text-sm text-muted-foreground inline-flex items-center gap-2">
          <Loader2 size={14} className="animate-spin" /> Loading governance data...
        </div>
      )}
    </div>
  );
}
