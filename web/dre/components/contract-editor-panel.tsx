'use client';

import { useEffect, useState } from 'react';
import { Bot, Loader2, Save } from 'lucide-react';
import { useConfirmDialog } from '@/dre/components/confirm-dialog';

import {
  aiModifyContract,
  getContractYaml,
  saveContractVersion,
} from '@/lib/dre-api';

type Props = {
  datasetName: string | null;
  onAfterSave?: () => void;
};

type EditorTab = 'active' | 'ai';

export default function ContractEditorPanel({ datasetName, onAfterSave }: Props) {
  const [loading, setLoading] = useState(false);
  const [tab, setTab] = useState<EditorTab>('active');
  const [yaml, setYaml] = useState('');
  const [originalYaml, setOriginalYaml] = useState('');
  const [aiYaml, setAiYaml] = useState('');
  const [aiInstruction, setAiInstruction] = useState('');
  const { dialog: confirmDialog, confirm } = useConfirmDialog();

  const canOperate = Boolean(datasetName);

  const refresh = async () => {
    if (!datasetName) return;
    setLoading(true);
    try {
      const doc = await getContractYaml(datasetName);
      const nextYaml = doc.yaml_content || '';
      setYaml(nextYaml);
      setOriginalYaml(nextYaml);
      setAiYaml('');
      setTab('active');
    } catch (error) {
      console.error('Failed to load active contract', error);
      setYaml('');
      setOriginalYaml('');
      setAiYaml('');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (!datasetName) return;
    void refresh();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [datasetName]);

  const currentYaml = tab === 'ai' ? aiYaml : yaml;
  const canSave = Boolean(currentYaml.trim());

  const onSaveAsVersion = async () => {
    if (!datasetName) return;
    const { confirmed } = await confirm({
      title: 'Save Contract Version',
      message: `Save current YAML as a new version for "${datasetName}"?`,
      confirmLabel: 'Save',
      variant: 'info',
    });
    if (!confirmed) return;

    setLoading(true);
    try {
      await saveContractVersion(datasetName, {
        yaml_content: currentYaml,
        change_type: tab === 'ai' ? 'ai_edit' : 'manual_edit',
        changed_by: 'next-ui',
      });
      await refresh();
      onAfterSave?.();
    } catch (error) {
      console.error('Version save failed', error);
    } finally {
      setLoading(false);
    }
  };

  const onAiModify = async () => {
    if (!datasetName) return;
    const instruction = aiInstruction.trim();
    if (!instruction) return;
    if (!yaml.trim()) return;

    setLoading(true);
    try {
      const resp = await aiModifyContract(datasetName, { instruction, current_yaml: yaml });
      setAiYaml(resp.modified_yaml || yaml);
      setTab('ai');
      setAiInstruction('');
    } catch (error) {
      console.error('AI modify failed', error);
    } finally {
      setLoading(false);
    }
  };

  const onCancel = () => {
    setYaml(originalYaml);
    setAiYaml('');
    setAiInstruction('');
    setTab('active');
  };

  if (!canOperate) {
    return (
      <section className="rounded-xl border border-border bg-card p-4 text-sm text-muted-foreground">
        Select a dataset to view or edit contracts.
      </section>
    );
  }

  return (
    <>
      {confirmDialog}
      <section className="rounded-xl border border-border bg-card overflow-hidden">
        <div className="px-4 py-3 border-b border-border flex items-center justify-between gap-3">
          <div className="text-sm font-medium">Contract Editor</div>
          <div className="flex items-center gap-2">
            <button
              onClick={() => setTab('active')}
              className={`rounded-lg px-3 py-1.5 text-xs font-medium ${tab === 'active' ? 'bg-foreground text-background' : 'border border-border bg-background text-muted-foreground hover:bg-accent'
                }`}
            >
              Active
            </button>
            <button
              onClick={() => setTab('ai')}
              className={`rounded-lg px-3 py-1.5 text-xs font-medium ${tab === 'ai' ? 'bg-foreground text-background' : 'border border-border bg-background text-muted-foreground hover:bg-accent'
                }`}
            >
              AI Generated
            </button>
            <button
              onClick={() => void refresh()}
              className="rounded-lg border border-border bg-background px-2 py-1 text-xs font-medium hover:bg-accent"
            >
              Reload
            </button>
          </div>
        </div>

        <div className="p-4 space-y-3">
          {loading && (
            <div className="text-sm text-muted-foreground flex items-center gap-2">
              <Loader2 size={16} className="animate-spin" /> Working...
            </div>
          )}

          <div className="rounded-xl border border-border bg-background overflow-hidden">
            <div className="px-4 py-3 border-b border-border text-sm font-medium">
              {tab === 'active' ? 'Active Contract YAML' : 'AI Generated YAML'}
            </div>
            <textarea
              value={currentYaml}
              onChange={(e) => {
                if (tab === 'ai') setAiYaml(e.target.value);
                else setYaml(e.target.value);
              }}
              className="h-72 w-full resize-none bg-background p-4 font-mono text-xs text-foreground outline-none"
              placeholder="# Contract YAML..."
            />
          </div>

          <div className="rounded-xl border border-border bg-background p-3 space-y-2">
            <div className="text-xs uppercase tracking-wide text-muted-foreground">AI Modify</div>
            <div className="flex gap-2">
              <input
                value={aiInstruction}
                onChange={(e) => setAiInstruction(e.target.value)}
                placeholder='e.g. "Make customer_id non-nullable"'
                className="flex-1 rounded-lg border border-border bg-background px-3 py-2 text-sm outline-none focus:ring-2 focus:ring-ring/40"
              />
              <button
                onClick={() => void onAiModify()}
                disabled={loading || !aiInstruction.trim() || !yaml.trim()}
                className="rounded-lg border border-border bg-background px-3 py-2 text-sm font-medium hover:bg-accent disabled:opacity-50 inline-flex items-center gap-2"
              >
                <Bot size={16} /> Generate
              </button>
            </div>
          </div>

          <div className="flex flex-wrap items-center gap-2">
            <button
              onClick={() => void onSaveAsVersion()}
              disabled={loading || !canSave}
              className="rounded-lg bg-foreground text-background px-3 py-2 text-sm font-medium hover:opacity-90 disabled:opacity-50 inline-flex items-center gap-2"
            >
              <Save size={16} /> Save Version
            </button>
            <button
              onClick={onCancel}
              disabled={loading}
              className="rounded-lg border border-border bg-background px-3 py-2 text-sm font-medium hover:bg-accent disabled:opacity-50"
            >
              Cancel
            </button>
          </div>
        </div>
      </section>
    </>
  );
}
