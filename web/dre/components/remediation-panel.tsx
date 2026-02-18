'use client';

import { useEffect, useMemo, useState } from 'react';
import { Loader2, Wand2 } from 'lucide-react';
import { useConfirmDialog } from '@/dre/components/confirm-dialog';

import { enqueueApplyRemediationJob, getRemediationPlan, type RemediationPlanResponse } from '@/lib/dre-api';

type Props = {
  datasetName: string | null;
  onAfterApply?: () => void;
};

type ViewMode = 'llm' | 'deterministic' | 'observed';

export default function RemediationPanel({ datasetName, onAfterApply }: Props) {
  const [loading, setLoading] = useState(false);
  const [plan, setPlan] = useState<RemediationPlanResponse | null>(null);
  const [view, setView] = useState<ViewMode>('llm');
  const [error, setError] = useState<string | null>(null);
  const { dialog: confirmDialog, confirm } = useConfirmDialog();

  const canOperate = Boolean(datasetName);

  const refresh = async () => {
    if (!datasetName) return;
    setLoading(true);
    setError(null);
    try {
      const next = await getRemediationPlan(datasetName);
      setPlan(next);
      if (next?.deterministic_yaml) setView('deterministic');
      else if (next?.observed_yaml) setView('observed');
      else setView('llm');
    } catch (err) {
      setPlan(null);
      setError(err instanceof Error ? err.message : 'Failed to load remediation plan');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void refresh();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [datasetName]);

  const yaml = useMemo(() => {
    if (!plan) return '';
    if (view === 'observed') return plan.observed_yaml || '';
    if (view === 'deterministic') return plan.deterministic_yaml || plan.proposed_yaml || '';
    return plan.proposed_yaml || '';
  }, [plan, view]);

  const canApply = Boolean(datasetName && yaml && yaml.trim());

  const onApply = async () => {
    if (!datasetName) return;
    if (!yaml.trim()) return;
    const { confirmed } = await confirm({
      title: 'Apply Remediation',
      message: `Apply remediation YAML for "${datasetName}"?\n\nThis writes a new contract version and should be followed by a scan.`,
      confirmLabel: 'Apply',
      variant: 'warning',
    });
    if (!confirmed) return;

    setLoading(true);
    setError(null);
    try {
      await enqueueApplyRemediationJob(datasetName, yaml, plan?.error || 'UI remediation apply');
      await refresh();
      onAfterApply?.();
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Remediation apply failed';
      setError(msg);
    } finally {
      setLoading(false);
    }
  };

  if (!canOperate) {
    return (
      <section className="rounded-xl border border-border bg-card p-4 text-sm text-muted-foreground">
        Select a dataset to view remediation options.
      </section>
    );
  }

  return (
    <>
      {confirmDialog}
      <section className="rounded-xl border border-border bg-card overflow-hidden">
        <div className="px-4 py-3 border-b border-border flex items-center justify-between gap-3">
          <div className="text-sm font-medium inline-flex items-center gap-2">
            <Wand2 size={16} /> Remediation
          </div>
          <div className="flex items-center gap-2">
            <button
              onClick={() => void refresh()}
              className="rounded-lg border border-border bg-background px-2 py-1 text-xs font-medium hover:bg-accent"
            >
              Refresh
            </button>
            <button
              onClick={() => void onApply()}
              disabled={!canApply || loading}
              className="rounded-lg bg-foreground text-background px-2 py-1 text-xs font-medium hover:opacity-90 disabled:opacity-50"
            >
              Apply (Async Job)
            </button>
          </div>
        </div>

        <div className="p-4 space-y-3">
          {loading && (
            <div className="text-sm text-muted-foreground flex items-center gap-2">
              <Loader2 size={16} className="animate-spin" /> Loading remediation plan...
            </div>
          )}

          {error && (
            <div className="rounded-xl border border-border bg-muted/30 p-3 text-xs text-muted-foreground">
              {error}
            </div>
          )}

          {!loading && !error && (!plan || plan.status !== 'remediation_available') && (
            <div className="rounded-xl border border-border bg-muted/20 p-4 text-sm text-muted-foreground">
              No remediation proposal available yet. Run a scan to populate run history and remediation suggestions.
            </div>
          )}

          {plan && plan.status === 'remediation_available' && (
            <>
              <div className="flex flex-wrap gap-2">
                <button
                  onClick={() => setView('llm')}
                  className={`rounded-lg border px-3 py-1.5 text-xs font-medium ${view === 'llm' ? 'bg-foreground text-background border-foreground' : 'bg-background border-border text-muted-foreground hover:bg-accent'
                    }`}
                >
                  LLM Proposal
                </button>
                <button
                  onClick={() => setView('deterministic')}
                  className={`rounded-lg border px-3 py-1.5 text-xs font-medium ${view === 'deterministic'
                      ? 'bg-foreground text-background border-foreground'
                      : 'bg-background border-border text-muted-foreground hover:bg-accent'
                    }`}
                  disabled={!plan.deterministic_yaml && !plan.proposed_yaml}
                >
                  Deterministic
                </button>
                <button
                  onClick={() => setView('observed')}
                  className={`rounded-lg border px-3 py-1.5 text-xs font-medium ${view === 'observed'
                      ? 'bg-foreground text-background border-foreground'
                      : 'bg-background border-border text-muted-foreground hover:bg-accent'
                    }`}
                  disabled={!plan.observed_yaml}
                >
                  Observed
                </button>
              </div>

              {plan.generation?.warnings?.length ? (
                <div className="rounded-xl border border-border bg-muted/20 p-3 text-xs text-muted-foreground">
                  Warnings: {String(plan.generation.warnings.join(' | '))}
                </div>
              ) : null}

              <div className="rounded-xl border border-border bg-background overflow-hidden">
                <div className="px-4 py-3 border-b border-border text-sm font-medium">YAML</div>
                <pre className="p-4 text-xs whitespace-pre-wrap overflow-auto max-h-[340px] text-muted-foreground">
                  {yaml || '# No YAML available for this view.'}
                </pre>
              </div>
            </>
          )}
        </div>
      </section>
    </>
  );
}

