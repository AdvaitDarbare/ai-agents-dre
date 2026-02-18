import { useEffect, useMemo, useState } from 'react';
import { CheckCircle2, Loader2, Sparkles, X } from 'lucide-react';

import {
  approveContract,
  aiModifyContract,
  getDatasetPreview,
  getDatasetProfile,
  proposeContract,
  type DatasetPreview,
  type ProfileResponse,
} from '@/lib/dre-api';

type Props = {
  open: boolean;
  datasetName: string | null;
  filePath?: string | null;
  onClose: () => void;
  onCompleted?: () => void;
};

type Step = 1 | 2 | 3;
type ApprovalState = 'idle' | 'submitting' | 'approved';

function inferType(value: unknown): string {
  if (value == null) return 'unknown';
  if (typeof value === 'number') return Number.isInteger(value) ? 'integer' : 'number';
  if (typeof value === 'boolean') return 'boolean';
  if (typeof value === 'string') return 'string';
  return 'unknown';
}

export default function ContractWizardModal({ open, datasetName, filePath, onClose, onCompleted }: Props) {
  const [step, setStep] = useState<Step>(1);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [proposalRunNotice, setProposalRunNotice] = useState<string | null>(null);
  const [approvalState, setApprovalState] = useState<ApprovalState>('idle');
  const [profileLockedMessage, setProfileLockedMessage] = useState<string | null>(null);
  const [profile, setProfile] = useState<ProfileResponse | null>(null);
  const [sample, setSample] = useState<DatasetPreview | null>(null);
  const [proposedYaml, setProposedYaml] = useState('');
  const [aiInstruction, setAiInstruction] = useState('');

  useEffect(() => {
    if (!open || !datasetName) return;
    setStep(1);
    setLoading(true);
    setError(null);
    setProposalRunNotice(null);
    setApprovalState('idle');
    setProfileLockedMessage(null);
    setProfile(null);
    setSample(null);
    setProposedYaml('');
    setAiInstruction('');

    Promise.allSettled([getDatasetProfile(datasetName), getDatasetPreview(datasetName, 5)])
      .then(([profileRes, sampleRes]) => {
        if (profileRes.status === 'fulfilled') {
          setProfile(profileRes.value);
        } else {
          const message = profileRes.reason instanceof Error ? profileRes.reason.message : 'Failed to profile dataset';
          const lowered = String(message).toLowerCase();
          if (
            lowered.includes('generate/approve yaml first') ||
            (lowered.includes('409') && lowered.includes('contract approval'))
          ) {
            setProfileLockedMessage('Generate/approve YAML first. Deep profile is available only after contract approval or first completed scan.');
          } else {
            setError(message);
          }
        }

        if (sampleRes.status === 'fulfilled') {
          setSample(sampleRes.value);
        } else if (!error) {
          const message = sampleRes.reason instanceof Error ? sampleRes.reason.message : 'Failed to load sample data';
          setError(message);
        }
      })
      .catch((err) => setError(err instanceof Error ? err.message : 'Failed to load wizard data'))
      .finally(() => setLoading(false));
  }, [open, datasetName]);

  const profileColumns = useMemo(() => {
    const columns = (profile?.column_profiles || profile?.columns || {}) as Record<string, any>;
    return Object.entries(columns);
  }, [profile]);

  const rowsCount = useMemo(() => {
    if (typeof profile?.total_rows === 'number') return profile.total_rows;
    if (typeof sample?.total_rows === 'number') return sample.total_rows;
    return Array.isArray(sample?.data) ? sample.data.length : 0;
  }, [profile?.total_rows, sample?.data, sample?.total_rows]);

  const detectedColumns = useMemo(() => {
    if (profileColumns.length > 0) {
      return profileColumns.map(([name, stats]) => ({
        name,
        type: String(stats?.type || stats?.data_type || 'unknown'),
        nullCount: String(stats?.null_count ?? '-'),
        sampleValue:
          sample?.data?.[0]?.[name as keyof Record<string, any>] != null
            ? String(sample.data[0][name as keyof Record<string, any>]).slice(0, 60)
            : '-',
      }));
    }

    const cols = Array.isArray(sample?.columns) ? sample.columns : [];
    const firstRow = (sample?.data?.[0] || {}) as Record<string, any>;
    return cols.map((name) => ({
      name,
      type: inferType(firstRow[name]),
      nullCount: '-',
      sampleValue: firstRow[name] != null ? String(firstRow[name]).slice(0, 60) : '-',
    }));
  }, [profileColumns, sample?.columns, sample?.data]);

  if (!open || !datasetName) return null;

  const generateProposal = async () => {
    setLoading(true);
    setError(null);
    setProposalRunNotice(null);
    try {
      const result = await proposeContract(datasetName, filePath ?? null);
      setProposedYaml(String(result?.proposed_yaml || ''));
      if (result?.scan?.enqueued && result?.scan?.job_id) {
        setProposalRunNotice(`Initial run queued automatically (${result.scan.job_id}).`);
      } else if (result?.scan?.error) {
        setProposalRunNotice(`Proposal generated, but auto-run could not start: ${result.scan.error}`);
      }
      setStep(2);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to generate contract proposal');
    } finally {
      setLoading(false);
    }
  };

  const approve = async () => {
    if (!proposedYaml.trim()) {
      setError('Proposal YAML is empty');
      return;
    }
    setStep(3);
    setApprovalState('submitting');
    setLoading(true);
    setError(null);
    try {
      await approveContract(datasetName, proposedYaml);
      setApprovalState('approved');
      onCompleted?.();
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Failed to approve contract';
      setStep(2);
      setApprovalState('idle');
      setError(message);
    } finally {
      setLoading(false);
    }
  };

  const applyAiModification = async () => {
    if (!datasetName) return;
    const instruction = aiInstruction.trim();
    if (!instruction) {
      setError('Enter an AI instruction first');
      return;
    }
    if (!proposedYaml.trim()) {
      setError('Generate proposal YAML first');
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const result = await aiModifyContract(datasetName, {
        instruction,
        current_yaml: proposedYaml,
      });
      setProposedYaml(String(result.modified_yaml || proposedYaml));
      setAiInstruction('');
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to AI-modify proposal');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/40">
      <div className="w-full max-w-6xl max-h-[90vh] overflow-hidden rounded-2xl border border-border bg-card shadow-xl">
        <div className="px-5 py-4 border-b border-border flex items-center justify-between">
          <div>
            <div className="text-lg font-semibold">Contract Wizard</div>
            <div className="text-xs text-muted-foreground uppercase tracking-wide mt-1">{datasetName}</div>
          </div>
          <div className="flex items-center gap-2 text-xs font-medium">
            <span className={step >= 1 ? 'text-foreground' : 'text-muted-foreground'}>1. Profile</span>
            <span className="text-muted-foreground">/</span>
            <span className={step >= 2 ? 'text-foreground' : 'text-muted-foreground'}>2. Propose</span>
            <span className="text-muted-foreground">/</span>
            <span className={step >= 3 ? 'text-foreground' : 'text-muted-foreground'}>3. Approve</span>
          </div>
          <button onClick={onClose} className="rounded-lg p-2 hover:bg-muted text-muted-foreground">
            <X size={18} />
          </button>
        </div>

        {loading && (
          <div className="px-5 py-4 border-b border-border bg-muted/30 text-sm flex items-center gap-2 text-muted-foreground">
            <Loader2 size={16} className="animate-spin" /> Processing...
          </div>
        )}
        {error && (
          <div className="px-5 py-3 border-b border-border bg-rose-50 text-rose-700 text-sm">{error}</div>
        )}

        <div className="p-5 overflow-auto max-h-[70vh]">
          {step === 1 && (
            <div className="space-y-4">
              <div className="text-sm font-medium">Step 1: Profile Dataset</div>
              {profileLockedMessage && (
                <div className="rounded-xl border border-amber-300/70 bg-amber-50 px-3 py-2 text-sm text-amber-900">
                  {profileLockedMessage}
                </div>
              )}
              <div className="grid grid-cols-1 lg:grid-cols-4 gap-3">
                <div className="rounded-xl border border-border p-3 bg-muted/20">
                  <div className="text-xs uppercase tracking-wide text-muted-foreground">Rows</div>
                  <div className="mt-1 text-xl font-semibold">{Number(rowsCount || 0).toLocaleString()}</div>
                </div>
                <div className="rounded-xl border border-border p-3 bg-muted/20">
                  <div className="text-xs uppercase tracking-wide text-muted-foreground">Columns</div>
                  <div className="mt-1 text-xl font-semibold">{detectedColumns.length}</div>
                </div>
                <div className="rounded-xl border border-border p-3 bg-muted/20">
                  <div className="text-xs uppercase tracking-wide text-muted-foreground">Quality</div>
                  <div className="mt-1 text-xl font-semibold">
                    {typeof profile?.overall_quality_score === 'number' ? `${profile.overall_quality_score.toFixed(1)}%` : 'N/A'}
                  </div>
                </div>
                <div className="rounded-xl border border-border p-3 bg-muted/20">
                  <div className="text-xs uppercase tracking-wide text-muted-foreground">Source</div>
                  <div className="mt-1 text-xl font-semibold">{filePath?.split('.').pop()?.toUpperCase() || 'AUTO'}</div>
                </div>
              </div>

              <div className="rounded-xl border border-border overflow-hidden">
                <div className="px-4 py-3 border-b border-border text-sm font-medium">Detected Columns</div>
                <div className="overflow-x-auto">
                  <table className="min-w-full text-sm">
                    <thead className="bg-muted/30 text-muted-foreground">
                      <tr>
                        <th className="text-left px-4 py-2">Column</th>
                        <th className="text-left px-4 py-2">Type</th>
                        <th className="text-left px-4 py-2">Nulls</th>
                        <th className="text-left px-4 py-2">Sample</th>
                      </tr>
                    </thead>
                    <tbody>
                      {detectedColumns.map((col) => (
                        <tr key={col.name} className="border-t border-border/60">
                          <td className="px-4 py-2 font-medium">{col.name}</td>
                          <td className="px-4 py-2 text-muted-foreground">{col.type}</td>
                          <td className="px-4 py-2 text-muted-foreground">{col.nullCount}</td>
                          <td className="px-4 py-2 text-muted-foreground">{col.sampleValue}</td>
                        </tr>
                      ))}
                      {detectedColumns.length === 0 && (
                        <tr className="border-t border-border/60">
                          <td className="px-4 py-3 text-muted-foreground" colSpan={4}>
                            No columns detected yet.
                          </td>
                        </tr>
                      )}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          )}

          {step === 2 && (
            <div className="space-y-4">
              <div className="text-sm font-medium flex items-center gap-2">
                <Sparkles size={14} className="text-orange-600" />
                Step 2: Proposed Contract
              </div>
              {proposalRunNotice && (
                <div className="rounded-xl border border-blue-300/70 bg-blue-50 px-3 py-2 text-sm text-blue-900">
                  {proposalRunNotice}
                </div>
              )}
              <div className="rounded-xl border border-border bg-background p-3 space-y-2">
                <div className="text-xs uppercase tracking-wide text-muted-foreground">AI Proposal Edit</div>
                <div className="flex gap-2">
                  <input
                    value={aiInstruction}
                    onChange={(e) => setAiInstruction(e.target.value)}
                    placeholder='e.g. "Make Age non-nullable and add min/max constraints"'
                    className="flex-1 rounded-lg border border-border bg-background px-3 py-2 text-sm outline-none focus:ring-2 focus:ring-ring/40"
                  />
                  <button
                    onClick={() => void applyAiModification()}
                    disabled={loading || !aiInstruction.trim() || !proposedYaml.trim()}
                    className="rounded-lg border border-border bg-background px-3 py-2 text-sm font-medium hover:bg-accent disabled:opacity-50 inline-flex items-center gap-2"
                  >
                    <Sparkles size={14} /> Apply AI
                  </button>
                </div>
              </div>
              <div className="rounded-xl border border-border overflow-hidden">
                <div className="px-4 py-2 border-b border-border text-xs uppercase tracking-wide text-muted-foreground">
                  Editable YAML
                </div>
                <textarea
                  className="w-full min-h-[420px] bg-zinc-950 text-green-400 font-mono text-xs p-4 outline-none"
                  value={proposedYaml}
                  onChange={(e) => setProposedYaml(e.target.value)}
                  spellCheck={false}
                />
              </div>
            </div>
          )}

          {step === 3 && (
            <div className="py-16 flex flex-col items-center text-center">
              {approvalState === 'submitting' ? (
                <>
                  <div className="w-16 h-16 rounded-full bg-blue-100 text-blue-700 flex items-center justify-center mb-4">
                    <Loader2 size={30} className="animate-spin" />
                  </div>
                  <div className="text-xl font-semibold">Approving Contract...</div>
                  <div className="text-sm text-muted-foreground mt-2">
                    Finalizing approval and validating the dataset.
                  </div>
                </>
              ) : (
                <>
                  <div className="w-16 h-16 rounded-full bg-emerald-100 text-emerald-700 flex items-center justify-center mb-4">
                    <CheckCircle2 size={30} />
                  </div>
                  <div className="text-xl font-semibold">Contract Approved</div>
                  <div className="text-sm text-muted-foreground mt-2">
                    {datasetName} is now managed and pending files will validate through the normal pipeline.
                  </div>
                </>
              )}
            </div>
          )}
        </div>

        <div className="px-5 py-4 border-t border-border bg-muted/20 flex justify-end gap-2">
          {step === 1 && (
            <button
              onClick={() => void generateProposal()}
              disabled={loading}
              className="rounded-xl bg-foreground text-background px-4 py-2 text-sm font-medium hover:opacity-90 disabled:opacity-50"
            >
              Generate Proposal
            </button>
          )}
          {step === 2 && (
            <>
              <button
                onClick={() => setStep(1)}
                className="rounded-xl border border-border bg-card px-4 py-2 text-sm font-medium hover:bg-muted"
              >
                Back
              </button>
              <button
                onClick={() => void approve()}
                disabled={loading}
                className="rounded-xl bg-emerald-600 text-white px-4 py-2 text-sm font-medium hover:bg-emerald-700 disabled:opacity-50"
              >
                Approve Contract
              </button>
            </>
          )}
          {step === 3 && approvalState === 'approved' && (
            <button onClick={onClose} className="rounded-xl bg-foreground text-background px-4 py-2 text-sm font-medium">
              Done
            </button>
          )}
        </div>
      </div>
    </div>
  );
}
