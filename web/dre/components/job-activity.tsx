import type { AsyncJob } from '@/lib/dre-api';

type Props = {
  jobs: AsyncJob[];
};

function statusClass(status: string): string {
  if (status === 'COMPLETED') return 'bg-emerald-50 text-emerald-700 ring-1 ring-emerald-200';
  if (status === 'FAILED') return 'bg-rose-50 text-rose-700 ring-1 ring-rose-200';
  if (status === 'RUNNING') return 'bg-blue-50 text-blue-700 ring-1 ring-blue-200';
  return 'bg-amber-50 text-amber-700 ring-1 ring-amber-200';
}

function formatTs(iso?: string | null): string {
  if (!iso) return 'n/a';
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return 'n/a';
  return date.toLocaleTimeString();
}

export default function JobActivity({ jobs }: Props) {
  const activeCount = jobs.filter((job) => job.status === 'QUEUED' || job.status === 'RUNNING').length;

  return (
    <section className="rounded-xl border border-border bg-card overflow-hidden">
      <div className="px-4 py-3 border-b border-border flex items-center justify-between gap-3">
        <div className="text-sm font-medium">Background Jobs</div>
        <div className="text-xs text-muted-foreground">Active: {activeCount}</div>
      </div>

      {jobs.length === 0 ? (
        <div className="px-4 py-4 text-sm text-muted-foreground">No jobs yet.</div>
      ) : (
        <div className="max-h-56 overflow-y-auto">
          <table className="min-w-full text-sm">
            <thead className="bg-muted/30 text-muted-foreground">
              <tr>
                <th className="text-left px-4 py-2">Action</th>
                <th className="text-left px-4 py-2">Dataset</th>
                <th className="text-left px-4 py-2">Status</th>
                <th className="text-left px-4 py-2">Requested</th>
              </tr>
            </thead>
            <tbody>
              {jobs.map((job) => (
                <tr key={job.job_id} className="border-t border-border/60">
                  <td className="px-4 py-2 text-foreground">{job.action}</td>
                  <td className="px-4 py-2 text-foreground">{job.dataset_name}</td>
                  <td className="px-4 py-2">
                    <span className={`inline-flex rounded-full px-2 py-0.5 text-xs font-medium ${statusClass(job.status)}`}>
                      {job.status}
                    </span>
                  </td>
                  <td className="px-4 py-2 text-muted-foreground">{formatTs(job.requested_at)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}
