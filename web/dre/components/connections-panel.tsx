'use client';

import { useEffect, useMemo, useState } from 'react';
import { Activity, AlertTriangle, Database, Plus, Server } from 'lucide-react';

import { getSourceIntegrations, type SourceIntegration } from '@/lib/dre-api';

function statusClass(status: string): string {
  if (status === 'CONNECTED') return 'bg-emerald-100 text-emerald-700';
  if (status === 'ERROR') return 'bg-rose-100 text-rose-700';
  return 'bg-zinc-200 text-zinc-700';
}

function iconForIntegration(id: string): typeof Database {
  if (id.includes('postgres')) return Server;
  if (id.includes('kafka') || id.includes('stream')) return Activity;
  return Database;
}

function toLocal(ts?: string): string {
  if (!ts) return 'Unknown';
  const dt = new Date(ts);
  if (Number.isNaN(dt.getTime())) return 'Unknown';
  return new Intl.DateTimeFormat(undefined, {
    hour: 'numeric',
    minute: '2-digit',
    second: '2-digit',
    hour12: true,
  }).format(dt);
}

export default function ConnectionsPanel() {
  const [integrations, setIntegrations] = useState<SourceIntegration[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    const run = async () => {
      setLoading(true);
      setError(null);
      try {
        const payload = await getSourceIntegrations();
        if (cancelled) return;
        setIntegrations(payload.integrations || []);
      } catch (e) {
        if (cancelled) return;
        setError(e instanceof Error ? e.message : 'Failed to load integrations');
        setIntegrations([]);
      } finally {
        if (!cancelled) setLoading(false);
      }
    };
    void run();
    return () => {
      cancelled = true;
    };
  }, []);

  const connectedCount = useMemo(
    () => integrations.filter((item) => String(item.status).toUpperCase() === 'CONNECTED').length,
    [integrations],
  );

  return (
    <section className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h3 className="text-base font-medium">Active Data Connections</h3>
          <p className="text-sm text-muted-foreground">
            Runtime-discovered connectors and dataset coverage.
          </p>
        </div>
        <button
          disabled
          className="inline-flex items-center gap-2 rounded-xl border border-border bg-card px-3 py-2 text-sm font-medium text-muted-foreground"
        >
          <Plus size={14} />
          Add Connection (Read-only)
        </button>
      </div>

      <div className="rounded-xl border border-border bg-card p-3 text-sm text-muted-foreground">
        {loading ? 'Loading integration state...' : `${connectedCount}/${integrations.length} connectors healthy`}
        {error && (
          <div className="mt-2 inline-flex items-center gap-2 text-rose-700">
            <AlertTriangle size={14} />
            {error}
          </div>
        )}
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-4">
        {integrations.map((conn) => {
          const Icon = iconForIntegration(conn.id);
          return (
            <article key={conn.id} className="rounded-xl border border-border bg-card p-4">
              <div className="flex items-start justify-between">
                <div className="rounded-lg bg-muted p-2 text-muted-foreground">
                  <Icon size={16} />
                </div>
                <span
                  className={`rounded-full px-2 py-1 text-[10px] font-semibold uppercase tracking-wide ${statusClass(String(conn.status).toUpperCase())}`}
                >
                  {conn.status}
                </span>
              </div>
              <div className="mt-3 text-sm font-medium">{conn.name}</div>
              <div className="text-xs uppercase tracking-wide text-muted-foreground mt-1">{conn.type || 'Connector'}</div>
              <div className="mt-4 text-xs text-muted-foreground flex items-center justify-between">
                <span>{conn.dataset_count} datasets</span>
                <span>{toLocal(conn.last_checked)}</span>
              </div>
              {conn.details?.error && <div className="mt-2 text-xs text-rose-700">{String(conn.details.error)}</div>}
            </article>
          );
        })}
        {integrations.length === 0 && !loading && (
          <div className="col-span-full rounded-xl border border-border bg-card p-6 text-sm text-muted-foreground">
            No connectors configured.
          </div>
        )}
      </div>
    </section>
  );
}
