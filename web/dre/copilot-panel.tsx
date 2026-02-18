'use client';

import { useChat } from '@ai-sdk/react';
import { DefaultChatTransport } from 'ai';
import { Bot, ChevronRight, Send, Square, User } from 'lucide-react';
import { useEffect, useMemo, useRef, useState } from 'react';
import type { ReactNode } from 'react';

import { parseDeepDiveEnvelope } from '@/dre/copilot-constants';
import type { CopilotMessage } from '@/dre/copilot-types';

type Props = {
  open: boolean;
  onClose: () => void;
  initialMessage?: { id: string; text: string } | null;
};

type PendingPart = Extract<CopilotMessage['parts'][number], { type: 'tool-showPendingContracts' }>;
type PulsePart = Extract<CopilotMessage['parts'][number], { type: 'tool-showPulseSnapshot' }>;
type SloPart = Extract<CopilotMessage['parts'][number], { type: 'tool-showSloSummary' }>;
type FailureEvidencePart = Extract<CopilotMessage['parts'][number], { type: 'tool-showFailureEvidence' }>;

function renderInlineMarkdown(text: string, keyPrefix: string) {
  const segments = text.split(/(\*\*[^*]+\*\*)/g).filter(Boolean);
  return segments.map((segment, idx) => {
    if (segment.startsWith('**') && segment.endsWith('**') && segment.length > 4) {
      return <strong key={`${keyPrefix}-b-${idx}`}>{segment.slice(2, -2)}</strong>;
    }
    return <span key={`${keyPrefix}-t-${idx}`}>{segment}</span>;
  });
}

function renderStructuredText(text: string) {
  const blocks = text
    .split(/\n{2,}/)
    .map((block) => block.trim())
    .filter(Boolean);
  if (blocks.length === 0) return null;

  return (
    <div className="space-y-2">
      {blocks.map((block, blockIndex) => {
        const lines = block
          .split('\n')
          .map((line) => line.trimEnd())
          .filter((line) => line.length > 0);
        if (lines.length === 0) return null;

        const bulletList = lines.every((line) => /^[-*]\s+/.test(line));
        const numberedList = lines.every((line) => /^\d+\.\s+/.test(line));

        if (bulletList) {
          return (
            <ul key={`ul-${blockIndex}`} className="list-disc list-inside space-y-1 text-sm leading-relaxed">
              {lines.map((line, idx) => (
                <li key={`ul-li-${idx}`}>{renderInlineMarkdown(line.replace(/^[-*]\s+/, ''), `ul-${blockIndex}-${idx}`)}</li>
              ))}
            </ul>
          );
        }

        if (numberedList) {
          return (
            <ol key={`ol-${blockIndex}`} className="list-decimal list-inside space-y-1 text-sm leading-relaxed">
              {lines.map((line, idx) => (
                <li key={`ol-li-${idx}`}>{renderInlineMarkdown(line.replace(/^\d+\.\s+/, ''), `ol-${blockIndex}-${idx}`)}</li>
              ))}
            </ol>
          );
        }

        return (
          <p key={`p-${blockIndex}`} className="text-sm leading-relaxed whitespace-pre-wrap">
            {lines.map((line, idx) => (
              <span key={`p-line-${idx}`}>
                {renderInlineMarkdown(line, `p-${blockIndex}-${idx}`)}
                {idx < lines.length - 1 ? '\n' : ''}
              </span>
            ))}
          </p>
        );
      })}
    </div>
  );
}

function ToolCard({ title, children }: { title: string; children: ReactNode }) {
  return (
    <div className="rounded-xl border border-border bg-muted/15 p-3 space-y-2 transition-colors hover:bg-muted/25">
      <div className="text-xs uppercase tracking-wide text-muted-foreground font-medium">{title}</div>
      {children}
    </div>
  );
}

function formatTime(iso?: string | null): string {
  if (!iso) return 'n/a';
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return 'n/a';
  return date.toLocaleString();
}

function renderPendingPart(part: PendingPart) {
  if (part.state !== 'output-available' || !part.output) {
    return <div className="text-xs text-muted-foreground">Loading pending contracts...</div>;
  }

  return (
    <ToolCard title="Pending Contracts">
      <div className="text-sm text-foreground">{part.output.count} datasets require YAML approval.</div>
      <div className="space-y-1">
        {part.output.datasets.slice(0, 6).map((item) => (
          <div key={`${item.dataset_name}-${item.proposed_at}`} className="text-xs text-muted-foreground">
            {item.dataset_name} · files: {item.pending_files}
          </div>
        ))}
      </div>
    </ToolCard>
  );
}

function renderPulsePart(part: PulsePart) {
  if (part.state !== 'output-available' || !part.output) {
    return <div className="text-xs text-muted-foreground">Loading pulse snapshot...</div>;
  }

  return (
    <ToolCard title="Health Snapshot">
      <div className="grid grid-cols-4 gap-2 text-center text-xs">
        <div className="rounded-lg border border-border bg-background p-2">Total<br />{part.output.total}</div>
        <div className="rounded-lg border border-border bg-background p-2">Pass<br />{part.output.healthy}</div>
        <div className="rounded-lg border border-border bg-background p-2">Warn<br />{part.output.warning}</div>
        <div className="rounded-lg border border-border bg-background p-2">Block<br />{part.output.blocked}</div>
      </div>
      <div className="space-y-1">
        {part.output.rows.slice(0, 4).map((row) => (
          <div key={row.name} className="text-xs text-muted-foreground">
            {row.name}: {row.status}
          </div>
        ))}
      </div>
    </ToolCard>
  );
}

function renderSloPart(part: SloPart) {
  if (part.state !== 'output-available' || !part.output) {
    return <div className="text-xs text-muted-foreground">Loading SLO summary...</div>;
  }

  return (
    <ToolCard title={`SLO Summary · ${part.output.dataset_name}`}>
      {part.output.summary.length === 0 ? (
        <div className="text-xs text-muted-foreground">No SLO rows found for this dataset.</div>
      ) : (
        <div className="space-y-1">
          {part.output.summary.slice(0, 5).map((row) => (
            <div key={row.slo_name} className="text-xs text-muted-foreground">
              {row.slo_name}: {row.attainment.toFixed(1)}% ({row.passed_runs}/{row.total_runs})
            </div>
          ))}
        </div>
      )}
      <div className="text-[11px] text-muted-foreground">Generated: {formatTime(part.output.generated_at)}</div>
    </ToolCard>
  );
}

function renderFailureEvidencePart(part: FailureEvidencePart) {
  if (part.state !== 'output-available' || !part.output) {
    return <div className="text-xs text-muted-foreground">Loading failure evidence...</div>;
  }

  const rows = Array.isArray(part.output.sample_rows) ? part.output.sample_rows : [];
  const columns = Array.isArray(part.output.sample_columns) ? part.output.sample_columns : [];

  return (
    <ToolCard title={`Failure Evidence · ${part.output.dataset_name}`}>
      <div className="text-xs text-muted-foreground">
        Run: {part.output.run_id || 'n/a'} · Status: {part.output.run_status || 'UNKNOWN'}
      </div>
      {part.output.run_reason && <div className="text-xs text-muted-foreground">Reason: {part.output.run_reason}</div>}

      {Array.isArray(part.output.evidence_summary) && part.output.evidence_summary.length > 0 ? (
        <div className="space-y-1">
          {part.output.evidence_summary.slice(0, 6).map((row, idx) => (
            <div key={`${row.check_type}-${idx}`} className="text-xs text-muted-foreground">
              {row.check_type}: violations {row.violation_count} · sample rows {row.sample_count}
              {row.column_name ? ` · ${row.column_name}` : ''}
            </div>
          ))}
        </div>
      ) : (
        <div className="text-xs text-muted-foreground">No diagnostics evidence found for the selected scope.</div>
      )}

      {rows.length > 0 && columns.length > 0 && (
        <div className="overflow-auto rounded-lg border border-border max-h-44">
          <table className="w-full text-xs">
            <thead className="bg-muted/30">
              <tr>
                {columns.slice(0, 8).map((column) => (
                  <th key={column} className="px-2 py-1.5 text-left text-muted-foreground">
                    {column}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.slice(0, 5).map((row, idx) => (
                <tr key={`r-${idx}`} className="border-t border-border">
                  {columns.slice(0, 8).map((column) => (
                    <td key={`${idx}-${column}`} className="px-2 py-1.5 align-top text-muted-foreground">
                      {row[column] == null ? '' : String(row[column])}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      <div className="text-[11px] text-muted-foreground">Generated: {formatTime(part.output.generated_at)}</div>
    </ToolCard>
  );
}

function renderAssistantPart(part: CopilotMessage['parts'][number], key: string) {
  if (part.type === 'text') {
    return (
      <div key={key}>{renderStructuredText(part.text)}</div>
    );
  }

  if (part.type === 'tool-showPendingContracts') {
    return <div key={key}>{renderPendingPart(part)}</div>;
  }

  if (part.type === 'tool-showPulseSnapshot') {
    return <div key={key}>{renderPulsePart(part)}</div>;
  }

  if (part.type === 'tool-showSloSummary') {
    return <div key={key}>{renderSloPart(part)}</div>;
  }

  if (part.type === 'tool-showFailureEvidence') {
    return <div key={key}>{renderFailureEvidencePart(part)}</div>;
  }

  return null;
}

function renderUserMessage(message: CopilotMessage) {
  const text = message.parts
    .filter((part) => part.type === 'text')
    .map((part) => part.text)
    .join('')
    .trim();

  if (!text) return null;

  const deepDive = parseDeepDiveEnvelope(text);
  const displayText = deepDive ? deepDive.visible_prompt : text;

  return <div className="text-sm leading-relaxed whitespace-pre-wrap">{displayText}</div>;
}

export default function CopilotPanel({ open, onClose, initialMessage = null }: Props) {
  const [draft, setDraft] = useState('');
  const endRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLTextAreaElement>(null);
  const consumedInitialIdRef = useRef<string | null>(null);

  const transport = useMemo(() => new DefaultChatTransport<CopilotMessage>({ api: '/api/chat/ui' }), []);

  const { messages, sendMessage, status, stop, error } = useChat<CopilotMessage>({ transport });

  const isStreaming = status === 'submitted' || status === 'streaming';

  useEffect(() => {
    endRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, status]);

  useEffect(() => {
    const el = inputRef.current;
    if (!el) return;
    el.style.height = '0px';
    const nextHeight = Math.min(Math.max(el.scrollHeight, 44), 140);
    el.style.height = `${nextHeight}px`;
  }, [draft]);

  useEffect(() => {
    if (!open || !initialMessage?.id || !initialMessage.text.trim()) return;
    if (consumedInitialIdRef.current === initialMessage.id) return;

    consumedInitialIdRef.current = initialMessage.id;
    void sendMessage({ text: initialMessage.text.trim() });
  }, [open, initialMessage, sendMessage]);

  if (!open) return null;

  const sendDraft = () => {
    const text = draft.trim();
    if (!text || isStreaming) return;
    setDraft('');
    void sendMessage({ text });
  };

  return (
    <aside className="w-[460px] max-w-[45vw] border-l border-border bg-background flex flex-col">
      <div className="px-5 py-4 border-b border-border bg-background/80 backdrop-blur flex items-center justify-between">
        <div>
          <div className="text-sm font-semibold text-foreground">DataPulse Copilot</div>
          <div className="text-[11px] text-muted-foreground uppercase tracking-wide">
            {isStreaming ? 'streaming' : 'ready'}
          </div>
        </div>
        <button onClick={onClose} className="p-2 rounded-lg hover:bg-accent text-muted-foreground transition-colors">
          <ChevronRight size={18} />
        </button>
      </div>

      <div className="flex-1 overflow-y-auto px-4 py-5 space-y-4 scroll-smooth">
        {messages.length === 0 && (
          <div className="text-sm text-muted-foreground border border-border rounded-xl p-3 bg-card animate-in fade-in duration-200">
            Ask things like: "show pending contracts", "health pulse summary", "SLO budget for yellow_tripdata".
          </div>
        )}

        {messages.map((message) => {
          const isUser = message.role === 'user';

          return (
            <div key={message.id} className={`flex gap-2 items-start transition-all duration-200 ${isUser ? 'justify-end' : 'justify-start'}`}>
              {!isUser && (
                <div className="w-7 h-7 rounded-full bg-muted text-foreground flex items-center justify-center mt-1">
                  <Bot size={14} />
                </div>
              )}

              <div
                className={`max-w-[85%] rounded-2xl px-3 py-2 text-sm leading-relaxed space-y-2 ${
                  isUser
                    ? 'bg-foreground text-background rounded-tr-none shadow-sm'
                    : 'bg-card text-foreground border border-border rounded-tl-none shadow-sm'
                }`}
              >
                {isUser
                  ? renderUserMessage(message)
                  : message.parts.map((part, index) => renderAssistantPart(part, `${message.id}-${index}`))}
              </div>

              {isUser && (
                <div className="w-7 h-7 rounded-full bg-foreground text-background flex items-center justify-center mt-1">
                  <User size={14} />
                </div>
              )}
            </div>
          );
        })}

        {isStreaming && (
          <div className="flex gap-2 items-start">
            <div className="w-7 h-7 rounded-full bg-muted text-foreground flex items-center justify-center mt-1">
              <Bot size={14} />
            </div>
            <div className="rounded-2xl rounded-tl-none border border-border bg-card px-3 py-2 text-xs text-muted-foreground">
              Working...
            </div>
          </div>
        )}

        {error && (
          <div className="text-xs text-foreground bg-muted border border-border rounded-lg p-2">
            {error.message}
          </div>
        )}

        <div ref={endRef} />
      </div>

      <div className="p-4 border-t border-border">
        <div className="relative">
          <textarea
            ref={inputRef}
            value={draft}
            onChange={(event) => setDraft(event.target.value)}
            onKeyDown={(event) => {
              if (event.key === 'Enter' && !event.shiftKey) {
                event.preventDefault();
                sendDraft();
              }
            }}
            rows={1}
            placeholder="Ask the copilot..."
            className="w-full resize-none rounded-xl border border-border bg-background px-3 py-2 pr-24 text-sm text-foreground focus:outline-none focus:ring-2 focus:ring-ring/40 transition-shadow"
          />
          <div className="absolute right-12 bottom-1.5 text-[10px] text-muted-foreground pointer-events-none">Enter send · Shift+Enter newline</div>

          {isStreaming ? (
            <button
              onClick={() => void stop()}
              className="absolute right-2 top-2 p-2 rounded-lg border border-border bg-muted text-foreground"
              title="Stop"
            >
              <Square size={14} />
            </button>
          ) : (
            <button
              onClick={sendDraft}
              disabled={!draft.trim()}
              className="absolute right-2 top-2 p-2 rounded-lg bg-foreground text-background disabled:opacity-50 transition-opacity"
              title="Send"
            >
              <Send size={14} />
            </button>
          )}
        </div>
      </div>
    </aside>
  );
}
