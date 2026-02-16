import { useMemo, useRef, useEffect, useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { ChevronRight, Send, Square, Zap, AlertCircle, Bot, User } from 'lucide-react';
import { useChat } from '@ai-sdk/react';
import { TextStreamChatTransport } from 'ai';

import { API_BASE_URL } from '../api';

function getMessageText(message) {
  if (!message) return '';

  if (typeof message.content === 'string') {
    return message.content;
  }

  if (Array.isArray(message.parts)) {
    return message.parts
      .filter((part) => part?.type === 'text' && typeof part.text === 'string')
      .map((part) => part.text)
      .join('');
  }

  if (Array.isArray(message.content)) {
    return message.content
      .filter((part) => part?.type === 'text' && typeof part.text === 'string')
      .map((part) => part.text)
      .join('');
  }

  return '';
}

export default function CopilotPanel({ isOpen, onClose }) {
  const [draft, setDraft] = useState('');
  const endRef = useRef(null);

  const transport = useMemo(
    () => new TextStreamChatTransport({ api: `${API_BASE_URL}/chat/stream` }),
    [],
  );

  const { messages, sendMessage, status, error, stop } = useChat({ transport });

  const isStreaming = status === 'submitted' || status === 'streaming';

  useEffect(() => {
    endRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, status]);

  const onSubmit = async () => {
    const text = draft.trim();
    if (!text || isStreaming) return;
    setDraft('');
    await sendMessage({ text });
  };

  return (
    <AnimatePresence>
      {isOpen && (
        <motion.aside
          initial={{ x: 400, opacity: 0 }}
          animate={{ x: 0, opacity: 1 }}
          exit={{ x: 400, opacity: 0 }}
          transition={{ type: 'spring', damping: 25, stiffness: 200 }}
          className="w-[420px] bg-card border-l border-border flex flex-col shadow-[-20px_0_40px_rgba(0,0,0,0.02)] z-30"
        >
          <div className="p-8 border-b border-border bg-muted/50 text-foreground">
            <div className="flex items-center justify-between mb-6">
              <div className="flex items-center gap-3">
                <div className="w-10 h-10 rounded-2xl bg-slate-900 shadow-lg shadow-black/10 flex items-center justify-center text-primary border border-slate-800">
                  <Zap size={20} className="fill-primary" />
                </div>
                <div>
                  <h3 className="font-black text-foreground text-sm tracking-tight">DRE Copilot</h3>
                  <div className="flex items-center gap-1.5 mt-0.5">
                    <div className={`w-1.5 h-1.5 rounded-full ${isStreaming ? 'bg-orange-500 shadow-[0_0_5px_rgba(249,115,22,0.6)]' : 'bg-green-500 shadow-[0_0_5px_rgba(34,197,94,0.5)]'}`} />
                    <span className="text-[10px] text-muted-foreground font-bold uppercase tracking-widest">
                      {isStreaming ? 'Streaming' : 'Ready'}
                    </span>
                  </div>
                </div>
              </div>
              <button
                onClick={onClose}
                className="p-2 hover:bg-muted/80 rounded-lg transition-colors text-muted-foreground/80"
              >
                <ChevronRight size={20} />
              </button>
            </div>

            <div className="bg-card p-4 rounded-2xl border border-border shadow-sm">
              <p className="text-[10px] font-black text-muted-foreground/80 uppercase tracking-widest mb-2">
                AI SDK Chat
              </p>
              <div className="grid grid-cols-2 gap-2">
                {['Root Cause', 'Lineage', 'Remediation', 'SLOs'].map((capability) => (
                  <div
                    key={capability}
                    className="bg-muted/50 px-2.5 py-1.5 rounded-lg text-[10px] font-bold text-muted-foreground border border-border flex items-center gap-1.5"
                  >
                    <div className="w-1 h-1 rounded-full bg-primary" />
                    {capability}
                  </div>
                ))}
              </div>
            </div>
          </div>

          <div className="flex-1 p-8 overflow-y-auto space-y-6 custom-scrollbar text-foreground">
            {messages.length === 0 && (
              <div className="bg-muted/60 border border-border rounded-2xl p-4 text-sm text-muted-foreground">
                Ask about recent incidents, quality regressions, contract mismatches, or SLO budget burn.
              </div>
            )}

            {messages.map((msg) => {
              const text = getMessageText(msg);
              if (!text) return null;
              const isUser = msg.role === 'user';

              return (
                <div key={msg.id} className={`flex gap-2 ${isUser ? 'justify-end' : 'justify-start'}`}>
                  {!isUser && (
                    <div className="w-7 h-7 rounded-full bg-primary/15 flex items-center justify-center mt-1">
                      <Bot size={14} className="text-primary" />
                    </div>
                  )}
                  <div
                    className={`max-w-[80%] p-4 rounded-2xl text-sm leading-relaxed shadow-sm ${
                      isUser
                        ? 'bg-primary text-white font-medium rounded-tr-none'
                        : 'bg-muted text-foreground/90 font-normal rounded-tl-none border border-border/50'
                    }`}
                  >
                    {text}
                  </div>
                  {isUser && (
                    <div className="w-7 h-7 rounded-full bg-blue-500/15 flex items-center justify-center mt-1">
                      <User size={14} className="text-blue-500" />
                    </div>
                  )}
                </div>
              );
            })}

            {isStreaming && (
              <div className="flex justify-start">
                <div className="bg-muted p-4 rounded-2xl rounded-tl-none border border-border/50 flex gap-1">
                  <div className="w-1 h-1 bg-muted-foreground rounded-full animate-bounce [animation-delay:-0.3s]" />
                  <div className="w-1 h-1 bg-muted-foreground rounded-full animate-bounce [animation-delay:-0.15s]" />
                  <div className="w-1 h-1 bg-muted-foreground rounded-full animate-bounce" />
                </div>
              </div>
            )}

            {error && (
              <div className="bg-rose-50 border border-rose-200 text-rose-700 rounded-xl p-3 text-xs flex items-start gap-2">
                <AlertCircle size={14} className="mt-0.5" />
                <span>{error.message || 'Copilot request failed.'}</span>
              </div>
            )}

            <div ref={endRef} />
          </div>

          <div className="p-8 bg-card border-t border-border">
            <div className="relative group">
              <textarea
                rows="1"
                value={draft}
                onChange={(e) => setDraft(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter' && !e.shiftKey) {
                    e.preventDefault();
                    onSubmit();
                  }
                }}
                placeholder="Ask about data health, incidents, or lineage..."
                className="w-full bg-muted/50 border border-border rounded-2xl pl-5 pr-14 py-4 text-sm focus:outline-none focus:ring-4 focus:ring-primary/10 focus:border-primary transition-all text-foreground placeholder:text-muted-foreground/80 resize-none font-medium"
              />

              {isStreaming ? (
                <button
                  onClick={stop}
                  className="absolute right-3 top-3 p-2 bg-muted text-foreground rounded-xl border border-border hover:bg-muted/80 transition-all"
                  title="Stop"
                >
                  <Square size={18} fill="currentColor" />
                </button>
              ) : (
                <button
                  onClick={onSubmit}
                  disabled={!draft.trim()}
                  className="absolute right-3 top-3 p-2 bg-primary text-white rounded-xl shadow-lg shadow-primary/30 hover:scale-105 active:scale-95 transition-all disabled:opacity-50 disabled:shadow-none"
                  title="Send"
                >
                  <Send size={18} fill="currentColor" />
                </button>
              )}
            </div>
          </div>
        </motion.aside>
      )}
    </AnimatePresence>
  );
}
