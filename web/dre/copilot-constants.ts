export const COPILOT_DEEP_DIVE_ENVELOPE_PREFIX = '__DRE_DEEP_DIVE__::';

export type CopilotDeepDiveEnvelope = {
  visible_prompt: string;
  hidden_context: Record<string, unknown>;
};

export function buildDeepDiveEnvelope(envelope: CopilotDeepDiveEnvelope): string {
  return `${COPILOT_DEEP_DIVE_ENVELOPE_PREFIX}${JSON.stringify(envelope)}`;
}

export function parseDeepDiveEnvelope(value: string): CopilotDeepDiveEnvelope | null {
  const text = String(value || '').trim();
  if (!text.startsWith(COPILOT_DEEP_DIVE_ENVELOPE_PREFIX)) return null;
  const json = text.slice(COPILOT_DEEP_DIVE_ENVELOPE_PREFIX.length);
  if (!json) return null;

  try {
    const parsed = JSON.parse(json) as CopilotDeepDiveEnvelope;
    if (!parsed || typeof parsed !== 'object') return null;
    const visible_prompt = String(parsed.visible_prompt || '').trim();
    if (!visible_prompt) return null;
    const hidden_context =
      parsed.hidden_context && typeof parsed.hidden_context === 'object' ? parsed.hidden_context : {};
    return { visible_prompt, hidden_context };
  } catch {
    return null;
  }
}

