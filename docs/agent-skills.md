# Agent Skills Playbook

This document explains how to use Agent Skills for this repository and where they should live so Codex can load them.

## What Agent Skills Are

Agent Skills are modular capability folders with:
- `SKILL.md` (required): trigger metadata + instructions
- `agents/openai.yaml` (recommended): UI metadata
- optional `scripts/`, `references/`, `assets/`

## Important Path Rule

For Codex, installed skills must live under:
- `$CODEX_HOME/skills` (typically `~/.codex/skills`)

Skills under project-local folders like `.agent/skills/` are useful as source material, but they are not auto-loaded by Codex unless copied/installed into `$CODEX_HOME/skills`.

## Skills Added/Updated

Installed in `~/.codex/skills`:
- `agent-best-practices`
- `token-efficiency-productivity`
- `dre-datapulse-ops`

`dre-datapulse-ops` includes:
- `references/dataset-lifecycle.md` (contract-first and HITL lifecycle guidance)
- `scripts/check_datapulse_endpoints.py` (quick backend state checks)

## Recommended Workflow For This Repo

1. Author/update skills in version-controlled repo docs first.
2. Validate content quality and trigger clarity.
3. Install/sync into `~/.codex/skills/<skill-name>` for runtime use.
4. Restart Codex after installing new skills.

## Data Contract Operating Model (Recommended)

1. Preferred path: contract-first
   - Contract YAML is versioned and available before data arrives.
   - Ingestion auto-validates and dataset appears in active DataPulse views.

2. Fallback path: observation + HITL
   - Unknown dataset goes to pending queue.
   - System proposes contract.
   - Human approves/rejects.
   - Approved contract promotes dataset to active scanning.

## Maintenance Checklist

- Keep `SKILL.md` short and action-oriented.
- Keep deep details in `references/`.
- Add scripts only for deterministic, repeated tasks.
- Re-validate skills after major updates.
