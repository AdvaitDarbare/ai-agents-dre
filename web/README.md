# DataPulse Next.js Frontend

This app is the primary DataPulse frontend.
It was bootstrapped from the Vercel AI chatbot template and adapted to this project's FastAPI backend.

## Run

```bash
cd web
cp .env.example .env.local
npm install --legacy-peer-deps
npm run dev
```

Open: `http://localhost:3000`

Production build check:

```bash
npm run build
```

## Backend

The app uses a Next.js server proxy (`/api/backend/*`) to reach the FastAPI backend.

- Server env: set `BACKEND_URL` (preferred for Vercel) or `NEXT_PUBLIC_BACKEND_URL` (local dev).
- Default backend: `http://127.0.0.1:8000`

## AI SDK Copilot

- UI uses `@ai-sdk/react` + `DefaultChatTransport`
- Text stream proxy: `POST /api/chat/stream` -> backend `POST /chat/stream`
- Generative UI stream: `POST /api/chat/ui` (AI SDK UIMessage tool parts)

## Notes

- Legacy Vite frontend in `frontend/` is archived as reference only.
- Dashboard actions now enqueue backend async jobs (`/jobs/*`) and show job activity/state in UI.
- Next dashboard now includes parity-focused tabs from the legacy UI:
  - Overview (pulse, pending contracts, SLO inspector, contract review queue)
  - History (recent runs)
  - Incidents (lifecycle actions: ACK/RESOLVE)
  - Lineage (dataset upstream/consumer map)
  - Workflow (resume demo mode hero, live incident feed, one-click investigate + AI brief)
  - Settings (runtime config + danger-zone runtime reset action)
- Original Vercel template files that are not currently active are parked in `web/_template_backup/`.
