# DataPulse Frontend (Archived Reference)

Legacy React/Vite dashboard for the Agentic DRE backend.
Primary UI now lives in `web/` (Next.js).

## Stack

- React 19
- Vite
- Tailwind CSS
- Recharts
- Framer Motion
- Axios

## Local Development (Reference Only)

```bash
npm install
npm run dev
```

App runs on `http://localhost:5173` by default.

## Backend Dependency

The frontend expects the API at:

- `http://localhost:8000`

Configured in `src/api/index.js`.

Start backend first:

```bash
uvicorn src.api:app --reload
```

## Key UI Areas

- Schema Health pulse table
- Dataset cards (active + unconfigured + pending contract bar)
- Expanded dataset detail panel tabs:
  - Data Quality
  - Anomalies & Violations
  - SLOs & Budget
  - Governance & History
  - Impact Lineage
- Contract workflows (propose/save/approve/reject)
- Copilot sidebar

## Build

```bash
npm run build
npm run preview
```

## Notes

- This lane is frozen and not the active product surface.
- Keep it only for historical comparison during migration audits.
