# Coding Patterns & Conventions

## Python Backend

### Database Access
```python
# DO: Use the connection pool context manager
from src.utils.database import get_connection

with get_connection() as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT x FROM t WHERE id = %s", (some_id,))
        row = cur.fetchone()

# DON'T: Create direct connections
import psycopg2
conn = psycopg2.connect(...)  # Never do this — use the pool

# DON'T: Use f-strings in SQL
cur.execute(f"SELECT * FROM t WHERE id = {id}")  # SQL injection risk

# DON'T: Use duckdb.connect() for persistent storage
import duckdb
conn = duckdb.connect("data/system.duckdb")  # Only for in-memory DataFrames
```

### DuckDB Usage (Restricted)
DuckDB is only used in two places for **in-memory DataFrame SQL**:
- `src/tools/data_profiler.py` — `duckdb.connect()` (no file path = in-memory)
- `src/tools/schema_validator.py` — `duckdb.connect(":memory:")`

Never add new DuckDB usage for persistent storage. That goes to PostgreSQL.

### Error Handling in API Endpoints
```python
# DO: Return proper HTTP errors
@app.get("/something")
def get_something():
    try:
        result = do_work()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# DON'T: Return raw exceptions or swallow errors silently
```

### Imports
```python
# DO: Import from src.utils.database
from src.utils.database import get_connection, init_tables

# DON'T: Import duckdb in api.py or monitor_agent.py
import duckdb  # Removed — PostgreSQL only
```

## React Frontend

### Component Structure
```jsx
// Chart components go in: frontend/src/components/charts/
// Feature components go in: frontend/src/components/

// DO: Self-contained components that fetch their own data
const VolumeAnomalyChart = ({ datasetName }) => {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  // ... fetch on mount, render chart
};

// DON'T: Pass deeply nested props through 5 layers
```

### Styling Conventions
```jsx
// DO: Use Tailwind utility classes matching existing patterns
<div className="bg-white rounded-xl border border-slate-200 p-5">
  <h4 className="text-xs font-black uppercase text-slate-500 tracking-wider">
    Title
  </h4>
</div>

// DO: Use consistent color coding for severity
// Emerald: PASSED/good     (bg-emerald-50, text-emerald-600)
// Amber: WARNING           (bg-amber-50, text-amber-600)
// Rose: BLOCKED/critical   (bg-rose-50, text-rose-600)
// Slate: neutral/disabled  (bg-slate-100, text-slate-400)
// Primary cyan: brand      (#13c8ec / primary from tailwind.config.js)

// DO: Use these font patterns
// Headers: text-xs font-black uppercase tracking-wider text-slate-500
// Values: text-xl font-black text-slate-700
// Labels: text-[10px] font-bold text-slate-400
// Badges: text-[10px] font-black uppercase px-2 py-0.5 rounded-full
```

### Chart Components (Recharts)
```jsx
// DO: Use ResponsiveContainer with fixed height
<ResponsiveContainer width="100%" height={220}>
  <ComposedChart data={chartData} margin={{ top: 5, right: 10, left: 0, bottom: 0 }}>
    <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
    <XAxis tick={{ fontSize: 10, fill: "#94a3b8" }} />
    <YAxis tick={{ fontSize: 10, fill: "#94a3b8" }} />
    ...
  </ComposedChart>
</ResponsiveContainer>

// DON'T: Use Tremor or other chart libraries (we standardized on Recharts)
```

### API Client
```javascript
// All API functions live in frontend/src/api/index.js
// DO: Add new endpoints there, import from './api'
export const getNewThing = (id) => api.get(`/new-thing/${id}`);

// DON'T: Use raw fetch() or create new axios instances
```

## Data Contracts (YAML)

```yaml
# config/expectations/{dataset_name}.yaml
info:
  owner: "Data Team"
  domain: "Finance"
  lifecycle: active          # active | deprecated
  version: "1.2.0"

columns:
  - name: transaction_id
    data_type: VARCHAR
    nullable: false
    isPrimaryKey: true
  - name: amount
    data_type: DOUBLE
    nullable: false
    min_value: 0

quality:
  anomaly_thresholds:
    z_score_warning: 2.5     # Soft gate threshold
    z_score_critical: 3.0    # Hard gate threshold
    quality_score_warn: 80   # Quality % warning
    quality_score_block: 50  # Quality % block
  custom_checks:
    - name: "positive_amounts"
      sql: "SELECT COUNT(*) FROM df WHERE amount < 0"
      expect: "result == 0"
```

## Git Conventions

- Feature branches off main
- Commit messages: imperative mood, describe the "why"
- Don't commit `.env`, `data/dre_system.duckdb`, `node_modules/`, `.venv/`
