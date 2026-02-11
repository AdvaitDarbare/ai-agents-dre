"""
Data Observability Dashboard - The Control Center

This Streamlit application serves as the visual interface for the Agentic Data Reliability Engineering platform.
It provides real-time insights into pipeline health, anomaly detection, and agent reasoning.

Key Features:
1. Pipeline Status & Trust Score
2. Visual Anomaly Detection (Advanced observability charts)
3. Live Agent Execution & Reasoning
4. Data Loading Controls
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import numpy as np
import time
import json
import os
from datetime import datetime, timedelta

# Import Backend Components
from src.agents.monitor_agent import MonitorAgent
from src.tools.anomaly_detector import AnomalyDetector
from src.dashboard.styles import get_main_styles

# ---------------------------------------------------------
# Configuration & Setup
# ---------------------------------------------------------
st.set_page_config(
    page_title="Agentic DRE Control Center",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Inject Global Styles (from styles.py)
st.markdown(get_main_styles(), unsafe_allow_html=True)

# Initialize Agent (Cached with TTL for live updates)
@st.cache_resource(ttl=300)
def get_agent():
    return MonitorAgent(contracts_path="config/expectations", lineage_path="config/lineage.yaml")

agent = get_agent()

# ---------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------

def calculate_trust_score(dataset_name, db_path):
    """
    Calculate Dynamic Trust Score based on real signals:
    1. Historical anomaly rate (from DuckDB metric_history)
    2. Current run verdict (PASSED / WARNING / BLOCKED)
    3. Current data quality score (from DataProfiler)
    
    Formula: Trust = (history_component * 0.4) + (verdict_component * 0.35) + (quality_component * 0.25)
    """
    import duckdb
    
    # --- Component 1: Historical Pass Rate (40% weight) ---
    history_score = 100.0
    try:
        conn = duckdb.connect(db_path, read_only=True)
        # Count total distinct runs
        total_runs = conn.execute(
            f"SELECT count(DISTINCT run_id) FROM metric_history WHERE dataset_name = '{dataset_name}'"
        ).fetchone()[0]
        
        if total_runs > 0:
            # Count runs that had anomalies (z_score entries that were flagged)
            # Anomalies are stored with metric_name containing 'row_count', 'mean_', 'null_rate_'
            # A run with a metric_value that deviates significantly suggests a problem
            # Use a simpler heuristic: count runs vs anomaly-flagged runs
            anomaly_runs = conn.execute(f"""
                SELECT count(DISTINCT run_id) FROM metric_history 
                WHERE dataset_name = '{dataset_name}'
                AND metric_name = 'row_count'
                AND metric_value < (
                    SELECT AVG(metric_value) - 2 * STDDEV(metric_value) 
                    FROM metric_history 
                    WHERE dataset_name = '{dataset_name}' AND metric_name = 'row_count'
                )
            """).fetchone()[0]
            
            history_score = max(0, ((total_runs - anomaly_runs) / total_runs) * 100)
        
        conn.close()
    except Exception:
        history_score = 100.0  # No history = assume clean
    
    # --- Component 2: Current Verdict (35% weight) ---
    last_result = st.session_state.get("last_result", {})
    current_status = last_result.get("status", "UNKNOWN")
    
    verdict_score_map = {
        "PASSED": 100.0,
        "WARNING": 60.0,
        "BLOCKED": 10.0,
        "UNKNOWN": 50.0,  # No run yet
    }
    verdict_score = verdict_score_map.get(current_status, 50.0)
    
    # --- Component 3: Data Quality Score (25% weight) ---
    profile_data = last_result.get("profile", {})
    quality_score = profile_data.get("overall_quality_score", 100.0)  # Default 100 if no run
    
    # --- Weighted Composite ---
    trust = (history_score * 0.40) + (verdict_score * 0.35) + (quality_score * 0.25)
    return round(min(100.0, max(0.0, trust)), 1)

def create_mock_history(days=30):
    """Generate extensive mock history for the chart if DB is empty."""
    dates = [datetime.now() - timedelta(days=i) for i in range(days)]
    dates.reverse()
    
    # Base pattern + Seasonality + Noise
    base = 1000
    values = []
    means = []
    upper_bounds = []
    lower_bounds = []
    
    for d in dates:
        # Weekly seasonality (drop on weekends)
        seasonality = 0.8 if d.weekday() >= 5 else 1.0
        
        # Random noise
        noise = np.random.normal(0, 50)
        
        val = (base * seasonality) + noise
        mean = base * seasonality
        std = 50
        
        values.append(val)
        means.append(mean)
        upper_bounds.append(mean + (3 * std))
        lower_bounds.append(mean - (3 * std))
        
    return pd.DataFrame({
        "timestamp": dates,
        "value": values,
        "mean": means,
        "upper": upper_bounds,
        "lower": lower_bounds
    })

def render_anomaly_chart(history_df, current_val=None):
    """Create the Monte Carlo style anomaly chart."""
    fig = go.Figure()

    # 1. The "Blue Zone" (Expected Range)
    # We use fill='tonexty' to shade between upper and lower
    fig.add_trace(go.Scatter(
        x=history_df['timestamp'], 
        y=history_df['upper'],
        mode='lines',
        line=dict(width=0),
        showlegend=False,
        hoverinfo='skip'
    ))
    
    fig.add_trace(go.Scatter(
        x=history_df['timestamp'], 
        y=history_df['lower'],
        mode='lines',
        fill='tonexty',
        fillcolor='rgba(0, 173, 181, 0.2)', # Cyan/Teal transparent
        line=dict(width=0),
        name='Expected Range (3σ)'
    ))

    # 2. The Actual Data (Purple Line)
    fig.add_trace(go.Scatter(
        x=history_df['timestamp'], 
        y=history_df['value'],
        mode='lines+markers',
        line=dict(color='#9D00FF', width=2),
        marker=dict(size=6),
        name='Actual Volume'
    ))
    
    # 3. Current Run (if available)
    if current_val is not None:
        fig.add_trace(go.Scatter(
            x=[datetime.now()],
            y=[current_val],
            mode='markers',
            marker=dict(color='red', size=12, symbol='star'),
            name='Current Run'
        ))

    fig.update_layout(
        title="Volume Anomaly Detection (Row Count)",
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        xaxis_title="Time",
        yaxis_title="Row Count",
        height=400,
        margin=dict(l=20, r=20, t=40, b=20),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    
    return fig

# ---------------------------------------------------------
# Sidebar Navigation
# ---------------------------------------------------------
with st.sidebar:
    st.header("🎛️ Control Panel")
    
    # Auto-discover datasets from contracts directory
    discovered = agent.discover_datasets()
    dataset_names = [ds["name"] for ds in discovered]
    st.session_state["discovered_datasets"] = discovered
    
    def on_change_ds():
        for k in ["monitor_status", "last_result", "current_row_count", "last_run_time"]:
            st.session_state.pop(k, None)

    selected_dataset = st.selectbox(
        "Select Dataset", 
        dataset_names if dataset_names else ["transactions"],
        key="dataset_selector",
        on_change=on_change_ds
    )
    
    # Show selected dataset metadata
    selected_meta = next((ds for ds in discovered if ds["name"] == selected_dataset), None)
    if selected_meta:
        st.caption(f"📊 {selected_meta['column_count']} columns · {selected_meta['criticality']} criticality")
        st.caption(f"👤 {selected_meta['owner']} · v{selected_meta['version']}")
    
    st.divider()
    
    st.header("🤖 Agent Copilot")
    
    # Chat Interface mock
    messages = st.session_state.get("chat_messages", [
        {"role": "assistant", "content": "Hello! I am monitoring the pipeline. Ask me anything about the recent runs."}
    ])
    
    for msg in messages:
        with st.chat_message(msg["role"]):
            st.write(msg["content"])
            
    if prompt := st.chat_input("Ask the agent..."):
        # Add user message
        messages.append({"role": "user", "content": prompt})
        # Add mock agent response
        response = "I'm analyzing the logs... It seems like the last anomaly was caused by a holiday drop in traffic." 
        if "fail" in prompt.lower():
            response = "The failure was triggered by a Schema Type Mismatch in the 'timestamp' column."
        messages.append({"role": "assistant", "content": response})
        st.session_state["chat_messages"] = messages
        st.rerun()

# ---------------------------------------------------------
# Tabs Layout
# ---------------------------------------------------------
tab_health, tab_overview, tab_deep_dive, tab_lineage, tab_history, tab_schema_log = st.tabs(["🏥 Schema Health", "🔍 Overview", "📉 Deep Dive", "🕸️ Lineage", "📜 History", "📋 Schema Changelog"])

# ---------------------------------------------------------
# Tab 0: Schema Health (Multi-Dataset Overview)
# ---------------------------------------------------------
with tab_health:
    st.subheader("🏥 Schema Health Overview")
    st.markdown("Monitor **all** dataset contracts at a glance — Enterprise-grade schema-level monitoring.")
    
    discovered = st.session_state.get("discovered_datasets", [])
    all_results = st.session_state.get("all_results", {})
    
    # Run All button
    scan_col1, scan_col2 = st.columns([3, 1])
    with scan_col1:
        run_all_clicked = st.button("🚀 Run All Health Checks", type="primary", key="run_all_btn")
    with scan_col2:
        smart_scan = st.toggle("⚡ Smart Scan", value=True, 
                               help="Skip datasets whose data hasn't changed since last scan")
    
    if run_all_clicked:
        with st.status("Running schema-wide health checks...", expanded=True) as all_status:
            st.write(f"📂 Found {len(discovered)} dataset contract(s)")
            if smart_scan:
                st.write("⚡ Smart Scan ON — skipping unchanged datasets")
            
            batch_result = agent.evaluate_all(skip_unchanged=smart_scan)
            st.session_state["all_results"] = batch_result.get("results", {})
            st.session_state["all_summary"] = batch_result.get("summary", {})
            st.session_state["last_run_time"] = datetime.now()
            
            summary = batch_result["summary"]
            if summary["blocked"] > 0:
                all_status.update(label=f"❌ {summary['blocked']} dataset(s) BLOCKED", state="error")
            elif summary["warning"] > 0:
                all_status.update(label=f"⚠️ {summary['warning']} warning(s)", state="complete")
            else:
                all_status.update(label=f"✅ All {summary['passed']} dataset(s) healthy!", state="complete")
            st.rerun()
    
    # Summary KPIs
    all_summary = st.session_state.get("all_summary", {})
    if all_summary:
        kpi1, kpi2, kpi3, kpi4 = st.columns(4)
        with kpi1:
            st.metric("Total Datasets", all_summary.get("total", 0))
        with kpi2:
            st.metric("✅ Passed", all_summary.get("passed", 0))
        with kpi3:
            st.metric("⚠️ Warnings", all_summary.get("warning", 0))
        with kpi4:
            st.metric("🚫 Blocked", all_summary.get("blocked", 0))
        st.divider()
    
    # Dataset Pulse Table
    st.markdown("### 💓 Dataset Pulse")
    
    # Table Header
    cols = st.columns([3, 2, 2, 1.5, 3])
    cols[0].markdown('<span class="pulse-header-text">DATASET</span>', unsafe_allow_html=True)
    cols[1].markdown('<span class="pulse-header-text">STATUS</span>', unsafe_allow_html=True)
    cols[2].markdown('<span class="pulse-header-text">CRITICALITY</span>', unsafe_allow_html=True)
    cols[3].markdown('<span class="pulse-header-text">OWNER</span>', unsafe_allow_html=True)
    cols[4].markdown('<span class="pulse-header-text">QUALITY TREND (7 RUNS)</span>', unsafe_allow_html=True)
    st.divider()
    
    for ds in discovered:
        name = ds["name"]
        result = all_results.get(name, {})
        status = result.get("status", "NOT RUN")
        reason = result.get("reason", "")
        
        # Metadata
        criticality = ds.get("criticality", "UNKNOWN")
        owner = ds.get("owner", "Unknown")
        
        # Badge Logic
        status_badges = {
            "PASSED": "badge-passed",
            "WARNING": "badge-warning",
            "BLOCKED": "badge-blocked",
            "SKIPPED": "badge-warning",
            "NOT RUN": "badge-warning"
        }
        badge_class = status_badges.get(status, "badge-warning")
        
        crit_badges = {
            "HIGH": "badge-criticality-high",
            "CRITICAL": "badge-criticality-high",
            "MEDIUM": "badge-criticality-med", 
            "LOW": "badge-passed"
        }
        crit_class = crit_badges.get(criticality, "badge-passed")
        
        icon = "✅" if status == "PASSED" else "🚫" if status == "BLOCKED" else "⚠️" if status == "WARNING" else "⏳"

        # Render Row
        with st.container():
            c = st.columns([3, 2, 2, 1.5, 3])
            
            # Name & Reason
            c[0].markdown(f"**{name}**")
            if status not in ["PASSED", "NOT RUN"]:
                c[0].caption(f"{reason[:60]}..." if len(reason) > 60 else reason)
            
            # Status Badge
            c[1].markdown(f'<span class="badge {badge_class}">{icon} {status}</span>', unsafe_allow_html=True)
            
            # Criticality Badge
            c[2].markdown(f'<span class="badge {crit_class}">{criticality}</span>', unsafe_allow_html=True)
            
            # Owner
            c[3].caption(owner)
            
            # Sparkline (Quality Score History)
            try:
                # Fetch history for sparkline
                history = agent.get_run_history(dataset_name=name, limit=10)
                if history:
                    scores = [h["quality_score"] for h in reversed(history)]
                    if len(scores) > 1:
                        c[4].line_chart(scores, height=30)
                    else:
                        c[4].caption(f"Score: {scores[0]:.0f}%")
                else:
                     c[4].caption("No History")
            except Exception:
                c[4].caption("—")
                
            st.markdown('<div style="border-bottom: 1px solid #f0f0f0; margin-bottom: 8px;"></div>', unsafe_allow_html=True)

# ---------------------------------------------------------
# Tab 1: Overview
# ---------------------------------------------------------
with tab_overview:
    # Hero Section (KPIs)
    col1, col2, col3, col4 = st.columns(4)
    
    monitor_status = st.session_state.get("monitor_status", "OPERATIONAL")
    status_color = "green" if monitor_status == "OPERATIONAL" else "red" if monitor_status == "CRITICAL" else "orange"

    with col1:
        st.markdown(f"### Pipeline Status")
        st.markdown(f"<h2 style='color: {status_color};'>● {monitor_status}</h2>", unsafe_allow_html=True)

    with col2:
        # Dynamic Trust Score
        trust_score = calculate_trust_score(selected_dataset, agent.anomaly_detector.db_path)
        st.metric(label="Trust Score", value=f"{trust_score:.1f}%")

    with col3:
        last_run = st.session_state.get("last_run_time", "Never")
        if last_run != "Never":
            time_diff = (datetime.now() - last_run).seconds // 60
            time_str = f"{time_diff} mins ago"
        else:
            time_str = "N/A"
        st.metric(label="Freshness", value=time_str)

    with col4:
        # Volume metric (Mock or calculated)
        current_rows = st.session_state.get("current_row_count", 0)
        st.metric(label="Current Volume", value=f"{current_rows} rows")

    st.divider()

    # Layout: Stacked
    # col_main, col_details = st.columns([2, 1])

    with st.container():
        st.subheader("🚀 Manual Execution")
        
        # Use auto-discovered data file path for the selected dataset
        _sel_meta = next((ds for ds in st.session_state.get('discovered_datasets', []) if ds['name'] == selected_dataset), None)
        mock_file_path = _sel_meta['data_file'] if _sel_meta and _sel_meta.get('data_file') else f"data/test/{selected_dataset}.csv"
        
        if st.button("Run Health Check Now", type="primary"):
            with st.status("Running Agentic Pipeline...", expanded=True) as status:
                st.toast("🚀 Agent started...")
                st.write("🔍 [Stage 1] Validating Schema Integrity...")
                time.sleep(1) 
                
                st.toast("📉 Analyzing Statistics...")
                st.write("📉 [Stage 2] Calculating Z-Scores & Drift...")
                time.sleep(0.5)
                
                st.write("🎯 [Stage 3] Assessing Business Impact...")
                time.sleep(0.5)
                
                st.write("🤖 [Stage 4] Generating Agent Verdict...")
                
                if not os.path.exists(mock_file_path):
                    st.error("Mock data file missing. Run src/main.py first to generate it.")
                    status.update(label="Failed", state="error")
                else:
                    try:
                        result = agent.evaluate_data_file(mock_file_path, selected_dataset)
                        st.session_state["last_result"] = result
                        st.session_state["last_run_time"] = datetime.now()
                        
                        # Update volume state
                        df_temp = pd.read_csv(mock_file_path)
                        st.session_state["current_row_count"] = len(df_temp)
                        
                        if result["status"] == "PASSED":
                            st.session_state["monitor_status"] = "OPERATIONAL"
                            status.update(label="✅ Pipeline Success!", state="complete")
                            st.toast("✅ Data Loaded Successfully!")
                            if trust_score == 100.0:
                                st.balloons()
                        elif result["status"] == "WARNING":
                            st.session_state["monitor_status"] = "WARNING"
                            status.update(label="⚠️ Completed with Warnings", state="complete")
                            st.toast("⚠️ Loaded with Warnings.")
                        else:
                            st.session_state["monitor_status"] = "CRITICAL"
                            status.update(label="❌ Pipeline Blocked", state="error")
                            st.toast("❌ Load Blocked by Agent.")
                            
                    except Exception as e:
                        st.error(f"Agent Execution Failed: {str(e)}")
                        status.update(label="System Error", state="error")

    st.divider()
    with st.container():
        st.subheader("📋 Verdict & Reasoning")
        
        if "last_result" in st.session_state:
            res = st.session_state["last_result"]
            
            # Color-coded Verdict Box
            v_color = "#28a745" if res["status"] == "PASSED" else "#ffc107" if res["status"] == "WARNING" else "#dc3545"
            st.markdown(f"""
            <div style="background-color: {v_color}; padding: 15px; border-radius: 8px; color: white; margin-bottom: 20px;">
                <h3 style="margin:0;">{res['status']}</h3>
                <p style="margin:5px 0 0 0;">{res['reason']}</p>
            </div>
            """, unsafe_allow_html=True)
            
            # LLM Advice
            st.markdown("**🤖 Agent Advice:**")
            st.info(res.get("llm_advice", "No advice generated."))
            
            # Anomalies List
            if res.get("anomalies"):
                st.markdown("**🚨 Detected Anomalies:**")
                for a in res["anomalies"]:
                    st.error(f"{a['metric']}: {a['details']}")
            
            # Schema Changes
            schema_ev = res.get("schema_evolution", {})
            if schema_ev.get("new_columns"):
                st.markdown("**Example: New Columns Detected:**")
                st.warning(f"{', '.join(schema_ev['new_columns'])}")
            # Wrapper for schema remediation logic
            if res["status"] == "BLOCKED" and ("Schema Violation" in res.get("reason", "") or res.get("schema_evolution", {}).get("type_mismatches")):
                st.markdown("---")
                st.subheader("🛠️ Remediation Proposal")
                
                # Use cached function or simple read
                current_schema = agent.get_schema_content(selected_dataset)
                
                # Check if we have a proposal for this specific run
                proposal_key = f"proposal_{selected_dataset}_{str(res.get('timestamp', datetime.now()))}"
                
                if proposal_key not in st.session_state:
                    with st.spinner("🤖 Agent is designing a fix..."):
                        error_details = json.dumps(res.get("schema_evolution", {}), indent=2)
                        proposal = agent.remediator.propose_schema_update(current_schema, error_details)
                        st.session_state[proposal_key] = proposal
                
                proposed_yaml = st.session_state[proposal_key]
                
                col_orig, col_new = st.columns(2)
                with col_orig:
                    st.caption("Current Schema")
                    st.code(current_schema, language="yaml")
                with col_new:
                    st.caption("Proposed Fix")
                    st.code(proposed_yaml, language="yaml")
                
                if st.button("Apply Recommended Schema Fix", type="primary"):
                    if agent.remediate_schema(selected_dataset, proposed_yaml):
                        st.success("✅ Schema Updated! Re-running pipeline...")
                        time.sleep(1)
                        # Re-run the check automatically
                        try:
                            # Re-fetch mock path (simplified)
                            mock_file_path = "data/test/transactions.csv"
                            new_result = agent.evaluate_data_file(mock_file_path, selected_dataset)
                            st.session_state["last_result"] = new_result
                            st.session_state["last_run_time"] = datetime.now()
                            st.rerun()
                        except Exception as e:
                            st.error(f"Re-run failed: {e}")
                    else:
                        st.error("Failed to update schema.")
                        
        else:
            st.info("Run a check to see the verdict.")

# ---------------------------------------------------------
# Tab 2: Deep Dive
# ---------------------------------------------------------
with tab_deep_dive:
    st.subheader("📊 Data Health Monitor")
    
    # --- Volume Chart ---
    history_df = create_mock_history()
    current_val = st.session_state.get("current_row_count", None)
    st.plotly_chart(render_anomaly_chart(history_df, current_val), use_container_width=True)
    
    st.divider()
    
    # --- Column Quality Scores (from DataProfiler) ---
    st.subheader("🔬 Column-Level Data Quality")
    
    last_res_dd = st.session_state.get("last_result", {})
    profile_data = last_res_dd.get("profile", {})
    col_scores = profile_data.get("column_scores", {})
    
    if col_scores:
        # Bar chart of per-column quality
        import plotly.express as px
        scores_df = pd.DataFrame({
            "Column": list(col_scores.keys()),
            "Quality Score (%)": list(col_scores.values())
        })
        
        fig_cols = px.bar(
            scores_df, x="Column", y="Quality Score (%)",
            color="Quality Score (%)",
            color_continuous_scale=["#dc3545", "#ffc107", "#28a745"],
            range_color=[0, 100],
            title="Per-Column Quality Score"
        )
        fig_cols.update_layout(
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            height=350,
            margin=dict(l=20, r=20, t=40, b=20)
        )
        st.plotly_chart(fig_cols, use_container_width=True)
        
        # Constraint Violations
        violations = profile_data.get("constraint_violations", [])
        custom_results = profile_data.get("custom_check_results", [])
        
        if violations:
            st.markdown("**⚠️ Constraint Violations:**")
            for v in violations:
                st.error(f"{v['type']}: {v['message']}")
        
        if custom_results:
            st.markdown("**🧪 Custom SQL Check Results:**")
            for cr in custom_results:
                icon = "✅" if cr.get("passed") else "❌"
                st.write(f"{icon} **{cr['name']}** — {cr.get('violation_count', 0)} violations ({cr.get('violation_rate', 0):.1%} of rows)")
        
        # Overall Quality Score
        overall = profile_data.get("overall_quality_score", 0)
        st.metric("Overall Data Quality Score", f"{overall:.1f}%")
    else:
        st.info("🔬 Run a Health Check to see column-level quality analysis.")
    
    st.divider()
    
    # --- Freshness Timeline ---
    st.subheader("⏱️ Data Freshness Timeline")
    
    last_run_ts = st.session_state.get("last_run_time", None)
    if last_run_ts:
        import plotly.express as px
        
        # Simulate freshness history (minutes since last update)
        now = datetime.now()
        freshness_data = []
        for i in range(24, 0, -1):
            ts = now - timedelta(hours=i)
            # Simulate freshness: data usually arrives within 15 mins, occasional delays
            freshness_mins = np.random.choice([5, 8, 10, 12, 15, 45, 90], p=[0.2, 0.25, 0.2, 0.15, 0.1, 0.07, 0.03])
            freshness_data.append({"Time": ts, "Freshness (mins)": freshness_mins})
        
        # Add the current actual freshness
        actual_freshness = (now - last_run_ts).total_seconds() / 60
        freshness_data.append({"Time": now, "Freshness (mins)": round(actual_freshness, 1)})
        
        freshness_df = pd.DataFrame(freshness_data)
        
        fig_fresh = go.Figure()
        fig_fresh.add_trace(go.Scatter(
            x=freshness_df["Time"],
            y=freshness_df["Freshness (mins)"],
            mode="lines+markers",
            name="Freshness",
            line=dict(color="#36b5d8", width=2),
            marker=dict(size=5)
        ))
        # SLA Line (30 minute threshold)
        fig_fresh.add_hline(y=30, line_dash="dash", line_color="#dc3545",
                           annotation_text="30-min SLA",
                           annotation_position="top right")
        fig_fresh.update_layout(
            title="Data Freshness (Minutes Since Last Update)",
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            height=300,
            margin=dict(l=20, r=20, t=40, b=20),
            yaxis_title="Minutes"
        )
        st.plotly_chart(fig_fresh, use_container_width=True)
    else:
        st.info("⏱️ Run a Health Check to see freshness data.")
    
    st.divider()
    
    # --- Null Rate Heatmap ---
    st.subheader("🗺️ Null Rate by Column")
    
    if col_scores:
        import plotly.express as px
        
        # Build null rate data from the profiler
        null_data = {}
        last_res_profile = st.session_state.get("last_result", {}).get("profile", {})
        
        # We don't have historical null data yet, so simulate 10 recent runs
        columns_list = list(col_scores.keys())
        run_labels = [f"Run {i}" for i in range(1, 11)]
        
        heatmap_data = []
        for run_label in run_labels[:-1]:  # Historical (simulated)
            for col in columns_list:
                null_rate = np.random.uniform(0, 0.05)  # Simulated low null rates
                heatmap_data.append({
                    "Run": run_label,
                    "Column": col,
                    "Null Rate (%)": round(null_rate * 100, 2)
                })
        
        # Current run (real data from profiler)
        real_null_rates = profile_data.get("null_rates", {})
        for col in columns_list:
            null_rate = real_null_rates.get(col, 0.0)
            heatmap_data.append({
                "Run": "Current",
                "Column": col,
                "Null Rate (%)": round(null_rate * 100, 2)
            })
        
        heatmap_df = pd.DataFrame(heatmap_data)
        pivot = heatmap_df.pivot(index="Column", columns="Run", values="Null Rate (%)")
        
        # Reorder columns so "Current" is last
        ordered_cols = [c for c in pivot.columns if c != "Current"] + ["Current"]
        pivot = pivot[ordered_cols]
        
        fig_null = px.imshow(
            pivot.values,
            labels=dict(x="Run", y="Column", color="Null Rate (%)"),
            x=list(pivot.columns),
            y=list(pivot.index),
            color_continuous_scale=["#28a745", "#ffc107", "#dc3545"],
            aspect="auto",
            title="Null Rate Heatmap (% Null per Column per Run)"
        )
        fig_null.update_layout(
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            height=350,
            margin=dict(l=20, r=20, t=40, b=20)
        )
        st.plotly_chart(fig_null, use_container_width=True)
    else:
        st.info("🗺️ Run a Health Check to see null rate analysis.")
    
    st.divider()
    
    # --- Distribution Drift Chart ---
    st.subheader("📈 Distribution Drift (Mean Amount Over Time)")
    
    import duckdb as _duckdb_dd
    try:
        _conn_dd = _duckdb_dd.connect(agent.anomaly_detector.db_path, read_only=True)
        drift_df = _conn_dd.execute(f"""
            SELECT timestamp, metric_value 
            FROM metric_history 
            WHERE dataset_name = '{selected_dataset}' 
              AND metric_name = 'mean_amount'
            ORDER BY timestamp ASC
            LIMIT 100
        """).fetchdf()
        _conn_dd.close()
        
        if len(drift_df) > 0:
            # Ensure timestamps are proper datetime objects
            drift_df["timestamp"] = pd.to_datetime(drift_df["timestamp"])
            
            # If all points are within 1 minute (seeded baseline), spread them 
            # across the last N days so the chart is immediately useful
            t_min = drift_df["timestamp"].min()
            t_max = drift_df["timestamp"].max()
            time_span = (t_max - t_min).total_seconds()
            
            if time_span < 60 and len(drift_df) > 1:
                now = datetime.now()
                n = len(drift_df)
                drift_df["timestamp"] = [now - timedelta(days=n - i) for i in range(n)]
                is_spread = True
            else:
                is_spread = False
            
            fig_drift = go.Figure()
            fig_drift.add_trace(go.Scatter(
                x=drift_df["timestamp"],
                y=drift_df["metric_value"],
                mode="lines+markers",
                name="Baseline" if is_spread else "Mean Amount",
                line=dict(color="#8b5cf6", width=2),
                marker=dict(size=6)
            ))
            
            # Add mean ± std band
            avg_val = drift_df["metric_value"].mean()
            std_val = drift_df["metric_value"].std()
            fig_drift.add_hline(y=avg_val, line_dash="dash", line_color="#6b7280",
                               annotation_text=f"Baseline: ${avg_val:.2f}",
                               annotation_position="top right")
            
            # Add 2σ warning band
            if std_val > 0:
                fig_drift.add_hrect(
                    y0=avg_val - 2 * std_val, y1=avg_val + 2 * std_val,
                    fillcolor="rgba(139, 92, 246, 0.08)",
                    line_width=0,
                    annotation_text="±2σ",
                    annotation_position="top left"
                )
            
            # Overlay current run value if available
            current_profile = st.session_state.get("last_result", {}).get("profile", {})
            # We don't store mean_amount in profile directly, but we can get it from anomaly data
            
            title = "Mean Amount Trend (Distribution Drift Monitor)"
            if is_spread:
                title += "  ·  Baseline data spread for visibility"
            
            fig_drift.update_layout(
                title=title,
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                height=320,
                margin=dict(l=20, r=20, t=40, b=20),
                yaxis_title="Mean Amount ($)",
                xaxis_title="Time"
            )
            st.plotly_chart(fig_drift, use_container_width=True)
        else:
            st.info("📈 Not enough historical data yet. Run a health check to start building a drift baseline.")
    except Exception as e:
        st.info(f"📈 Distribution drift data not available yet. ({e})")

# ---------------------------------------------------------
# Tab 3: Lineage
# ---------------------------------------------------------
with tab_lineage:
    st.subheader("🕸️ Dynamic Data Lineage")
    
    # 1. Get Real Downstream Impact
    impact = agent.impact_analyzer.get_downstream_impact(selected_dataset)
    consumers = impact.get("impacted_consumers", [])
    
    # 2. Determine Pipeline State
    last_res = st.session_state.get("last_result", {})
    status = last_res.get("status", "UNKNOWN")
    
    # Colors
    color_pass = "#28a745" # Green
    color_block = "#dc3545" # Red
    color_gray = "#cccccc"
    
    # Edge Styles
    # Default (No Run Yet)
    edge_doris_color = color_gray
    edge_doris_style = "dashed"
    edge_quarantine_color = color_gray
    edge_quarantine_style = "dashed"
    
    if status in ["PASSED", "WARNING"]:
        edge_doris_color = color_pass
        edge_doris_style = "solid"
        edge_quarantine_color = color_gray
        edge_quarantine_style = "dashed"
    elif status == "BLOCKED":
        edge_doris_color = color_gray
        edge_doris_style = "dashed"
        edge_quarantine_color = color_block
        edge_quarantine_style = "solid"
        
    # 3. Construct Graphviz DOT
    dot = f"""
    digraph Lineage {{
      rankdir="LR";
      bgcolor="transparent";
      node [style="filled", shape="box", fontname="Arial", fontsize=10];
      edge [fontname="Arial", fontsize=8];

      # Nodes
      Source [label="{selected_dataset}.csv", fillcolor="#eee"];
      Agent [label="Monitor Agent", fillcolor="#ff99ff"];
      Decision [label="Checks Pass?", shape="diamond", fillcolor="#ffffcc"];
      Doris [label="dw.{selected_dataset}", shape="cylinder", fillcolor="#99ccff"];
      Quarantine [label="Quarantine", fillcolor="#ffcccc"];
      
      # Core Pipeline Edges
      Source -> Agent [label="Ingest"];
      Agent -> Decision [label="Validate"];
      
      # Conditional Edges
      Decision -> Doris [label="Yes", color="{edge_doris_color}", style="{edge_doris_style}", penwidth=2];
      Decision -> Quarantine [label="No", color="{edge_quarantine_color}", style="{edge_quarantine_style}", penwidth=2];
    """
    
    # Add Downstream Consumers
    for i, user in enumerate(consumers):
        safe_name = f"Consumer_{i}"
        label = user.get("name", "Unknown")
        c_type = user.get("type", "consumer")
        
        # Color based on criticality
        fill = "#ccffcc" if user.get("criticality") == "LOW" else "#ffcc99" if user.get("criticality") == "MEDIUM" else "#ff9999"
        
        dot += f'  {safe_name} [label="{label}\\n({c_type})", fillcolor="{fill}"];\n'
        # Edge from Doris to Consumer (only active if passes)
        consumer_edge_color = color_pass if status in ["PASSED", "WARNING"] else color_gray
        consumer_edge_style = "solid" if status in ["PASSED", "WARNING"] else "dashed"
        
        dot += f'  Doris -> {safe_name} [color="{consumer_edge_color}", style="{consumer_edge_style}"];\n'

    dot += "}"
    
    st.graphviz_chart(dot)
    
    if status == "BLOCKED":
        st.error(f"🚫 IMPACT PREVENTED: Bad data was blocked from reaching {len(consumers)} downstream consumers.")
    elif status == "UNKNOWN":
        st.info("Run a health check to see the active data flow.")
    else:
        st.success(f"✅ DATA FLOWING: Clean data is populating {len(consumers)} downstream systems.")

# ---------------------------------------------------------
# Tab 4: History
# ---------------------------------------------------------
with tab_history:
    st.subheader("📜 Run History")
    
    # Show structured run history from system tables (Phase 3)
    import duckdb as _duckdb_hist
    try:
        _hist_conn = _duckdb_hist.connect(agent.anomaly_detector.db_path)
        
        # Primary: Show run_history (structured outcomes)
        run_hist_df = _hist_conn.execute("""
            SELECT 
                timestamp as "Timestamp",
                dataset_name as "Dataset",
                status as "Status", 
                ROUND(quality_score, 1) as "Quality %",
                anomaly_count as "Anomalies",
                ROUND(z_score_max, 2) as "Max Z-Score",
                duration_ms as "Duration (ms)",
                reason as "Reason"
            FROM run_history 
            WHERE dataset_name = ? 
            ORDER BY timestamp DESC 
            LIMIT 50
        """, (selected_dataset,)).fetchdf()
        
        if not run_hist_df.empty:
            st.markdown(f"**{len(run_hist_df)}** recorded run(s) for `{selected_dataset}`")
            
            # Color-code status column
            def _style_status(val):
                colors = {"PASSED": "#28a745", "WARNING": "#ff9800", "BLOCKED": "#dc3545"}
                return f"color: {colors.get(val, '#666')}"
            
            styled = run_hist_df.style.applymap(_style_status, subset=["Status"])
            st.dataframe(styled, use_container_width=True, hide_index=True)
            
            # --- Timeline Chart ---
            st.markdown("### ⏱️ Run Duration & Status Trend")
            try:
                import plotly.express as px
                
                # Ensure datetime
                run_hist_df["Timestamp"] = pd.to_datetime(run_hist_df["Timestamp"])
                
                fig = px.scatter(
                    run_hist_df,
                    x="Timestamp",
                    y="Duration (ms)",
                    color="Status",
                    size="Quality %", # Size by quality score logic? Or anomalies?
                    color_discrete_map={
                        "PASSED": "#10b981", 
                        "WARNING": "#f59e0b", 
                        "BLOCKED": "#ef4444"
                    },
                    hover_data=["Reason", "Anomalies"],
                    title="Performance & Stability Over Time",
                    template="plotly_white"
                )
                fig.update_layout(
                    hovermode="x unified",
                    plot_bgcolor="white",
                    paper_bgcolor="white",
                    font_family="Inter",
                )
                fig.update_xaxes(showgrid=True, gridcolor="#f0f0f0")
                fig.update_yaxes(showgrid=True, gridcolor="#f0f0f0")
                
                st.plotly_chart(fig, use_container_width=True)
            except Exception as e:
                st.error(f"Could not render timeline: {e}")

        else:
            # Fallback: Show old metric_history
            st.info("No structured run history yet. Showing raw metric history.")
            history_data = _hist_conn.execute(
                "SELECT timestamp, metric_name, metric_value FROM metric_history "
                f"WHERE dataset_name = '{selected_dataset}' ORDER BY timestamp DESC LIMIT 50"
            ).fetchdf()
            st.dataframe(history_data, use_container_width=True)
        
        # Summary stats
        summary_df = _hist_conn.execute("""
            SELECT 
                COUNT(*) as total_runs,
                SUM(CASE WHEN status = 'PASSED' THEN 1 ELSE 0 END) as passed,
                SUM(CASE WHEN status = 'WARNING' THEN 1 ELSE 0 END) as warnings,
                SUM(CASE WHEN status = 'BLOCKED' THEN 1 ELSE 0 END) as blocked,
                ROUND(AVG(quality_score), 1) as avg_quality,
                ROUND(AVG(duration_ms), 0) as avg_duration_ms
            FROM run_history
            WHERE dataset_name = ?
        """, (selected_dataset,)).fetchdf()
        
        if not summary_df.empty and summary_df.iloc[0]["total_runs"] > 0:
            row = summary_df.iloc[0]
            st.divider()
            st.markdown("**📊 Run Statistics**")
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Total Runs", int(row["total_runs"]))
            c2.metric("Pass Rate", f"{row['passed']/row['total_runs']*100:.0f}%")
            c3.metric("Avg Quality", f"{row['avg_quality']}%")
            c4.metric("Avg Duration", f"{row['avg_duration_ms']:.0f}ms")
        
        _hist_conn.close()
    except Exception as e:
        st.warning(f"Could not load history: {e}")

# ---------------------------------------------------------
# Tab 5: Schema Changelog
# ---------------------------------------------------------
with tab_schema_log:
    st.subheader("📋 Schema Changelog")
    st.markdown("View schema remediation history and compare versions.")
    
    from pathlib import Path
    import difflib
    
    schema_dir = Path("config/expectations")
    current_file = schema_dir / f"{selected_dataset}.yaml"
    
    # Find all backup files for this dataset
    backup_files = sorted(
        schema_dir.glob(f"{selected_dataset}.backup_*"),
        key=lambda p: p.stat().st_mtime,
        reverse=True
    )
    
    if backup_files:
        st.success(f"📦 Found {len(backup_files)} backup version(s) for **{selected_dataset}**")
        
        # Show timeline
        for i, bf in enumerate(backup_files):
            ts_str = bf.stem.split("backup_")[-1]  # e.g., 20260211_073000
            try:
                from datetime import datetime as dt_parse
                ts = dt_parse.strptime(ts_str, "%Y%m%d_%H%M%S")
                display_ts = ts.strftime("%b %d, %Y at %I:%M:%S %p")
            except Exception:
                display_ts = ts_str
            
            with st.expander(f"🕐 Version {len(backup_files) - i}: {display_ts}", expanded=(i == 0)):
                # Read both files
                try:
                    current_content = current_file.read_text()
                    backup_content = bf.read_text()
                    
                    if current_content == backup_content:
                        st.info("No differences from current schema.")
                    else:
                        # Generate unified diff
                        diff = list(difflib.unified_diff(
                            backup_content.splitlines(keepends=True),
                            current_content.splitlines(keepends=True),
                            fromfile=f"backup ({display_ts})",
                            tofile="current",
                            lineterm=""
                        ))
                        
                        if diff:
                            diff_text = "\n".join(diff)
                            st.code(diff_text, language="diff")
                        
                        # Side-by-side view
                        col_old, col_new = st.columns(2)
                        with col_old:
                            st.markdown("**📦 Backup Version**")
                            st.code(backup_content, language="yaml")
                        with col_new:
                            st.markdown("**✅ Current Version**")
                            st.code(current_content, language="yaml")
                except Exception as e:
                    st.error(f"Could not read files: {e}")
    else:
        st.info("No schema changes have been recorded yet. Schema backups are created automatically when the self-healing loop applies a fix.")
    
    # Show current schema
    st.divider()
    st.subheader(f"📄 Current Schema: `{selected_dataset}.yaml`")
    try:
        st.code(current_file.read_text(), language="yaml")
    except Exception:
        st.warning("Schema file not found.")

# Footer
st.markdown("---")
st.caption("Agentic DRE Platform v1.2 | Powered by Agno & DuckDB")
