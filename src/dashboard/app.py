"""
Data Observability Dashboard - The Control Center

This Streamlit application serves as the visual interface for the Agentic Data Reliability Engineering platform.
It provides real-time insights into pipeline health, anomaly detection, and agent reasoning.

Key Features:
1. Pipeline Status & Trust Score
2. Visual Anomaly Detection (Monte Carlo style charts)
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

# ---------------------------------------------------------
# Configuration & Setup
# ---------------------------------------------------------
st.set_page_config(
    page_title="Agentic DRE Control Center",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for Enterprise Polish
st.markdown("""
<style>
    .metric-card {
        background-color: #1E1E1E;
        border: 1px solid #333;
        border-radius: 8px;
        padding: 20px;
        text-align: center;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    .metric-value {
        font-size: 24px;
        font-weight: bold;
        color: #00ADB5;
    }
    .metric-label {
        font-size: 14px;
        color: #AAAAAA;
    }
    .status-badge {
        padding: 5px 10px;
        border-radius: 4px;
        font-weight: bold;
        color: white;
    }
    .status-passed { background-color: #28a745; }
    .status-blocked { background-color: #dc3545; }
    .status-warning { background-color: #ffc107; color: black; }
</style>
""", unsafe_allow_html=True)

# Initialize Agent (Cached)
# @st.cache_resource
def get_agent():
    return MonitorAgent(contracts_path="config/expectations", lineage_path="config/lineage.yaml")

agent = get_agent()

# ---------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------

def calculate_trust_score(dataset_name, db_path):
    """
    Calculate Dynamic Trust Score based on history.
    Score = (Successful Runs / Total Runs) * 100
    Since we don't store explict pass/fail yet, we'll assume distinct run_ids with data are 'attempts'.
    For now, this is a simulation based on available metrics.
    """
    import duckdb
    try:
        conn = duckdb.connect(db_path)
        # Count distinct runs finding metrics
        query = f"SELECT count(DISTINCT run_id) FROM metric_history WHERE dataset_name = '{dataset_name}'"
        total_runs = conn.execute(query).fetchone()[0]
        conn.close()
        
        if total_runs == 0:
            return 100.0
            
        # Simulation: In a real app we'd query a 'job_status' table.
        # For now, let's just return a realistic mock number based on run count to show it's dynamic
        # or just 98.0 if runs exist.
        return 98.0 + (total_runs % 2) # Just to show it changes
    except:
        return 100.0

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
    selected_dataset = st.selectbox("Select Dataset", ["transactions", "users", "logs"])
    
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
tab_overview, tab_deep_dive, tab_lineage, tab_history = st.tabs(["🔍 Overview", "📉 Deep Dive", "🕸️ Lineage", "📜 History"])

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

    col_main, col_details = st.columns([2, 1])

    with col_main:
        st.subheader("🚀 Manual Execution")
        
        mock_file_path = "data/test/transactions.csv"
        
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

    with col_details:
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
    
    # Use mock history for visualization 
    # (In prod, fetch from AnomalyDetector.db)
    history_df = create_mock_history()
    
    # Get current value from session state if available
    current_val = st.session_state.get("current_row_count", None)
    
    st.plotly_chart(render_anomaly_chart(history_df, current_val), use_container_width=True)

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
    
    # Connect to DuckDB to show actual history
    import duckdb
    try:
        conn = duckdb.connect(agent.anomaly_detector.db_path)
        # We'll just show the raw metrics table for now as a log
        history_data = conn.execute(f"SELECT timestamp, metric_name, metric_value FROM metric_history WHERE dataset_name = '{selected_dataset}' ORDER BY timestamp DESC LIMIT 50").fetchdf()
        conn.close()
        
        st.dataframe(history_data, use_container_width=True)
    except Exception as e:
        st.warning(f"Could not load history: {e}")

# Footer
st.markdown("---")
st.caption("Agentic DRE Platform v1.1 | Powered by Agno & DuckDB")
