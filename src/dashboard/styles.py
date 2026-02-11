
def get_main_styles():
    return """
    <style>
        /* Import Inter Font */
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

        /* Global Resets */
        html, body, [class*="css"] {
            font-family: 'Inter', sans-serif;
        }

        /* App Background */
        .stApp {
            background-color: #f8f9fa; /* Light grey background like Databricks */
        }

        /* Sidebar Styling */
        section[data-testid="stSidebar"] {
            background-color: #ffffff;
            border-right: 1px solid #e0e0e0;
            padding-top: 2rem;
        }

        /* Headings */
        h1, h2, h3 {
            color: #1a1a1a;
            font-weight: 700;
        }
        
        h3 {
            font-size: 1.25rem;
            margin-bottom: 1rem;
        }

        /* Metrics */
        div[data-testid="stMetricValue"] {
            font-size: 2rem;
            font-weight: 700;
            color: #2c3e50;
        }
        div[data-testid="stMetricLabel"] {
            color: #6c757d;
            font-weight: 500;
            font-size: 0.9rem;
        }

        /* Custom Card Container */
        .agentic-card {
            background-color: #ffffff;
            border: 1px solid #e0e0e0;
            border-radius: 8px;
            padding: 1.5rem;
            margin-bottom: 1rem;
            box-shadow: 0 2px 4px rgba(0,0,0,0.02);
            transition: box-shadow 0.2s ease;
        }
        .agentic-card:hover {
            box-shadow: 0 4px 8px rgba(0,0,0,0.05);
        }

        /* Pulse Table Styling */
        .pulse-row {
            background-color: white;
            border-bottom: 1px solid #f0f0f0;
            padding: 12px 16px;
            transition: background-color 0.1s;
        }
        .pulse-row:hover {
            background-color: #fafafa;
        }
        
        .pulse-header-text {
            color: #888;
            font-weight: 600;
            font-size: 0.8rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }

        /* Status Badges */
        .badge {
            display: inline-flex;
            align-items: center;
            padding: 2px 8px;
            border-radius: 12px;
            font-size: 0.75rem;
            font-weight: 600;
            line-height: 1.5;
        }
        
        .badge-passed {
            background-color: #ecfdf5;
            color: #047857;
            border: 1px solid #d1fae5;
        }
        
        .badge-warning {
            background-color: #fffbeb;
            color: #b45309;
            border: 1px solid #fde68a;
        }
        
        .badge-blocked {
            background-color: #fef2f2;
            color: #b91c1c;
            border: 1px solid #fecaca;
        }
        
        .badge-criticality-high {
            background-color: #fef2f2;
            color: #dc2626;
            font-weight: 700;
        }
        
        .badge-criticality-med {
            background-color: #fff7ed;
            color: #ea580c;
        }

        /* Utility */
        .text-muted { color: #6c757d; }
        .text-small { font-size: 0.85rem; }
        .font-bold { font-weight: 600; }
        
        /* Divider update */
        hr {
            margin: 1.5rem 0;
            border-color: #eee;
        }

        /* Streamlit Element Overrides to make it look less like Streamlit */
        div[data-testid="stDecoration"] {
            background-image: linear-gradient(90deg, #4f46e5, #06b6d4);
        }
        
        button[kind="primary"] {
            background-color: #4f46e5;
            border-color: #4f46e5;
            transition: all 0.2s;
        }
        button[kind="primary"]:hover {
            background-color: #4338ca;
            border-color: #4338ca;
            box-shadow: 0 4px 6px rgba(79, 70, 229, 0.2);
        }

    </style>
    """
