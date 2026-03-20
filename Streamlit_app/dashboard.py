# ================================================================
# streamlit_app\app\dashboard.py
# ChargeEase EV Network - Dashboard with Smart AI Assistant
# RUN: "C:\Program Files\Python311\python.exe" -m streamlit run streamlit_app\app\dashboard.py
# ================================================================

import streamlit as st        
import snowflake.connector
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# ============================================================
# CREDENTIALS
# ============================================================
SNOWFLAKE_ACCOUNT   = st.secrets.get("SNOWFLAKE_ACCOUNT",   "SNYFGVD-AE23200")
SNOWFLAKE_USER      = st.secrets.get("SNOWFLAKE_USER",      "SATYAPRAKASH")
SNOWFLAKE_PASSWORD  = st.secrets.get("SNOWFLAKE_PASSWORD",  "RollsRoyce@22290")
SNOWFLAKE_WAREHOUSE = st.secrets.get("SNOWFLAKE_WAREHOUSE", "CHARGEEASE_WH")
SNOWFLAKE_DATABASE  = st.secrets.get("SNOWFLAKE_DATABASE",  "CHARGEEASE_DB")
SNOWFLAKE_SCHEMA    = st.secrets.get("SNOWFLAKE_SCHEMA",    "RAW")

# ============================================================
# PAGE CONFIG
# ============================================================
st.set_page_config(
    page_title            = "ChargeEase EV Platform",
    page_icon             = "⚡",
    layout                = "wide",
    initial_sidebar_state = "expanded",
)

# ============================================================
# CSS
# ============================================================
st.markdown("""
<style>
    .stApp { background-color: #0a0e1a; color: #e0e6f0; }
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0d1526 0%, #111d35 100%);
        border-right: 1px solid #1e3a5f;
    }
    [data-testid="metric-container"] {
        background: #111d35;
        border: 1px solid #1e3a5f;
        border-radius: 12px;
        padding: 16px;
    }
    [data-testid="stMetricValue"] { color: #00d4ff; font-size: 2rem !important; }
    [data-testid="stMetricLabel"] { color: #7a9cc4; }
    h1, h2, h3 { color: #00d4ff !important; }
    .stButton > button {
        background: #00d4ff; color: #0a0e1a;
        border: none; border-radius: 8px;
        font-weight: 600; padding: 8px 24px;
    }
    .stButton > button:hover { background: #00b8d9; }
    [data-testid="stDataFrame"] { border: 1px solid #1e3a5f; border-radius: 8px; }
    .chat-msg-user {
        background: #1e3a5f; border-radius: 12px;
        padding: 12px 16px; margin: 8px 0;
        border-left: 3px solid #00d4ff;
    }
    .chat-msg-ai {
        background: #111d35; border-radius: 12px;
        padding: 12px 16px; margin: 8px 0;
        border-left: 3px solid #00ff88;
    }
</style>
""", unsafe_allow_html=True)


# ============================================================
# SNOWFLAKE
# ============================================================
@st.cache_resource
def get_conn():
    return snowflake.connector.connect(
        account   = SNOWFLAKE_ACCOUNT,
        user      = SNOWFLAKE_USER,
        password  = SNOWFLAKE_PASSWORD,
        warehouse = SNOWFLAKE_WAREHOUSE,
        database  = SNOWFLAKE_DATABASE,
        schema    = SNOWFLAKE_SCHEMA,
    )

@st.cache_data(ttl=300)
def run_query(_conn, sql):
    try:
        return pd.read_sql(sql, _conn)
    except Exception as e:
        st.error(f"Query error: {e}")
        return pd.DataFrame()

conn = get_conn()


# ============================================================
# SMART AI ASSISTANT - Works with your real Snowflake data
# ============================================================
def smart_ai(question, conn):
    q = question.lower().strip()

    # ── Fetch live data ──────────────────────────────────────
    df_anomaly = run_query(conn, "SELECT * FROM CHARGEEASE_DB.CURATED.VW_KPI1_ANOMALY_SCORE ORDER BY ANOMALY_SCORE_PCT DESC")
    df_uptime  = run_query(conn, "SELECT * FROM CHARGEEASE_DB.CURATED.VW_KPI2_UPTIME_SCORE ORDER BY UPTIME_PCT ASC")
    df_active  = run_query(conn, """
        SELECT (SELECT COUNT(*) FROM CHARGEEASE_DB.CURATED.DIM_USERS) AS total,
               COUNT(DISTINCT user_id) AS active,
               ROUND(COUNT(DISTINCT user_id)*100.0/NULLIF((SELECT COUNT(*) FROM CHARGEEASE_DB.CURATED.DIM_USERS),0),1) AS ratio
        FROM CHARGEEASE_DB.CURATED.FACT_SESSIONS
    """)
    df_grid    = run_query(conn, "SELECT * FROM CHARGEEASE_DB.CURATED.VW_KPI4_GRID_LOAD ORDER BY GRID_LOAD_INDEX ASC")
    df_arps    = run_query(conn, "SELECT * FROM CHARGEEASE_DB.CURATED.VW_KPI5_ARPS ORDER BY AVG_REVENUE_PER_SESSION DESC")
    df_fraud   = run_query(conn, "SELECT fraud_rule, COUNT(*) AS cnt FROM CHARGEEASE_DB.AUDIT.FRAUD_ALERTS GROUP BY fraud_rule ORDER BY cnt DESC")
    df_sessions= run_query(conn, "SELECT * FROM CHARGEEASE_DB.CURATED.FACT_SESSIONS")
    df_chargers= run_query(conn, "SELECT * FROM CHARGEEASE_DB.CURATED.DIM_CHARGERS WHERE is_current=TRUE")
    df_stations= run_query(conn, "SELECT * FROM CHARGEEASE_DB.CURATED.DIM_STATIONS WHERE is_current=TRUE")
    df_users   = run_query(conn, "SELECT * FROM CHARGEEASE_DB.CURATED.DIM_USERS")

    # ── ANOMALY questions ────────────────────────────────────
    if any(w in q for w in ["anomaly","highest anomaly","worst charger","flagged","suspicious"]):
        if df_anomaly.empty:
            return "⚡ No anomaly data found yet. Run the pipeline first."
        worst = df_anomaly.iloc[0]
        best  = df_anomaly.iloc[-1]
        total_flagged = df_sessions['IS_FLAGGED'].sum() if not df_sessions.empty else 0
        return f"""📊 **KPI 1 — Charger Anomaly Score Analysis**

🔴 **Highest Anomaly:** Charger **{int(worst['CHARGER_ID'])}** in **{worst['CITY']}** ({worst['ZONE']} zone) with **{worst['ANOMALY_SCORE_PCT']}%** anomaly score
✅ **Best Performing:** Charger **{int(best['CHARGER_ID'])}** in **{best['CITY']}** with **{best['ANOMALY_SCORE_PCT']}%** anomaly score
🚩 **Total Flagged Sessions:** {int(total_flagged)} sessions across all chargers

**Top 3 Anomalous Chargers:**
{chr(10).join([f"  • Charger {int(row['CHARGER_ID'])} ({row['CITY']}-{row['ZONE']}): {row['ANOMALY_SCORE_PCT']}%" for _, row in df_anomaly.head(3).iterrows()])}

**Recommendations:**
• Investigate Charger {int(worst['CHARGER_ID'])} immediately — high fraud risk
• Check firmware version on top anomalous chargers
• Review flagged sessions in AUDIT.FRAUD_ALERTS table"""

    # ── UPTIME questions ─────────────────────────────────────
    elif any(w in q for w in ["uptime","downtime","offline","available","maintenance","sla"]):
        if df_uptime.empty:
            return "⚡ No uptime data found yet."
        worst_zone = df_uptime.iloc[0]
        best_zone  = df_uptime.iloc[-1]
        avg_uptime = df_uptime['UPTIME_PCT'].mean()
        offline_count = len(df_chargers[df_chargers['STATUS'].isin(['Offline','Fault'])]) if not df_chargers.empty else 0
        return f"""🔋 **KPI 2 — Charger Uptime Score Analysis**

📊 **Average Uptime:** {avg_uptime:.1f}% across all zones
{'✅ Above 90% SLA target!' if avg_uptime >= 90 else '⚠️ Below 90% SLA target — action needed!'}

🔴 **Worst Zone:** {worst_zone['CITY']}-{worst_zone['ZONE']} with only **{worst_zone['UPTIME_PCT']}%** uptime
✅ **Best Zone:** {best_zone['CITY']}-{best_zone['ZONE']} with **{best_zone['UPTIME_PCT']}%** uptime
⚡ **Offline/Fault Chargers:** {offline_count} chargers need attention

**All Zones:**
{chr(10).join([f"  • {row['CITY']}-{row['ZONE']}: {row['UPTIME_PCT']}% ({row['UP_CHARGERS']}/{row['TOTAL_CHARGERS']} chargers up)" for _, row in df_uptime.iterrows()])}

**Recommendations:**
• Prioritize maintenance in {worst_zone['CITY']}-{worst_zone['ZONE']} zone
• Schedule predictive maintenance for chargers with error codes
• Target 95%+ uptime for SLA compliance"""

    # ── ACTIVE USERS questions ───────────────────────────────
    elif any(w in q for w in ["active user","user ratio","engagement","registered","inactive user"]):
        if df_active.empty:
            return "⚡ No user data found yet."
        row = df_active.iloc[0]
        inactive = int(row['TOTAL']) - int(row['ACTIVE'])
        fleet_users = len(df_users[df_users['IS_FLEET_USER']==1]) if not df_users.empty else 0
        verified = len(df_users[df_users['KYC_STATUS']=='Verified']) if not df_users.empty else 0
        return f"""👥 **KPI 3 — Active User Charging Ratio Analysis**

📊 **Total Registered Users:** {int(row['TOTAL'])}
✅ **Active Users:** {int(row['ACTIVE'])} users have charged at least once
😴 **Inactive Users:** {inactive} users never charged
📈 **Active Ratio:** {row['RATIO']}%

**User Breakdown:**
  • Fleet Users: {fleet_users}
  • Verified KYC: {verified}
  • Pending/Rejected KYC: {int(row['TOTAL']) - verified}

**Cities with Users:**
{chr(10).join([f"  • {row['CITY']}: {row['cnt']} users" for _, row in df_users.groupby('CITY').size().reset_index(name='cnt').iterrows()]) if not df_users.empty else "  No city data"}

**Recommendations:**
• Send re-engagement notifications to {inactive} inactive users
• Offer first-charge discount to convert inactive users
• Focus fleet user acquisition for bulk revenue"""

    # ── GRID LOAD questions ──────────────────────────────────
    elif any(w in q for w in ["grid","load","discom","capacity","power","energy","station"]):
        if df_grid.empty:
            return "⚡ No grid data found yet."
        worst = df_grid.iloc[0]
        best  = df_grid.iloc[-1]
        total_energy = df_sessions['ENERGY_KWH'].sum() if not df_sessions.empty else 0
        return f"""⚡ **KPI 4 — Grid Load Distribution Index Analysis**

📊 **Total Energy Delivered:** {total_energy:.1f} kWh across all sessions
🔴 **Most Stressed Zone:** {worst['CITY']}-{worst['ZONE']} with grid index **{worst['GRID_LOAD_INDEX']}%**
✅ **Best Balanced Zone:** {best['CITY']}-{best['ZONE']} with index **{best['GRID_LOAD_INDEX']}%**

**All Zones Grid Status:**
{chr(10).join([f"  • {row['CITY']}-{row['ZONE']}: {row['GRID_LOAD_INDEX']}% ({row['BALANCED_STATIONS']}/{row['TOTAL_STATIONS']} stations balanced)" for _, row in df_grid.iterrows()])}

**Recommendations:**
• Implement time-of-use pricing to shift load from peak hours
• Add more charging capacity in {worst['CITY']}-{worst['ZONE']}
• Coordinate with DISCOM for load balancing in stressed zones
• Use Snowflake Streams to trigger real-time load alerts"""

    # ── REVENUE/ARPS questions ───────────────────────────────
    elif any(w in q for w in ["revenue","arps","tariff","pricing","money","income","profit"]):
        if df_arps.empty:
            return "⚡ No revenue data found yet."
        best_tariff  = df_arps.iloc[0]
        worst_tariff = df_arps.iloc[-1]
        total_rev    = df_arps['TOTAL_REVENUE'].sum()
        total_sess   = df_arps['TOTAL_SESSIONS'].sum()
        return f"""💰 **KPI 5 — Average Revenue Per Session (ARPS) Analysis**

📊 **Total Revenue:** Rs {total_rev:.2f} across {int(total_sess)} sessions
💎 **Best Tariff:** {best_tariff['TARIFF_TYPE']} — Rs {best_tariff['AVG_REVENUE_PER_SESSION']} avg per session
📉 **Lowest Tariff:** {worst_tariff['TARIFF_TYPE']} — Rs {worst_tariff['AVG_REVENUE_PER_SESSION']} avg per session

**Revenue by Tariff:**
{chr(10).join([f"  • {row['TARIFF_TYPE']}: Rs {row['AVG_REVENUE_PER_SESSION']} avg | {int(row['TOTAL_SESSIONS'])} sessions | Rs {row['TOTAL_REVENUE']} total" for _, row in df_arps.iterrows()])}

**Recommendations:**
• Promote {best_tariff['TARIFF_TYPE']} tariff to maximize revenue
• Review {worst_tariff['TARIFF_TYPE']} pricing — consider increasing rates
• Introduce dynamic pricing based on demand patterns
• Target corporate fleet deals for guaranteed revenue"""

    # ── FRAUD questions ──────────────────────────────────────
    elif any(w in q for w in ["fraud","alert","theft","suspicious","soc jump","ghost","parallel","idle","anomaly rule"]):
        if df_fraud.empty:
            return "✅ No fraud alerts found in the system currently."
        total_alerts = df_fraud['CNT'].sum()
        top_rule     = df_fraud.iloc[0]
        return f"""🚨 **Fraud Detection Analysis**

⚠️ **Total Fraud Alerts:** {int(total_alerts)} suspicious sessions detected

**Alerts by Rule:**
{chr(10).join([f"  {'🔴' if i==0 else '🟡'} {row['FRAUD_RULE']}: {int(row['CNT'])} alerts" for i, (_, row) in enumerate(df_fraud.iterrows())])}

**Most Triggered Rule:** {top_rule['FRAUD_RULE']} with {int(top_rule['CNT'])} alerts

**Fraud Rules Explained:**
  • **SOC_JUMP** — Battery SOC rose >30% in <2 minutes (energy theft)
  • **ENERGY_OVERFLOW** — Energy delivered > charger max capacity
  • **GHOST_SESSION** — Flagged session with <60 seconds duration
  • **PARALLEL_SESSION** — Same user charging at 2 places simultaneously
  • **EXCESSIVE_IDLE** — Charger blocked for >1 hour after charging

**Recommendations:**
• Immediately investigate sessions with {top_rule['FRAUD_RULE']} alerts
• Cross-check with payment records for billing fraud
• Update firmware on chargers with frequent ghost sessions"""

    # ── PIPELINE/TECHNICAL questions ────────────────────────
    elif any(w in q for w in ["pipeline","raw","validated","curated","scd","merge","copy into","stage","s3"]):
        raw_counts = {t: int(run_query(conn, f"SELECT COUNT(*) AS c FROM CHARGEEASE_DB.RAW.{t}")['C'].iloc[0])
                      for t in ["EV_STATIONS","CHARGERS","SESSIONS","USERS","VEHICLES"]}
        val_counts = {t: int(run_query(conn, f"SELECT COUNT(*) AS c FROM CHARGEEASE_DB.VALIDATED.{t}")['C'].iloc[0])
                      for t in ["EV_STATIONS","CHARGERS","SESSIONS","USERS","VEHICLES"]}
        return f"""🔧 **Data Pipeline Status**

**RAW Layer (S3 → Snowflake via COPY INTO):**
{chr(10).join([f"  • {t}: {c} rows" for t,c in raw_counts.items()])}

**VALIDATED Layer (DQ checks applied):**
{chr(10).join([f"  • {t}: {c} rows" for t,c in val_counts.items()])}

**Pipeline Architecture:**
  1. CSV files uploaded to S3 bucket via boto3 API
  2. Snowflake External Stage links S3 → Snowflake
  3. COPY INTO loads files into RAW tables
  4. SP_VALIDATE_ALL() applies DQ checks → VALIDATED
  5. SP_MERGE_*() procedures build CURATED star schema
  6. SCD-2 tracks historical changes to stations & chargers
  7. SP_DETECT_FRAUD() runs 5 fraud detection rules
  8. TASK_DAILY_PIPELINE runs automatically every day at 1am UTC

**S3 Bucket:** chargeease-ev-data-2024-984543547413-us-east-1-an
**Snowflake DB:** CHARGEEASE_DB (RAW → VALIDATED → CURATED → AUDIT)"""

    # ── KPI SUMMARY ──────────────────────────────────────────
    elif any(w in q for w in ["summary","all kpi","overview","report","analyze","status","platform"]):
        avg_anomaly = df_anomaly['ANOMALY_SCORE_PCT'].mean() if not df_anomaly.empty else 0
        avg_uptime  = df_uptime['UPTIME_PCT'].mean() if not df_uptime.empty else 0
        active_ratio= df_active['RATIO'].iloc[0] if not df_active.empty else 0
        avg_grid    = df_grid['GRID_LOAD_INDEX'].mean() if not df_grid.empty else 0
        best_arps   = df_arps['AVG_REVENUE_PER_SESSION'].max() if not df_arps.empty else 0
        total_fraud = df_fraud['CNT'].sum() if not df_fraud.empty else 0
        total_sess  = len(df_sessions) if not df_sessions.empty else 0

        issues = []
        recs   = []
        if avg_anomaly > 50:
            issues.append(f"🔴 High anomaly score ({avg_anomaly:.1f}%) — possible fraud/theft")
            recs.append("• Investigate top flagged chargers immediately")
        if avg_uptime < 90:
            issues.append(f"🟡 Uptime below SLA ({avg_uptime:.1f}% < 90%)")
            recs.append("• Schedule urgent maintenance for offline chargers")
        if active_ratio < 50:
            issues.append(f"🟡 Low user engagement ({active_ratio}% active ratio)")
            recs.append("• Launch user re-engagement campaign")
        if total_fraud > 5:
            issues.append(f"🔴 High fraud alerts ({int(total_fraud)} alerts detected)")
            recs.append("• Review fraud alerts in AUDIT.FRAUD_ALERTS table")

        return f"""📋 **ChargeEase Platform — Complete KPI Summary**

**Platform Overview:**
  • Total Sessions: {total_sess}
  • Total Users: {len(df_users) if not df_users.empty else 0}
  • Total Stations: {len(df_stations) if not df_stations.empty else 0}
  • Total Chargers: {len(df_chargers) if not df_chargers.empty else 0}

**KPI Scorecard:**
  📊 KPI 1 — Anomaly Score:    {avg_anomaly:.1f}%  {'✅' if avg_anomaly < 30 else '⚠️'}
  🔋 KPI 2 — Charger Uptime:  {avg_uptime:.1f}%  {'✅' if avg_uptime >= 90 else '⚠️'}
  👥 KPI 3 — Active Users:     {active_ratio}%   {'✅' if float(active_ratio) >= 50 else '⚠️'}
  ⚡ KPI 4 — Grid Load Index:  {avg_grid:.1f}%  {'✅' if avg_grid >= 80 else '⚠️'}
  💰 KPI 5 — Best ARPS:        Rs {best_arps}   ✅
  🚨 Fraud Alerts:             {int(total_fraud)} alerts {'✅' if total_fraud == 0 else '⚠️'}

**Top Issues Identified:**
{chr(10).join(issues) if issues else '  ✅ No critical issues found!'}

**Recommendations:**
{chr(10).join(recs) if recs else '  ✅ Platform is performing well!'}"""

    # ── CHARGER questions ────────────────────────────────────
    elif any(w in q for w in ["charger","ac","dc","firmware","maintenance","error code"]):
        if df_chargers.empty:
            return "⚡ No charger data found."
        ac = len(df_chargers[df_chargers['CHARGER_TYPE']=='AC'])
        dc = len(df_chargers[df_chargers['CHARGER_TYPE']=='DC'])
        fault = len(df_chargers[df_chargers['STATUS'].isin(['Fault','Offline'])])
        return f"""🔌 **Charger Fleet Analysis**

**Total Chargers:** {len(df_chargers)}
  • AC Chargers: {ac}
  • DC Chargers: {dc}
  • Fault/Offline: {fault} need attention

**Status Breakdown:**
{chr(10).join([f"  • {status}: {count}" for status, count in df_chargers['STATUS'].value_counts().items()])}

**Max Power Capacity:**
  • Highest: {df_chargers['MAX_KW'].max()} kW
  • Average: {df_chargers['MAX_KW'].mean():.1f} kW
  • Lowest: {df_chargers['MAX_KW'].min()} kW

**Recommendations:**
• Replace chargers with persistent error codes
• Upgrade firmware on older versions
• Add more DC fast chargers for fleet users"""

    # ── DEFAULT response ─────────────────────────────────────
    else:
        return f"""🤖 **ChargeEase AI Assistant**

I can answer questions about your platform! Try asking:

📊 **KPI Questions:**
  • "Which charger has highest anomaly?"
  • "How is the uptime score performing?"
  • "What is the active user ratio?"
  • "Show grid load analysis"
  • "Which tariff generates most revenue?"

🚨 **Fraud & Security:**
  • "How many fraud alerts are there?"
  • "Explain the fraud detection rules"
  • "Which sessions are suspicious?"

🔧 **Technical:**
  • "Explain the data pipeline"
  • "How does SCD-2 work here?"
  • "What is in the RAW layer?"

📋 **Business:**
  • "Give me a platform summary"
  • "Analyze all KPIs"
  • "What are the top issues?"

Your question: **"{question}"** — try rephrasing with keywords above!"""


# ============================================================
# SIDEBAR
# ============================================================
with st.sidebar:
    st.markdown("## ⚡ ChargeEase")
    st.markdown("**EV Charging Network**")
    st.markdown("*Data Platform Dashboard*")
    st.markdown("---")

    page = st.selectbox("Navigate", [
        "🏠  Overview",
        "📊  KPI 1 — Anomaly Score",
        "🔋  KPI 2 — Charger Uptime",
        "👥  KPI 3 — Active Users",
        "⚡  KPI 4 — Grid Load",
        "💰  KPI 5 — Revenue (ARPS)",
        "🚨  Fraud Alerts",
        "✅  Data Quality",
        "🔍  Data Lineage",
        "🤖  AI Assistant",
    ])

    st.markdown("---")
    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()

    st.markdown("---")
    st.markdown("**Platform Info**")
    st.markdown("DB: **CHARGEEASE_DB**")
    st.markdown("Cloud: **AWS us-east-1**")
    st.markdown("User: **SATYAPRAKASH**")


# ================================================================
# OVERVIEW
# ================================================================
if "Overview" in page:
    st.markdown("# ⚡ ChargeEase EV Network Platform")
    st.markdown("*Real-time insights from Snowflake CURATED layer*")
    st.markdown("---")

    col1,col2,col3,col4,col5 = st.columns(5)
    df_a  = run_query(conn,"SELECT ROUND(AVG(anomaly_score_pct),1) AS v FROM CHARGEEASE_DB.CURATED.VW_KPI1_ANOMALY_SCORE")
    df_u  = run_query(conn,"SELECT ROUND(AVG(uptime_pct),1) AS v FROM CHARGEEASE_DB.CURATED.VW_KPI2_UPTIME_SCORE")
    df_ar = run_query(conn,"SELECT ROUND(COUNT(DISTINCT user_id)*100.0/NULLIF((SELECT COUNT(*) FROM CHARGEEASE_DB.CURATED.DIM_USERS),0),1) AS v FROM CHARGEEASE_DB.CURATED.FACT_SESSIONS")
    df_g  = run_query(conn,"SELECT ROUND(AVG(grid_load_index),1) AS v FROM CHARGEEASE_DB.CURATED.VW_KPI4_GRID_LOAD")
    df_r  = run_query(conn,"SELECT ROUND(AVG(avg_revenue_per_session),0) AS v FROM CHARGEEASE_DB.CURATED.VW_KPI5_ARPS")

    with col1: st.metric("Anomaly Score",  f"{df_a['V'].iloc[0] if not df_a.empty else 0}%",   "lower=better")
    with col2: st.metric("Charger Uptime", f"{df_u['V'].iloc[0] if not df_u.empty else 0}%",  "target 90%")
    with col3: st.metric("Active Users",   f"{df_ar['V'].iloc[0] if not df_ar.empty else 0}%","engagement")
    with col4: st.metric("Grid Load",      f"{df_g['V'].iloc[0] if not df_g.empty else 0}%",  "balanced")
    with col5: st.metric("Avg Revenue",    f"Rs {df_r['V'].iloc[0] if not df_r.empty else 0}","per session")

    st.markdown("---")
    col_l,col_r = st.columns(2)
    with col_l:
        st.subheader("Revenue by Tariff Type")
        df = run_query(conn,"SELECT * FROM CHARGEEASE_DB.CURATED.VW_KPI5_ARPS")
        if not df.empty:
            fig = px.bar(df,x="TARIFF_TYPE",y="TOTAL_REVENUE",color="TARIFF_TYPE",text="AVG_REVENUE_PER_SESSION",
                         color_discrete_sequence=["#00d4ff","#00ff88","#ffcc00","#ff6b6b"])
            fig.update_traces(texttemplate="Rs%{text}",textposition="outside")
            fig.update_layout(paper_bgcolor="#111d35",plot_bgcolor="#111d35",font_color="#e0e6f0",showlegend=False,height=300)
            fig.update_yaxes(gridcolor="#1e3a5f")
            st.plotly_chart(fig,use_container_width=True)
    with col_r:
        st.subheader("Charger Status Distribution")
        df = run_query(conn,"SELECT status,COUNT(*) AS cnt FROM CHARGEEASE_DB.CURATED.DIM_CHARGERS WHERE is_current=TRUE GROUP BY status")
        if not df.empty:
            fig = px.pie(df,names="STATUS",values="CNT",hole=0.4,
                         color_discrete_sequence=["#00d4ff","#00ff88","#ffcc00","#ff6b6b"])
            fig.update_layout(paper_bgcolor="#111d35",font_color="#e0e6f0",height=300)
            st.plotly_chart(fig,use_container_width=True)

    col_a,col_b = st.columns(2)
    with col_a:
        st.subheader("Charger Uptime by City")
        df = run_query(conn,"SELECT * FROM CHARGEEASE_DB.CURATED.VW_KPI2_UPTIME_SCORE")
        if not df.empty:
            fig = px.bar(df,x="CITY",y="UPTIME_PCT",color="ZONE",barmode="group",text="UPTIME_PCT",
                         color_discrete_sequence=["#00d4ff","#00ff88","#ffcc00","#a78bfa"])
            fig.add_hline(y=90,line_dash="dash",line_color="#ff4444",annotation_text="90% SLA")
            fig.update_layout(paper_bgcolor="#111d35",plot_bgcolor="#111d35",font_color="#e0e6f0",height=300)
            fig.update_yaxes(gridcolor="#1e3a5f")
            st.plotly_chart(fig,use_container_width=True)
    with col_b:
        st.subheader("Anomaly Score by Charger")
        df = run_query(conn,"SELECT * FROM CHARGEEASE_DB.CURATED.VW_KPI1_ANOMALY_SCORE ORDER BY ANOMALY_SCORE_PCT DESC")
        if not df.empty:
            fig = px.bar(df,x="CHARGER_ID",y="ANOMALY_SCORE_PCT",color="ANOMALY_SCORE_PCT",text="ANOMALY_SCORE_PCT",
                         color_continuous_scale=["#00ff88","#ffcc00","#ff4444"])
            fig.update_traces(texttemplate="%{text}%",textposition="outside")
            fig.update_layout(paper_bgcolor="#111d35",plot_bgcolor="#111d35",font_color="#e0e6f0",showlegend=False,height=300)
            fig.update_yaxes(gridcolor="#1e3a5f")
            st.plotly_chart(fig,use_container_width=True)

    st.markdown("---")
    st.subheader("🚨 Recent Fraud Alerts")
    df = run_query(conn,"SELECT session_id,user_id,charger_id,fraud_rule,detail,alerted_at FROM CHARGEEASE_DB.AUDIT.FRAUD_ALERTS ORDER BY alerted_at DESC LIMIT 10")
    if df.empty: st.success("✅ No fraud alerts found!")
    else: st.dataframe(df,use_container_width=True)


# ================================================================
# KPI 1
# ================================================================
elif "Anomaly" in page:
    st.markdown("# 📊 KPI 1 — Charger Anomaly Score")
    st.info("**Formula:** (Flagged Sessions / Total Sessions) x 100 | **Use:** Detect theft, faulty firmware")
    df = run_query(conn,"SELECT * FROM CHARGEEASE_DB.CURATED.VW_KPI1_ANOMALY_SCORE ORDER BY ANOMALY_SCORE_PCT DESC")
    if not df.empty:
        c1,c2,c3 = st.columns(3)
        c1.metric("Highest Anomaly",f"{df['ANOMALY_SCORE_PCT'].max()}%")
        c2.metric("Average Score",  f"{round(df['ANOMALY_SCORE_PCT'].mean(),1)}%")
        c3.metric("Total Chargers", len(df))
        col_l,col_r = st.columns(2)
        with col_l:
            fig = px.bar(df,x="CHARGER_ID",y="ANOMALY_SCORE_PCT",color="ANOMALY_SCORE_PCT",text="ANOMALY_SCORE_PCT",
                         color_continuous_scale=["#00ff88","#ffcc00","#ff4444"],title="Anomaly Score per Charger (%)")
            fig.update_traces(texttemplate="%{text}%",textposition="outside")
            fig.update_layout(paper_bgcolor="#111d35",plot_bgcolor="#111d35",font_color="#e0e6f0",height=400)
            fig.update_yaxes(gridcolor="#1e3a5f")
            st.plotly_chart(fig,use_container_width=True)
        with col_r:
            fig = px.scatter(df,x="TOTAL_SESSIONS",y="ANOMALY_SCORE_PCT",size="FLAGGED_SESSIONS",color="CITY",
                             hover_data=["CHARGER_ID"],title="Sessions vs Anomaly Score",
                             color_discrete_sequence=["#00d4ff","#00ff88","#ffcc00"])
            fig.update_layout(paper_bgcolor="#111d35",plot_bgcolor="#111d35",font_color="#e0e6f0",height=400)
            st.plotly_chart(fig,use_container_width=True)
        st.dataframe(df,use_container_width=True)


# ================================================================
# KPI 2
# ================================================================
elif "Uptime" in page:
    st.markdown("# 🔋 KPI 2 — Charger Uptime Score")
    st.info("**Formula:** % of chargers in Available or Busy status | **Use:** Core SLA metric")
    df = run_query(conn,"SELECT * FROM CHARGEEASE_DB.CURATED.VW_KPI2_UPTIME_SCORE ORDER BY UPTIME_PCT DESC")
    if not df.empty:
        c1,c2,c3 = st.columns(3)
        c1.metric("Best Uptime",   f"{df['UPTIME_PCT'].max()}%")
        c2.metric("Average Uptime",f"{round(df['UPTIME_PCT'].mean(),1)}%")
        c3.metric("Zones Tracked", len(df))
        fig = px.bar(df,x="ZONE",y="UPTIME_PCT",color="CITY",barmode="group",text="UPTIME_PCT",
                     title="Charger Uptime % by City and Zone",
                     color_discrete_sequence=["#00d4ff","#00ff88","#ffcc00"])
        fig.add_hline(y=90,line_dash="dash",line_color="#ff4444",annotation_text="90% SLA")
        fig.update_layout(paper_bgcolor="#111d35",plot_bgcolor="#111d35",font_color="#e0e6f0",height=420)
        fig.update_yaxes(gridcolor="#1e3a5f")
        st.plotly_chart(fig,use_container_width=True)
        st.dataframe(df,use_container_width=True)


# ================================================================
# KPI 3
# ================================================================
elif "Active Users" in page:
    st.markdown("# 👥 KPI 3 — Active User Charging Ratio")
    st.info("**Formula:** % of registered users with >= 1 session | **Use:** Network engagement")
    df = run_query(conn,"""
        SELECT (SELECT COUNT(*) FROM CHARGEEASE_DB.CURATED.DIM_USERS) AS TOTAL_REGISTERED,
               COUNT(DISTINCT user_id) AS ACTIVE_USERS,
               ROUND(COUNT(DISTINCT user_id)*100.0/NULLIF((SELECT COUNT(*) FROM CHARGEEASE_DB.CURATED.DIM_USERS),0),2) AS ACTIVE_RATIO_PCT
        FROM CHARGEEASE_DB.CURATED.FACT_SESSIONS
    """)
    if not df.empty:
        row = df.iloc[0]
        c1,c2,c3 = st.columns(3)
        c1.metric("Total Registered",int(row["TOTAL_REGISTERED"]))
        c2.metric("Active Users",    int(row["ACTIVE_USERS"]))
        c3.metric("Active Ratio",    f"{row['ACTIVE_RATIO_PCT']}%")
        fig = go.Figure(go.Indicator(
            mode="gauge+number+delta",value=float(row["ACTIVE_RATIO_PCT"]),
            title={"text":"Active User Ratio (%)","font":{"color":"#e0e6f0"}},
            delta={"reference":60},
            gauge={"axis":{"range":[0,100],"tickcolor":"#7a9cc4"},"bar":{"color":"#00d4ff"},
                   "bgcolor":"#111d35",
                   "steps":[{"range":[0,40],"color":"#3d0000"},{"range":[40,70],"color":"#3d2d00"},{"range":[70,100],"color":"#003d1f"}],
                   "threshold":{"line":{"color":"#ff4444","width":4},"value":60}}
        ))
        fig.update_layout(paper_bgcolor="#111d35",font_color="#e0e6f0",height=380)
        st.plotly_chart(fig,use_container_width=True)
    df_city = run_query(conn,"SELECT city,kyc_status,COUNT(*) AS users FROM CHARGEEASE_DB.CURATED.DIM_USERS GROUP BY city,kyc_status")
    if not df_city.empty:
        fig = px.bar(df_city,x="CITY",y="USERS",color="KYC_STATUS",title="Users by City and KYC Status",
                     color_discrete_sequence=["#00d4ff","#00ff88","#ffcc00"])
        fig.update_layout(paper_bgcolor="#111d35",plot_bgcolor="#111d35",font_color="#e0e6f0",height=350)
        fig.update_yaxes(gridcolor="#1e3a5f")
        st.plotly_chart(fig,use_container_width=True)


# ================================================================
# KPI 4
# ================================================================
elif "Grid" in page:
    st.markdown("# ⚡ KPI 4 — Grid Load Distribution Index")
    st.info("**Formula:** % of stations with peak load < capacity | **Use:** Grid management with DISCOMs")
    df = run_query(conn,"SELECT * FROM CHARGEEASE_DB.CURATED.VW_KPI4_GRID_LOAD ORDER BY GRID_LOAD_INDEX DESC")
    if df.empty:
        st.warning("No grid data available")
    else:
        c1,c2 = st.columns(2)
        c1.metric("Best Grid Index",f"{df['GRID_LOAD_INDEX'].max()}%")
        c2.metric("Cities Tracked", df['CITY'].nunique())
        col_l,col_r = st.columns(2)
        with col_l:
            fig = px.bar(df,x="CITY",y="GRID_LOAD_INDEX",color="ZONE",barmode="group",
                         title="Grid Load Index by City & Zone (%)",
                         color_discrete_sequence=["#00d4ff","#00ff88","#ffcc00","#a78bfa"])
            fig.add_hline(y=80,line_dash="dash",line_color="#ffcc00",annotation_text="80% target")
            fig.update_layout(paper_bgcolor="#111d35",plot_bgcolor="#111d35",font_color="#e0e6f0",height=380)
            fig.update_yaxes(gridcolor="#1e3a5f")
            st.plotly_chart(fig,use_container_width=True)
        with col_r:
            fig = px.scatter(df,x="TOTAL_STATIONS",y="BALANCED_STATIONS",size="GRID_LOAD_INDEX",color="CITY",
                             title="Total vs Balanced Stations",
                             color_discrete_sequence=["#00d4ff","#00ff88","#ffcc00"])
            fig.update_layout(paper_bgcolor="#111d35",plot_bgcolor="#111d35",font_color="#e0e6f0",height=380)
            st.plotly_chart(fig,use_container_width=True)
        st.dataframe(df,use_container_width=True)


# ================================================================
# KPI 5
# ================================================================
elif "Revenue" in page:
    st.markdown("# 💰 KPI 5 — Average Revenue per Session (ARPS)")
    st.info("**Tariffs:** Flat / TimeOfUse / Peak / CorporateFleet | **Use:** Pricing optimization")
    df = run_query(conn,"SELECT * FROM CHARGEEASE_DB.CURATED.VW_KPI5_ARPS")
    if not df.empty:
        c1,c2,c3 = st.columns(3)
        c1.metric("Highest ARPS", f"Rs {df['AVG_REVENUE_PER_SESSION'].max()}")
        c2.metric("Total Revenue",f"Rs {df['TOTAL_REVENUE'].sum():.0f}")
        c3.metric("Tariff Types", len(df))
        col_l,col_r = st.columns(2)
        with col_l:
            fig = px.bar(df,x="TARIFF_TYPE",y="AVG_REVENUE_PER_SESSION",color="TARIFF_TYPE",
                         text="AVG_REVENUE_PER_SESSION",title="Avg Revenue per Session (Rs)",
                         color_discrete_sequence=["#00d4ff","#00ff88","#ffcc00","#ff6b6b"])
            fig.update_traces(texttemplate="Rs%{text}",textposition="outside")
            fig.update_layout(paper_bgcolor="#111d35",plot_bgcolor="#111d35",font_color="#e0e6f0",showlegend=False,height=380)
            fig.update_yaxes(gridcolor="#1e3a5f")
            st.plotly_chart(fig,use_container_width=True)
        with col_r:
            fig = px.pie(df,names="TARIFF_TYPE",values="TOTAL_REVENUE",title="Revenue Share by Tariff",hole=0.4,
                         color_discrete_sequence=["#00d4ff","#00ff88","#ffcc00","#ff6b6b"])
            fig.update_layout(paper_bgcolor="#111d35",font_color="#e0e6f0",height=380)
            st.plotly_chart(fig,use_container_width=True)
        st.dataframe(df,use_container_width=True)


# ================================================================
# FRAUD
# ================================================================
elif "Fraud" in page:
    st.markdown("# 🚨 Fraud & Anomaly Detection")
    df_summary = run_query(conn,"SELECT fraud_rule,COUNT(*) AS hits FROM CHARGEEASE_DB.AUDIT.FRAUD_ALERTS GROUP BY fraud_rule ORDER BY hits DESC")
    if not df_summary.empty:
        col_l,col_r = st.columns(2)
        with col_l:
            fig = px.bar(df_summary,x="FRAUD_RULE",y="HITS",color="FRAUD_RULE",title="Fraud Alerts by Rule",
                         color_discrete_sequence=["#ff4444","#ff8800","#ffcc00","#ff6b6b","#ff3399"])
            fig.update_layout(paper_bgcolor="#111d35",plot_bgcolor="#111d35",font_color="#e0e6f0",showlegend=False,height=350)
            fig.update_yaxes(gridcolor="#1e3a5f")
            st.plotly_chart(fig,use_container_width=True)
        with col_r:
            fig = px.pie(df_summary,names="FRAUD_RULE",values="HITS",title="Fraud Rule Distribution",hole=0.4,
                         color_discrete_sequence=["#ff4444","#ff8800","#ffcc00","#ff6b6b","#ff3399"])
            fig.update_layout(paper_bgcolor="#111d35",font_color="#e0e6f0",height=350)
            st.plotly_chart(fig,use_container_width=True)
    st.subheader("All Fraud Alert Details")
    df_all = run_query(conn,"SELECT alert_id,session_id,user_id,charger_id,fraud_rule,detail,alerted_at FROM CHARGEEASE_DB.AUDIT.FRAUD_ALERTS ORDER BY alerted_at DESC")
    if df_all.empty: st.success("✅ No fraud alerts found!")
    else:
        st.dataframe(df_all,use_container_width=True)
        st.download_button("⬇ Download Fraud Report",df_all.to_csv(index=False),"fraud_alerts.csv")


# ================================================================
# DATA QUALITY
# ================================================================
elif "Quality" in page:
    st.markdown("# ✅ Data Quality Dashboard")
    col_l,col_r = st.columns(2)
    with col_l:
        st.subheader("DQ Violations by Table")
        df = run_query(conn,"SELECT table_name,violation_type,COUNT(*) AS cnt FROM CHARGEEASE_DB.AUDIT.DQ_VIOLATIONS GROUP BY table_name,violation_type")
        if df.empty: st.success("✅ No DQ violations — data is clean!")
        else:
            fig = px.bar(df,x="TABLE_NAME",y="CNT",color="VIOLATION_TYPE",barmode="stack",
                         color_discrete_sequence=["#ff4444","#ff8800","#ffcc00"])
            fig.update_layout(paper_bgcolor="#111d35",plot_bgcolor="#111d35",font_color="#e0e6f0",height=350)
            st.plotly_chart(fig,use_container_width=True)
    with col_r:
        st.subheader("Batch Load Summary")
        df = run_query(conn,"SELECT table_name,source_file,rows_loaded,loaded_at FROM CHARGEEASE_DB.AUDIT.BATCH_LOAD_LOG ORDER BY loaded_at DESC LIMIT 15")
        if df.empty: st.warning("No batch log data")
        else: st.dataframe(df,use_container_width=True)
    df_rules = run_query(conn,"SELECT * FROM CHARGEEASE_DB.AUDIT.VW_RULE_HIT_RATES")
    if not df_rules.empty:
        st.subheader("Fraud Rule Hit Rates")
        fig = px.bar(df_rules,x="RULE_NAME",y="TOTAL_HITS",color="RULE_NAME",
                     color_discrete_sequence=["#ff4444","#ff8800","#ffcc00","#ff6b6b","#ff3399"])
        fig.update_layout(paper_bgcolor="#111d35",plot_bgcolor="#111d35",font_color="#e0e6f0",showlegend=False,height=300)
        st.plotly_chart(fig,use_container_width=True)


# ================================================================
# DATA LINEAGE - FIXED
# ================================================================
elif "Lineage" in page:
    st.markdown("# 🔍 Data Lineage & Audit Trail")

    c1,c2,c3 = st.columns(3)
    tables = ["EV_STATIONS","CHARGERS","SESSIONS","USERS","VEHICLES"]

    raw_total = sum(int(run_query(conn,f"SELECT COUNT(*) AS c FROM CHARGEEASE_DB.RAW.{t}")['C'].iloc[0]) for t in tables)
    val_total = sum(int(run_query(conn,f"SELECT COUNT(*) AS c FROM CHARGEEASE_DB.VALIDATED.{t}")['C'].iloc[0]) for t in tables)
    cur_tables = ["DIM_STATIONS","DIM_CHARGERS","DIM_USERS","DIM_VEHICLES","FACT_SESSIONS"]
    cur_total  = sum(int(run_query(conn,f"SELECT COUNT(*) AS c FROM CHARGEEASE_DB.CURATED.{t}")['C'].iloc[0]) for t in cur_tables)

    c1.metric("RAW Layer",       f"{raw_total} rows")
    c2.metric("VALIDATED Layer", f"{val_total} rows")
    c3.metric("CURATED Layer",   f"{cur_total} rows")

    st.markdown("---")
    col_l,col_r = st.columns(2)
    with col_l:
        dims   = ["DIM_STATIONS","DIM_CHARGERS","DIM_TARIFFS","DIM_USERS","DIM_VEHICLES"]
        counts = [int(run_query(conn,f"SELECT COUNT(*) AS c FROM CHARGEEASE_DB.CURATED.{d}")['C'].iloc[0]) for d in dims]
        fig = px.bar(x=dims,y=counts,title="CURATED Dimension Table Counts",color=dims,
                     color_discrete_sequence=["#00d4ff","#00ff88","#ffcc00","#a78bfa","#ff6b6b"])
        fig.update_layout(paper_bgcolor="#111d35",plot_bgcolor="#111d35",font_color="#e0e6f0",showlegend=False,height=350)
        fig.update_yaxes(gridcolor="#1e3a5f")
        st.plotly_chart(fig,use_container_width=True)

    with col_r:
        df_fact = run_query(conn,"SELECT tariff_type,COUNT(*) AS sessions FROM CHARGEEASE_DB.CURATED.FACT_SESSIONS GROUP BY tariff_type")
        if not df_fact.empty:
            fig = px.pie(df_fact,names="TARIFF_TYPE",values="SESSIONS",title="FACT_SESSIONS by Tariff",hole=0.4,
                         color_discrete_sequence=["#00d4ff","#00ff88","#ffcc00","#ff6b6b"])
            fig.update_layout(paper_bgcolor="#111d35",font_color="#e0e6f0",height=350)
            st.plotly_chart(fig,use_container_width=True)

    st.subheader("RAW vs VALIDATED Row Counts")
    raw_data = []
    for t in tables:
        r = int(run_query(conn,f"SELECT COUNT(*) AS c FROM CHARGEEASE_DB.RAW.{t}")['C'].iloc[0])
        v = int(run_query(conn,f"SELECT COUNT(*) AS c FROM CHARGEEASE_DB.VALIDATED.{t}")['C'].iloc[0])
        raw_data.append({"Table":t,"RAW":r,"VALIDATED":v,"Passed DQ":v,"Rejected":r-v})
    st.dataframe(pd.DataFrame(raw_data),use_container_width=True)

    st.subheader("Uptime Change Log")
    df_uptime = run_query(conn,"SELECT charger_id,old_status,new_status,changed_at FROM CHARGEEASE_DB.AUDIT.UPTIME_CHANGE_LOG ORDER BY changed_at DESC LIMIT 20")
    if df_uptime.empty: st.info("No status changes recorded yet")
    else: st.dataframe(df_uptime,use_container_width=True)


# ================================================================
# AI ASSISTANT - Smart, no API key needed
# ================================================================
elif "AI" in page:
    st.markdown("# 🤖 ChargeEase AI Assistant")
    st.markdown("*Powered by your live Snowflake data — no API key needed!*")
    st.markdown("---")

    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []

    # Quick question buttons
    st.subheader("Quick Questions — Click to Ask")
    col1,col2,col3 = st.columns(3)
    quick_qs = [
        ("Which charger has highest anomaly?", col1),
        ("How is uptime performing?",          col2),
        ("What is the active user ratio?",     col3),
        ("Show fraud alert summary",           col1),
        ("Which tariff earns most revenue?",   col2),
        ("Give me full platform summary",      col3),
        ("Explain the data pipeline",          col1),
        ("Analyze all KPIs",                   col2),
        ("Show grid load analysis",            col3),
    ]
    for q, col in quick_qs:
        if col.button(q, key=f"q_{q}"):
            answer = smart_ai(q, conn)
            st.session_state.chat_history.append({"role":"user",    "content":q})
            st.session_state.chat_history.append({"role":"assistant","content":answer})
            st.rerun()

    st.markdown("---")

    # Chat display
    if st.session_state.chat_history:
        st.subheader("Conversation")
        for msg in st.session_state.chat_history:
            if msg["role"] == "user":
                st.markdown(f'<div class="chat-msg-user">👤 <b>You:</b><br>{msg["content"]}</div>',
                            unsafe_allow_html=True)
            else:
                st.markdown(f'<div class="chat-msg-ai">🤖 <b>ChargeEase AI:</b><br>{msg["content"]}</div>',
                            unsafe_allow_html=True)

    # Input
    st.markdown("---")
    user_input = st.text_input(
        "Ask anything about your EV platform...",
        placeholder="e.g. Which city has worst uptime? How many fraud alerts today?",
        key="ai_input"
    )

    col_send, col_clear = st.columns([1,1])
    with col_send:
        if st.button("Send ➤") and user_input:
            answer = smart_ai(user_input, conn)
            st.session_state.chat_history.append({"role":"user",    "content":user_input})
            st.session_state.chat_history.append({"role":"assistant","content":answer})
            st.rerun()

    with col_clear:
        if st.button("Clear Chat 🗑"):
            st.session_state.chat_history = []
            st.rerun()


# Footer
st.markdown("---")
st.markdown(
    "<center style='color:#7a9cc4;font-size:12px'>"
    "ChargeEase EV Charging Network Data Platform | "
    "Powered by Snowflake + AWS S3 + Streamlit"
    "</center>",
    unsafe_allow_html=True
)
