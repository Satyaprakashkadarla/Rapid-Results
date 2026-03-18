# ⚡ ChargeEase EV Charging Network Data Platform

> A complete end-to-end data engineering solution built on **Snowflake** and **AWS S3** for an EV Charging Network operator — developed as part of a Hackathon.

---

## 📌 Business Objective

ChargeEase (EV charging network operator) wants a unified, trusted Snowflake-based data platform to manage **stations, chargers, sessions, vehicles, users, and payments**.

The goal is to improve:
- Charger uptime and predictive maintenance
- Fraud and anomaly detection
- Billing accuracy and revenue insights
- Grid load forecasting
- EV user experience

---

## 🏗️ System Architecture

```
┌─────────────────────────────────────────────────────────┐
│              Source Layer (Local Machine)                │
│  ev_stations · chargers · sessions · users · vehicles   │
│              master CSV + incremental CSV                │
└──────────────────────┬──────────────────────────────────┘
                       │ boto3 API (Python)
                       ▼
┌─────────────────────────────────────────────────────────┐
│                   AWS S3 Bucket                          │
│     chargeease-ev-data-2024 (us-east-1)                 │
│  stations/ · chargers/ · sessions/ · users/ · vehicles/ │
└──────────────────────┬──────────────────────────────────┘
                       │ Snowflake External Stage
                       ▼
┌─────────────────────────────────────────────────────────┐
│              Snowflake — CHARGEEASE_DB                   │
│                                                          │
│  RAW Schema      →  COPY INTO (10 CSV files)            │
│  VALIDATED Schema →  SP_VALIDATE_ALL() DQ checks        │
│  CURATED Schema  →  Star Schema SCD-2 MERGE             │
│  AUDIT Schema    →  Fraud Alerts · DQ Logs · Lineage    │
└──────────────────────┬──────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────┐
│         Streamlit Dashboard (10 pages)                   │
│     KPI 1-5 · Fraud · Quality · Lineage · AI Assistant  │
└─────────────────────────────────────────────────────────┘
```

---

## 🛠️ Tech Stack

| Technology | Purpose |
|---|---|
| **Snowflake** | Cloud data warehouse — RAW, VALIDATED, CURATED, AUDIT |
| **AWS S3** | Cloud storage for all 10 CSV dataset files |
| **Python + boto3** | Create S3 bucket and upload files via API |
| **PyCharm** | IDE for Python development |
| **Streamlit** | Frontend KPI monitoring dashboard |
| **Plotly** | Interactive charts and visualizations |
| **Python 3.11** | Pipeline scripting and automation |

---

## 📂 Project Structure

```
EV-Charging-Network-Data-Platform/
│
├── data/                              ← 10 CSV datasets
│   ├── ev_stations_master.csv         ← 10 records (bulk load)
│   ├── ev_stations_inc.csv            ← 4 records (incremental)
│   ├── chargers_master.csv
│   ├── chargers_inc.csv
│   ├── sessions_master.csv
│   ├── sessions_inc.csv
│   ├── users_master.csv
│   ├── users_inc.csv
│   ├── vehicles_master.csv
│   └── vehicles_inc.csv
│
├── sql/
│   └── FINAL_snowflake_setup.sql      ← Run once in Snowflake Worksheet
│
├── _upload_to_s3/
│   └── upload_to_s3.py                ← Upload CSVs to AWS S3
│
├── snowflake_load/
│   └── snowflake_load.py              ← Connect Snowflake + run pipeline
│
├── streamlit_app/
│   └── app/
│       └── dashboard.py               ← Streamlit KPI dashboard
│
└── README.md
```

---

## 🚀 How to Run

### Prerequisites
```bash
pip install snowflake-connector-python boto3 streamlit plotly pandas
```

### Step 1 — Run Snowflake SQL (one time only)
```
Open sql/FINAL_snowflake_setup.sql
Paste in Snowflake Worksheet → Run All
Creates: DB, schemas, tables, procedures, KPI views, security policies
```

### Step 2 — Upload CSV files to AWS S3
```bash
python _upload_to_s3/upload_to_s3.py
```
Output:
```
[OK] ev_stations_master.csv → s3://chargeease-ev-data-2024/stations/master/
[OK] sessions_master.csv    → s3://chargeease-ev-data-2024/sessions/master/
...
ALL 10 FILES UPLOADED SUCCESSFULLY!
```

### Step 3 — Run Snowflake Pipeline
```bash
python snowflake_load/snowflake_load.py
```
Output:
```
PART 1: Snowflake Connected
PART 2: RAW tables — 13 rows each
PART 3: Validate → Merge → Fraud Detection
PART 4: VALIDATED counts
PART 5: CURATED star schema counts
PART 6: All 5 KPI results
PART 7: Fraud alerts summary
PART 8: DQ violations + audit logs
```

### Step 4 — Launch Streamlit Dashboard
```bash
streamlit run streamlit_app/app/dashboard.py
```
Open browser → `http://localhost:8501`

---

## 🗄️ Data Pipeline — 3 Layers

### RAW Layer
- Exact copy of CSV files loaded via `COPY INTO`
- Snowflake External Stage points to S3 bucket
- Source file tracked via `METADATA$FILENAME`
- Both master (bulk) and inc (incremental) files loaded

### VALIDATED Layer
- `SP_VALIDATE_ALL()` stored procedure applies:
  - Deduplication by natural keys using `QUALIFY ROW_NUMBER()`
  - Timestamps normalized to UTC
  - Geo precision standardized (ROUND to 6 decimals)
  - Zone codes uppercased
  - Range validations: `energy_kwh >= 0`, `soc BETWEEN 0 AND 100`
  - Referential checks: `charger_id IN chargers`, `user_id IN users`
  - Temporal validity: `end_time >= start_time`
  - Violations logged to `AUDIT.DQ_VIOLATIONS`

### CURATED Layer — Star Schema
```
FACT_SESSIONS (21 columns)
    ├── DIM_STATIONS  (SCD-2) ← status, capacity, operator tracked
    ├── DIM_CHARGERS  (SCD-2) ← firmware, status, error_code tracked
    ├── DIM_TARIFFS   (SCD-2) ← rate changes tracked
    ├── DIM_USERS     (MERGE) ← simple upsert
    └── DIM_VEHICLES  (MERGE) ← simple upsert
```

---

## 📊 5 KPIs Delivered

### KPI 1 — Charger Anomaly Score
```sql
(COUNT(is_flagged=1) / COUNT(*)) × 100
```
Use: Detect theft, faulty firmware, dangerous charging patterns

### KPI 2 — Charger Uptime Score
```sql
% of chargers with status IN ('Available', 'Busy')
```
Use: Core SLA metric, triggers predictive maintenance

### KPI 3 — Active User Charging Ratio
```sql
COUNT(DISTINCT user_id with ≥1 session) / total_registered × 100
```
Use: Network engagement and seasonal patterns

### KPI 4 — Grid Load Distribution Index
```sql
% of stations where peak_load < capacity_kw
```
Use: Grid management with DISCOMs

### KPI 5 — Average Revenue Per Session (ARPS)
```sql
AVG(price) GROUP BY tariff_type
```
Tariffs: Flat / TimeOfUse / Peak / CorporateFleet

---

## 🚨 Fraud Detection — 5 Rules

| Rule | Trigger | Risk |
|---|---|---|
| `SOC_JUMP` | Battery SOC rose >30% in <2 minutes | Energy theft |
| `ENERGY_OVERFLOW` | Energy > charger max capacity | Meter tampering |
| `GHOST_SESSION` | Flagged session lasted <60 seconds | Billing fraud |
| `PARALLEL_SESSION` | Same user at 2 chargers simultaneously | Account sharing |
| `EXCESSIVE_IDLE` | Charger blocked >1 hour after charging | Slot blocking |

All alerts stored in `AUDIT.FRAUD_ALERTS` table.

---

## 🔄 SCD-2 — Slowly Changing Dimensions

Three tables track historical changes using SCD-2:

| Table | Tracks changes to |
|---|---|
| `DIM_STATIONS` | status, capacity_kw, operator |
| `DIM_CHARGERS` | status, firmware_version, error_code |
| `DIM_TARIFFS` | rate_per_kwh |

**How it works:**
1. Old record expired: `eff_end_date = TODAY-1`, `is_current = FALSE`
2. New record inserted: `eff_start_date = TODAY`, `eff_end_date = 9999-12-31`, `is_current = TRUE`

---

## 🔒 Security and Governance

### Dynamic Data Masking (PII Protection)
| Column | Non-Admin sees |
|---|---|
| `email` | `****@mail.com` |
| `phone` | `XXXXXX1234` |
| `vin` | `VIN-XXXX-0001` |

### Row Level Security
- Users see only data from their city zone
- `ACCOUNTADMIN` sees all data

### Audit Logs
- `AUDIT.BATCH_LOAD_LOG` — session counts per batch file
- `AUDIT.DQ_VIOLATIONS` — data quality failures
- `AUDIT.RULE_HIT_LOG` — fraud rule hit rates
- `AUDIT.UPTIME_CHANGE_LOG` — charger status history
- `AUDIT.DATA_LINEAGE` — end-to-end data flow tracking

### Automation
- `STREAM_NEW_SESSIONS` — detects new session records
- `STREAM_CHARGER_HEALTH` — detects charger status changes
- `TASK_DAILY_PIPELINE` — runs full pipeline at 1am UTC daily

---

## 💻 Streamlit Dashboard — 10 Pages

| Page | Content |
|---|---|
| 🏠 Overview | All 5 KPI metrics + 4 charts + fraud table |
| 📊 KPI 1 | Anomaly score bar chart + scatter plot |
| 🔋 KPI 2 | Uptime by city/zone + SLA threshold line |
| 👥 KPI 3 | Active user gauge meter + city breakdown |
| ⚡ KPI 4 | Grid load index charts |
| 💰 KPI 5 | Revenue by tariff pie + bar chart |
| 🚨 Fraud | All 5 fraud rules + download CSV button |
| ✅ Quality | DQ violations + batch load log |
| 🔍 Lineage | Data flow counts + star schema charts |
| 🤖 AI Assistant | Smart chatbot powered by live Snowflake data |

---

## 📋 Datasets Used

| File | Records | Description |
|---|---|---|
| ev_stations_master.csv | 10 | Initial bulk load |
| ev_stations_inc.csv | 4 | Incremental daily load |
| chargers_master.csv | 10 | Initial bulk load |
| chargers_inc.csv | 4 | Incremental daily load |
| sessions_master.csv | 10 | Initial bulk load |
| sessions_inc.csv | 4 | Incremental daily load |
| users_master.csv | 10 | Initial bulk load |
| users_inc.csv | 4 | Incremental daily load |
| vehicles_master.csv | 10 | Initial bulk load |
| vehicles_inc.csv | 4 | Incremental daily load |

Cities covered: **Vijayawada, Guntur, Hyderabad** (Andhra Pradesh)

---

## 👤 Author

**Satyaprakash**
EV Charging Network Data Platform
Snowflake | AWS S3 | Python | Streamlit | boto3

---

## 📄 License

This project was developed as part of a Hackathon use case.
