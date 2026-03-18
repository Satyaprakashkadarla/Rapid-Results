<div align="center">

# ⚡ ChargeEase EV Charging Network Data Platform

<img src="https://img.shields.io/badge/Python-3.11-3776AB?style=for-the-badge&logo=python&logoColor=white"/>
<img src="https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white"/>
<img src="https://img.shields.io/badge/AWS_S3-FF9900?style=for-the-badge&logo=amazons3&logoColor=white"/>
<img src="https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white"/>
<img src="https://img.shields.io/badge/Plotly-3F4F75?style=for-the-badge&logo=plotly&logoColor=white"/>
<img src="https://img.shields.io/badge/Status-Complete-2ea44f?style=for-the-badge"/>

<br/><br/>

> 🏆 **Hackathon Project** — A complete end-to-end data engineering solution built on **Snowflake** and **AWS S3** for an EV Charging Network operator.

<br/>

![ChargeEase Banner](https://capsule-render.vercel.app/api?type=waving&color=0:29B5E8,100:1D9E75&height=200&section=header&text=ChargeEase%20EV%20Platform&fontSize=40&fontColor=ffffff&animation=fadeIn&fontAlignY=38&desc=Snowflake%20%2B%20AWS%20S3%20%2B%20Python%20%2B%20Streamlit&descAlignY=55&descAlign=50)

</div>

---

## 📌 Business Objective

<table>
<tr>
<td width="60%">

ChargeEase operates **EV charging stations** across Vijayawada, Guntur, and Hyderabad. Their data was completely scattered — no visibility into fraud, uptime, or revenue.

**We built a unified Snowflake-based data platform to:**

- ⚡ Improve charger availability and predictive maintenance
- 🚨 Detect anomalous sessions and energy theft
- 💰 Monitor revenue by tariff, location, and time-of-day
- 🔋 Optimize grid load using demand patterns
- 👥 Enhance EV user engagement

</td>
<td width="40%">

```
📊 Platform Stats
─────────────────────
🏗️  Schemas     : 4
📋  Tables      : 15+
⚙️  Procedures  : 8
📈  KPI Views   : 5
🚨  Fraud Rules : 5
🔄  Tasks       : 3
💻  Dashboard   : 10 pages
```

</td>
</tr>
</table>

---

## 🏗️ System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    SOURCE LAYER                                   │
│   📄 ev_stations  📄 chargers  📄 sessions  📄 users  📄 vehicles │
│              master.csv (10 rows) + inc.csv (4 rows)             │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                    🐍 boto3 API (Python)
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                    ☁️  AWS S3 BUCKET                              │
│         chargeease-ev-data-2024-984543547413-us-east-1-an        │
│   stations/  chargers/  sessions/  users/  vehicles/             │
│   master/    master/    master/    master/  master/               │
│   inc/       inc/       inc/       inc/     inc/                  │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                  🔗 Snowflake External Stage
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                 ❄️  SNOWFLAKE — CHARGEEASE_DB                     │
│                                                                   │
│  📥 RAW Schema                                                    │
│     └── COPY INTO from S3 → EV_STATIONS, CHARGERS, SESSIONS...  │
│                                                                   │
│  ✅ VALIDATED Schema                                              │
│     └── SP_VALIDATE_ALL() → Dedup, UTC, Range, Referential      │
│                                                                   │
│  ⭐ CURATED Schema (Star Schema)                                  │
│     └── FACT_SESSIONS + DIM_STATIONS + DIM_CHARGERS + ...       │
│     └── SCD-2 for Stations, Chargers, Tariffs                   │
│                                                                   │
│  📊 AUDIT Schema                                                  │
│     └── Fraud Alerts · DQ Violations · Lineage · Batch Logs     │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                 🔄 Streams + Tasks (Auto daily 1am UTC)
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│              🖥️  STREAMLIT DASHBOARD (10 Pages)                   │
│   KPI 1-5 · Fraud Alerts · Data Quality · Lineage · AI Chat     │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Tech Stack

<div align="center">

| Technology | Version | Purpose |
|:---:|:---:|:---|
| <img src="https://img.shields.io/badge/Snowflake-29B5E8?logo=snowflake&logoColor=white"/> | Enterprise | Cloud data warehouse — all 4 schemas |
| <img src="https://img.shields.io/badge/AWS_S3-FF9900?logo=amazons3&logoColor=white"/> | us-east-1 | Cloud storage for 10 CSV datasets |
| <img src="https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white"/> | 3.11 | Pipeline scripting and automation |
| <img src="https://img.shields.io/badge/boto3-FF9900?logo=amazonaws&logoColor=white"/> | Latest | S3 bucket creation and file upload API |
| <img src="https://img.shields.io/badge/Streamlit-FF4B4B?logo=streamlit&logoColor=white"/> | Latest | Frontend KPI monitoring dashboard |
| <img src="https://img.shields.io/badge/Plotly-3F4F75?logo=plotly&logoColor=white"/> | Latest | Interactive charts and visualizations |
| <img src="https://img.shields.io/badge/PyCharm-000000?logo=pycharm&logoColor=white"/> | 2024 | IDE for development |

</div>

---

## 📂 Project Structure

```
📁 EV-Charging-Network-Data-Platform/
│
├── 📁 data/                              ← 10 CSV datasets
│   ├── 📄 ev_stations_master.csv         ← 10 records (bulk load)
│   ├── 📄 ev_stations_inc.csv            ← 4 records  (incremental)
│   ├── 📄 chargers_master.csv
│   ├── 📄 chargers_inc.csv
│   ├── 📄 sessions_master.csv
│   ├── 📄 sessions_inc.csv
│   ├── 📄 users_master.csv
│   ├── 📄 users_inc.csv
│   ├── 📄 vehicles_master.csv
│   └── 📄 vehicles_inc.csv
│
├── 📁 sql/
│   └── 📄 FINAL_snowflake_setup.sql      ← Run once in Snowflake Worksheet
│
├── 📁 _upload_to_s3/
│   └── 🐍 upload_to_s3.py                ← Upload CSVs to AWS S3 via boto3
│
├── 📁 snowflake_load/
│   └── 🐍 snowflake_load.py              ← Connect Snowflake + run pipeline
│
├── 📁 streamlit_app/
│   └── 📁 app/
│       └── 🐍 dashboard.py               ← Streamlit KPI dashboard
│
└── 📄 README.md
```

---

## 🚀 Quick Start

### 1️⃣ Install Dependencies
```bash
pip install snowflake-connector-python boto3 streamlit plotly pandas
```

### 2️⃣ Run Snowflake SQL Setup (one time only)
```
Open  →  sql/FINAL_snowflake_setup.sql
Paste →  Snowflake Worksheet
Run   →  Run All (takes 1-2 minutes)
Creates: DB, schemas, tables, procedures, KPI views, security policies
```

### 3️⃣ Upload CSV Files to AWS S3
```bash
python _upload_to_s3/upload_to_s3.py
```
```
✅ [OK] ev_stations_master.csv → s3://chargeease-ev-data-2024/stations/master/
✅ [OK] chargers_master.csv    → s3://chargeease-ev-data-2024/chargers/master/
✅ [OK] sessions_master.csv    → s3://chargeease-ev-data-2024/sessions/master/
...
🎉 ALL 10 FILES UPLOADED SUCCESSFULLY!
```

### 4️⃣ Run Snowflake Pipeline
```bash
python snowflake_load/snowflake_load.py
```
```
========================================
  ChargeEase - Snowflake Pipeline
========================================
PART 1: Connecting to Snowflake    ✅
PART 2: RAW table counts (13 rows) ✅
PART 3: Validate → Merge → Fraud   ✅
PART 4: VALIDATED layer counts     ✅
PART 5: CURATED star schema        ✅
PART 6: All 5 KPI results          ✅
PART 7: Fraud alerts summary       ✅
PART 8: DQ violations + audit      ✅
========================================
```

### 5️⃣ Launch Streamlit Dashboard
```bash
streamlit run streamlit_app/app/dashboard.py
```
> 🌐 Open browser → `http://localhost:8501`

---

## 🗄️ Data Pipeline — 3 Layers

### 📥 RAW Layer
| Feature | Detail |
|---|---|
| Load method | `COPY INTO` from Snowflake External Stage |
| Source | AWS S3 bucket via `@EV_S3_STAGE` |
| Pattern matching | `.*stations/.*[.]csv` per table |
| File tracking | `METADATA$FILENAME` → `_source_file` column |
| Load types | master (bulk 10 rows) + inc (incremental 4 rows) |

### ✅ VALIDATED Layer
`SP_VALIDATE_ALL()` stored procedure applies:

| Check | Detail |
|---|---|
| Deduplication | `QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _load_ts DESC) = 1` |
| UTC normalize | `CURRENT_TIMESTAMP()::TIMESTAMP_NTZ` |
| Geo precision | `ROUND(latitude, 6)`, `ROUND(longitude, 6)` |
| Zone codes | `UPPER(TRIM(zone))` |
| Range checks | `energy_kwh >= 0`, `soc BETWEEN 0 AND 100`, `duration_sec >= 0` |
| Referential | `charger_id IN chargers`, `user_id IN users`, `station_id IN stations` |
| Temporal | `end_time >= start_time` |
| Violations | Logged to `AUDIT.DQ_VIOLATIONS` |

### ⭐ CURATED Layer — Star Schema

```
                    ┌─────────────────┐
                    │  DIM_STATIONS   │ ← SCD-2
                    │  (city, zone,   │
                    │   capacity,     │
                    │   operator)     │
                    └────────┬────────┘
                             │
┌──────────────┐    ┌────────▼────────┐    ┌──────────────────┐
│ DIM_CHARGERS │    │                 │    │   DIM_USERS      │
│ (SCD-2)      │────▶  FACT_SESSIONS  ◀────│   (email masked) │
│ firmware,    │    │                 │    │   (phone masked) │
│ status,      │    │  21 columns     │    └──────────────────┘
│ error_code   │    │  session_id     │
└──────────────┘    │  soc_delta      │    ┌──────────────────┐
                    │  idle_time_sec  │    │  DIM_VEHICLES    │
┌──────────────┐    │  fraud_reason   │    │  (vin masked)    │
│ DIM_TARIFFS  │    │                 │────▶  model, battery │
│ (SCD-2)      │────▶                 │    └──────────────────┘
│ rate_per_kwh │    └─────────────────┘
└──────────────┘
```

---

## 📊 5 KPIs Delivered

<table>
<tr>
<th>KPI</th>
<th>Formula</th>
<th>Business Use</th>
</tr>
<tr>
<td>📊 <b>KPI 1</b><br/>Charger Anomaly Score</td>
<td><code>(Flagged Sessions / Total Sessions) × 100</code></td>
<td>Detect theft, faulty firmware, dangerous patterns</td>
</tr>
<tr>
<td>🔋 <b>KPI 2</b><br/>Charger Uptime Score</td>
<td><code>% chargers with status IN ('Available','Busy')</code></td>
<td>Core SLA metric, predictive maintenance</td>
</tr>
<tr>
<td>👥 <b>KPI 3</b><br/>Active User Ratio</td>
<td><code>COUNT(DISTINCT users with ≥1 session) / total × 100</code></td>
<td>Network engagement and seasonal patterns</td>
</tr>
<tr>
<td>⚡ <b>KPI 4</b><br/>Grid Load Index</td>
<td><code>% stations where peak_load < capacity_kw</code></td>
<td>Grid management with DISCOMs</td>
</tr>
<tr>
<td>💰 <b>KPI 5</b><br/>ARPS by Tariff</td>
<td><code>AVG(price) GROUP BY tariff_type</code></td>
<td>Pricing optimization and revenue insights</td>
</tr>
</table>

---

## 🚨 Fraud Detection — 5 Rules

```
                    FACT_SESSIONS
                         │
          ┌──────────────┼──────────────────┐
          │              │                  │
    ┌─────▼──────┐ ┌─────▼──────┐ ┌────────▼──────┐
    │ SOC_JUMP   │ │  ENERGY    │ │    GHOST      │
    │ SOC > 30%  │ │  OVERFLOW  │ │    SESSION    │
    │ in < 2 min │ │ kWh > max  │ │ dur < 60 sec  │
    └────────────┘ └────────────┘ └───────────────┘
          │              │
    ┌─────▼──────┐ ┌─────▼──────┐
    │ PARALLEL   │ │ EXCESSIVE  │
    │  SESSION   │ │   IDLE     │
    │ 2 places   │ │ > 1 hour   │
    │ same time  │ │ after chg  │
    └────────────┘ └────────────┘
                         │
                  AUDIT.FRAUD_ALERTS
```

| 🚨 Rule | Trigger | Risk Type |
|---|---|---|
| `SOC_JUMP` | Battery SOC rose >30% in <2 minutes | ⚡ Energy theft |
| `ENERGY_OVERFLOW` | Energy > charger max capacity | 🔧 Meter tampering |
| `GHOST_SESSION` | Flagged session lasted <60 seconds | 💳 Billing fraud |
| `PARALLEL_SESSION` | Same user at 2 chargers at once | 👤 Account sharing |
| `EXCESSIVE_IDLE` | Charger blocked >1 hour after charging | ⏱️ Slot blocking |

---

## 🔄 SCD-2 — Historical Tracking

```
DAY 1 — Station_3 loaded (Inactive)
┌────┬────────────┬──────────┬─────────────┬─────────────┬────────────┐
│ SK │ station_id │ status   │ eff_start   │ eff_end     │ is_current │
├────┼────────────┼──────────┼─────────────┼─────────────┼────────────┤
│  1 │     3      │ Inactive │ 2024-01-01  │ 9999-12-31  │    TRUE    │
└────┴────────────┴──────────┴─────────────┴─────────────┴────────────┘

DAY 2 — Station_3 changes to Active (SCD-2 fires)
┌────┬────────────┬──────────┬─────────────┬─────────────┬────────────┐
│ SK │ station_id │ status   │ eff_start   │ eff_end     │ is_current │
├────┼────────────┼──────────┼─────────────┼─────────────┼────────────┤
│  1 │     3      │ Inactive │ 2024-01-01  │ 2024-01-01  │   FALSE ❌ │ ← expired
│  2 │     3      │ Active   │ 2024-01-02  │ 9999-12-31  │   TRUE  ✅ │ ← new
└────┴────────────┴──────────┴─────────────┴─────────────┴────────────┘
```

**Tables with SCD-2:** `DIM_STATIONS` · `DIM_CHARGERS` · `DIM_TARIFFS`

---

## 🔒 Security and Governance

### 🎭 Dynamic Data Masking (PII Protection)

```python
# Non-ADMIN role sees:
email  →  ****@mail.com
phone  →  XXXXXX1234
vin    →  VIN-XXXX-0001

# ACCOUNTADMIN sees:
email  →  user1@mail.com
phone  →  8000000001
vin    →  VIN00001
```

### 🔐 Row Level Security
```sql
-- Users only see data from their own city
CREATE ROW ACCESS POLICY RLS_BY_CITY AS (city STRING) RETURNS BOOLEAN ->
    CURRENT_ROLE() = 'ACCOUNTADMIN' OR city = CURRENT_USER();
```

### 📋 Audit Logs

| Table | Tracks |
|---|---|
| `AUDIT.BATCH_LOAD_LOG` | Session counts per batch file |
| `AUDIT.DQ_VIOLATIONS` | Data quality failures |
| `AUDIT.RULE_HIT_LOG` | Fraud rule hit rates |
| `AUDIT.UPTIME_CHANGE_LOG` | Charger status changes |
| `AUDIT.DATA_LINEAGE` | End-to-end data flow |

### ⚡ Automation
```
STREAM_NEW_SESSIONS    → detects new charging sessions
STREAM_CHARGER_HEALTH  → detects charger status changes
TASK_DAILY_PIPELINE    → runs full pipeline at 1am UTC daily
```

---

## 💻 Streamlit Dashboard

<div align="center">

| Page | Features |
|:---:|:---|
| 🏠 **Overview** | 5 KPI cards + 4 Plotly charts + fraud table |
| 📊 **KPI 1** | Anomaly score bar + scatter by city |
| 🔋 **KPI 2** | Uptime bar + 90% SLA threshold line |
| 👥 **KPI 3** | Active user gauge meter + KYC breakdown |
| ⚡ **KPI 4** | Grid load bar + scatter chart |
| 💰 **KPI 5** | Revenue bar + tariff pie chart |
| 🚨 **Fraud** | Rule charts + detailed table + CSV download |
| ✅ **Quality** | DQ violations + batch load log |
| 🔍 **Lineage** | Layer counts + star schema charts |
| 🤖 **AI Chat** | Smart assistant powered by live Snowflake data |

</div>

---

## 📋 Datasets

<div align="center">

| File | Records | Type | Description |
|---|:---:|:---:|---|
| `ev_stations_master.csv` | 10 | Bulk | Stations in Vijayawada, Guntur, Hyderabad |
| `ev_stations_inc.csv` | 4 | Incremental | Updated station records |
| `chargers_master.csv` | 10 | Bulk | AC/DC chargers with firmware details |
| `chargers_inc.csv` | 4 | Incremental | Updated charger records |
| `sessions_master.csv` | 10 | Bulk | Charging sessions with SOC and energy |
| `sessions_inc.csv` | 4 | Incremental | New charging sessions |
| `users_master.csv` | 10 | Bulk | EV users with KYC status |
| `users_inc.csv` | 4 | Incremental | New/updated users |
| `vehicles_master.csv` | 10 | Bulk | MG ZS EV, Tata Nexon EV, Hyundai Kona |
| `vehicles_inc.csv` | 4 | Incremental | New vehicles |

**Cities:** Vijayawada · Guntur · Hyderabad (Andhra Pradesh, India)

</div>

---

## 👤 Author

<div align="center">

**Satyaprakash**

[![Snowflake](https://img.shields.io/badge/Snowflake-Expert-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)](https://snowflake.com)
[![AWS](https://img.shields.io/badge/AWS-S3-FF9900?style=for-the-badge&logo=amazonaws&logoColor=white)](https://aws.amazon.com)
[![Python](https://img.shields.io/badge/Python-Developer-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)](https://streamlit.io)

*EV Charging Network Data Platform — Hackathon 2024*

</div>

---

## 📄 License

This project was developed as part of a Hackathon use case for the EV Charging Network Data Platform.

---

<div align="center">

![Footer](https://capsule-render.vercel.app/api?type=waving&color=0:1D9E75,100:29B5E8&height=100&section=footer)

**⚡ ChargeEase — Powering EV Intelligence with Data ⚡**

</div>
