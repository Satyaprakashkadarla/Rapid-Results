import snowflake.connector

# ============================================================
# FILL YOUR SNOWFLAKE PASSWORD ONLY
# ============================================================
SNOWFLAKE_ACCOUNT   = "ACCCOUNT"
SNOWFLAKE_USER      = "USER"
SNOWFLAKE_PASSWORD  = "PASSWORD"
SNOWFLAKE_WAREHOUSE = "WH"
SNOWFLAKE_DATABASE  = "DB"
SNOWFLAKE_SCHEMA    = "SCHEMA"


# ============================================================
# PART 1 - CONNECT TO SNOWFLAKE
# ============================================================
def connect_snowflake():
    print()
    print("=" * 60)
    print("   ChargeEase EV Network - Snowflake Pipeline")
    print("=" * 60)
    print()
    print("=" * 60)
    print("   PART 1: Connecting to Snowflake")
    print("=" * 60)

    conn = snowflake.connector.connect(
        account   = SNOWFLAKE_ACCOUNT,
        user      = SNOWFLAKE_USER,
        password  = SNOWFLAKE_PASSWORD,
        warehouse = SNOWFLAKE_WAREHOUSE,
        database  = SNOWFLAKE_DATABASE,
        schema    = SNOWFLAKE_SCHEMA,
    )

    cur = conn.cursor()
    cur.execute("SELECT CURRENT_USER(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_ROLE()")
    row = cur.fetchone()
    cur.close()

    print(f"   User          : {row[0]}")
    print(f"   Warehouse     : {row[1]}")
    print(f"   Database      : {row[2]}")
    print(f"   Role          : {row[3]}")
    print(f"   Status        : Connected Successfully!")
    return conn


# ============================================================
# PART 2 - CHECK RAW TABLE COUNTS
# ============================================================
def check_raw_counts(conn):
    print()
    print("=" * 60)
    print("   PART 2: RAW Layer - Table Row Counts")
    print("=" * 60)

    cur = conn.cursor()
    tables = ['EV_STATIONS','CHARGERS','SESSIONS','USERS','VEHICLES']
    total  = 0
    for t in tables:
        cur.execute(f"SELECT COUNT(*) FROM CHARGEEASE_DB.RAW.{t}")
        count = cur.fetchone()[0]
        total += count
        status = "OK" if count > 0 else "EMPTY"
        print(f"   {t:<20} rows = {count:<5}  [{status}]")
    print(f"   {'TOTAL':<20} rows = {total}")
    cur.close()


# ============================================================
# PART 3 - RUN PIPELINE
# ============================================================
def run_pipeline(conn):
    print()
    print("=" * 60)
    print("   PART 3: Running Pipeline")
    print("   RAW -> VALIDATED -> CURATED -> FRAUD")
    print("=" * 60)

    steps = [
        ("Validate all tables (DQ)",    "CALL CHARGEEASE_DB.VALIDATED.SP_VALIDATE_ALL()"),
        ("Merge Stations  (SCD-2)",     "CALL CHARGEEASE_DB.CURATED.SP_MERGE_STATIONS()"),
        ("Merge Chargers  (SCD-2)",     "CALL CHARGEEASE_DB.CURATED.SP_MERGE_CHARGERS()"),
        ("Merge Tariffs   (SCD-2)",     "CALL CHARGEEASE_DB.CURATED.SP_MERGE_TARIFFS()"),
        ("Merge Users",                 "CALL CHARGEEASE_DB.CURATED.SP_MERGE_USERS()"),
        ("Merge Vehicles",              "CALL CHARGEEASE_DB.CURATED.SP_MERGE_VEHICLES()"),
        ("Merge Sessions + Fraud tag",  "CALL CHARGEEASE_DB.CURATED.SP_MERGE_SESSIONS()"),
        ("Detect Fraud Alerts",         "CALL CHARGEEASE_DB.AUDIT.SP_DETECT_FRAUD()"),
    ]

    cur = conn.cursor()
    for label, sql in steps:
        print(f"\n   Running  : {label}")
        try:
            cur.execute(sql)
            result = cur.fetchone()[0]
            print(f"   Result   : {result}")
        except Exception as e:
            err = str(e)
            # Duplicate rows means data already exists - not a real error
            if "Duplicate row" in err:
                print(f"   Result   : Already up to date (skipped duplicates)")
            else:
                print(f"   ERROR    : {err}")
    cur.close()
    print()
    print("   Pipeline Complete!")


# ============================================================
# PART 4 - SHOW VALIDATED COUNTS
# ============================================================
def check_validated_counts(conn):
    print()
    print("=" * 60)
    print("   PART 4: VALIDATED Layer - Row Counts")
    print("=" * 60)

    cur = conn.cursor()
    tables = ['EV_STATIONS','CHARGERS','SESSIONS','USERS','VEHICLES']
    for t in tables:
        cur.execute(f"SELECT COUNT(*) FROM CHARGEEASE_DB.VALIDATED.{t}")
        count = cur.fetchone()[0]
        print(f"   {t:<20} rows = {count}")
    cur.close()


# ============================================================
# PART 5 - SHOW CURATED COUNTS
# ============================================================
def check_curated_counts(conn):
    print()
    print("=" * 60)
    print("   PART 5: CURATED Layer - Star Schema Counts")
    print("=" * 60)

    cur = conn.cursor()
    tables = [
        ('DIM_STATIONS',  'CURATED'),
        ('DIM_CHARGERS',  'CURATED'),
        ('DIM_TARIFFS',   'CURATED'),
        ('DIM_USERS',     'CURATED'),
        ('DIM_VEHICLES',  'CURATED'),
        ('FACT_SESSIONS', 'CURATED'),
        ('FRAUD_ALERTS',  'AUDIT'),
    ]
    for t, schema in tables:
        cur.execute(f"SELECT COUNT(*) FROM CHARGEEASE_DB.{schema}.{t}")
        count = cur.fetchone()[0]
        print(f"   {schema}.{t:<25} rows = {count}")
    cur.close()


# ============================================================
# PART 6 - SHOW ALL 5 KPI RESULTS
# ============================================================
def show_kpis(conn):
    print()
    print("=" * 60)
    print("   PART 6: KPI Results")
    print("=" * 60)

    cur = conn.cursor()

    # KPI 1
    print()
    print("   --- KPI 1: Charger Anomaly Score ---")
    print("   Formula: (Flagged Sessions / Total Sessions) x 100")
    cur.execute("""
        SELECT charger_id, city, zone,
               total_sessions, flagged_sessions, anomaly_score_pct
        FROM CHARGEEASE_DB.CURATED.VW_KPI1_ANOMALY_SCORE
        ORDER BY anomaly_score_pct DESC
    """)
    cols = [d[0] for d in cur.description]
    print("   " + " | ".join(f"{c:<18}" for c in cols))
    print("   " + "-" * 60)
    for row in cur.fetchall():
        print("   " + " | ".join(f"{str(v):<18}" for v in row))

    # KPI 2
    print()
    print("   --- KPI 2: Charger Uptime Score ---")
    print("   Formula: % chargers in Available or Busy status")
    cur.execute("""
        SELECT city, zone, total_chargers, up_chargers, uptime_pct
        FROM CHARGEEASE_DB.CURATED.VW_KPI2_UPTIME_SCORE
        ORDER BY uptime_pct DESC
    """)
    cols = [d[0] for d in cur.description]
    print("   " + " | ".join(f"{c:<18}" for c in cols))
    print("   " + "-" * 60)
    for row in cur.fetchall():
        print("   " + " | ".join(f"{str(v):<18}" for v in row))

    # KPI 3 - Fixed: use full date range since data is from 2024
    print()
    print("   --- KPI 3: Active User Charging Ratio ---")
    print("   Formula: % users with >= 1 session")
    cur.execute("""
        SELECT
            (SELECT COUNT(*) FROM CHARGEEASE_DB.CURATED.DIM_USERS) AS total_registered,
            COUNT(DISTINCT user_id) AS active_users,
            ROUND(COUNT(DISTINCT user_id)*100.0 /
                  NULLIF((SELECT COUNT(*) FROM CHARGEEASE_DB.CURATED.DIM_USERS),0),2)
                  AS active_ratio_pct
        FROM CHARGEEASE_DB.CURATED.FACT_SESSIONS
    """)
    cols = [d[0] for d in cur.description]
    print("   " + " | ".join(f"{c:<22}" for c in cols))
    print("   " + "-" * 60)
    for row in cur.fetchall():
        print("   " + " | ".join(f"{str(v):<22}" for v in row))

    # KPI 4
    print()
    print("   --- KPI 4: Grid Load Distribution Index ---")
    print("   Formula: % stations with peak load < capacity")
    cur.execute("""
        SELECT city, zone, total_stations, balanced_stations, grid_load_index
        FROM CHARGEEASE_DB.CURATED.VW_KPI4_GRID_LOAD
        ORDER BY grid_load_index DESC
    """)
    cols = [d[0] for d in cur.description]
    print("   " + " | ".join(f"{c:<18}" for c in cols))
    print("   " + "-" * 60)
    rows = cur.fetchall()
    if not rows:
        print("   (no data)")
    for row in rows:
        print("   " + " | ".join(f"{str(v):<18}" for v in row))

    # KPI 5
    print()
    print("   --- KPI 5: Average Revenue Per Session (ARPS) ---")
    print("   Formula: Avg revenue grouped by tariff type")
    cur.execute("""
        SELECT tariff_type, total_sessions, total_revenue, avg_revenue_per_session
        FROM CHARGEEASE_DB.CURATED.VW_KPI5_ARPS
    """)
    cols = [d[0] for d in cur.description]
    print("   " + " | ".join(f"{c:<25}" for c in cols))
    print("   " + "-" * 60)
    for row in cur.fetchall():
        print("   " + " | ".join(f"{str(v):<25}" for v in row))

    cur.close()


# ============================================================
# PART 7 - FRAUD ALERTS SUMMARY
# ============================================================
def show_fraud(conn):
    print()
    print("=" * 60)
    print("   PART 7: Fraud Alerts Summary")
    print("=" * 60)

    cur = conn.cursor()

    print()
    print("   --- Fraud Rules Hit ---")
    cur.execute("""
        SELECT fraud_rule, COUNT(*) AS total_alerts
        FROM CHARGEEASE_DB.AUDIT.FRAUD_ALERTS
        GROUP BY fraud_rule
        ORDER BY total_alerts DESC
    """)
    cols = [d[0] for d in cur.description]
    print("   " + " | ".join(f"{c:<25}" for c in cols))
    print("   " + "-" * 50)
    rows = cur.fetchall()
    if not rows:
        print("   (no fraud alerts)")
    for row in rows:
        print("   " + " | ".join(f"{str(v):<25}" for v in row))

    print()
    print("   --- All Fraud Alert Details ---")
    cur.execute("""
        SELECT session_id, user_id, charger_id, fraud_rule, detail
        FROM CHARGEEASE_DB.AUDIT.FRAUD_ALERTS
        ORDER BY alerted_at DESC
        LIMIT 20
    """)
    cols = [d[0] for d in cur.description]
    print("   " + " | ".join(f"{c:<20}" for c in cols))
    print("   " + "-" * 60)
    rows = cur.fetchall()
    if not rows:
        print("   (no fraud alerts)")
    for row in rows:
        print("   " + " | ".join(f"{str(v):<20}" for v in row))

    cur.close()


# ============================================================
# PART 8 - DQ VIOLATIONS + AUDIT LOGS
# ============================================================
def show_audit(conn):
    print()
    print("=" * 60)
    print("   PART 8: Data Quality + Audit Logs")
    print("=" * 60)

    cur = conn.cursor()

    print()
    print("   --- Batch Load Log ---")
    cur.execute("""
        SELECT table_name, source_file, rows_loaded, loaded_at
        FROM CHARGEEASE_DB.AUDIT.BATCH_LOAD_LOG
        ORDER BY loaded_at DESC
        LIMIT 10
    """)
    rows = cur.fetchall()
    if not rows:
        print("   (no batch log)")
    else:
        cols = [d[0] for d in cur.description]
        print("   " + " | ".join(f"{c:<20}" for c in cols))
        print("   " + "-" * 60)
        for row in rows:
            print("   " + " | ".join(f"{str(v):<20}" for v in row))

    print()
    print("   --- DQ Violations ---")
    cur.execute("""
        SELECT table_name, violation_type, COUNT(*) AS cnt
        FROM CHARGEEASE_DB.AUDIT.DQ_VIOLATIONS
        GROUP BY table_name, violation_type
    """)
    rows = cur.fetchall()
    if not rows:
        print("   No DQ violations found - data is clean!")
    else:
        cols = [d[0] for d in cur.description]
        print("   " + " | ".join(f"{c:<20}" for c in cols))
        for row in rows:
            print("   " + " | ".join(f"{str(v):<20}" for v in row))

    cur.close()


# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    try:
        conn = connect_snowflake()
        check_raw_counts(conn)
        run_pipeline(conn)
        check_validated_counts(conn)
        check_curated_counts(conn)
        show_kpis(conn)
        show_fraud(conn)
        show_audit(conn)
        conn.close()

        print()
        print("=" * 60)
        print("   ALL DONE! ChargeEase Pipeline Complete!")
        print()
        print("   Next step - Launch Streamlit Dashboard:")
        print("   streamlit run streamlit_app/dashboard.py")
        print("=" * 60)

    except Exception as e:
        print(f"\n   ERROR: {e}")
        raise