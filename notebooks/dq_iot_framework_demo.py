# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Framework Demo (IIoT / High-frequency events)
# MAGIC
# MAGIC ## Purpose
# MAGIC This notebook demonstrates a compact, production-style **Data Quality (DQ)** framework for time-series sensor events.
# MAGIC
# MAGIC ## What we build (end-to-end)
# MAGIC 1. **Bronze**: read raw ingested data  
# MAGIC 2. **Silver**: normalize to a long “event stream” format  
# MAGIC 3. **Rules**: define expectations per tag (type, range, max lateness)  
# MAGIC 4. **Checks**: validate each event (null / cast / range / duplicate / too-late)  
# MAGIC 5. **Quarantine**: isolate invalid rows with **reason codes**  
# MAGIC 6. **KPIs**: compute quality metrics per **1-minute** and **5-minute** windows  
# MAGIC 7. **Threshold breaches**: simple alerting logic  
# MAGIC 8. **Executive summary**: single-row output for reporting
# MAGIC
# MAGIC ## Why this matters
# MAGIC In real pipelines, we typically **do not block ingestion** when some data is bad.  
# MAGIC Instead we **quarantine**, **measure**, and **alert** when quality drops.

# COMMAND ----------

# MAGIC %md
# MAGIC # Architecture (Bronze → Silver → DQ → KPIs → Alerts)
# MAGIC
# MAGIC                ┌──────────────────────────┐
# MAGIC                │  Bronze: Raw ingestion   │
# MAGIC                │  dq_demo.bronze_iiot_raw │
# MAGIC                └─────────────┬────────────┘
# MAGIC                              │ (parse timestamp + normalize)
# MAGIC                              ▼
# MAGIC                ┌──────────────────────────┐
# MAGIC                │ Silver: Event stream     │
# MAGIC                │ dq_demo.silver_iiot_events│
# MAGIC                │ (sensor_id, tag, time, value)
# MAGIC                └─────────────┬────────────┘
# MAGIC                              │ (join rules + checks)
# MAGIC                              ▼
# MAGIC                ┌──────────────────────────┐
# MAGIC                │ Rules: Expectations      │
# MAGIC                │ dq_demo.dq_rules         │
# MAGIC                └─────────────┬────────────┘
# MAGIC                              │
# MAGIC                              ▼
# MAGIC                ┌──────────────────────────┐
# MAGIC                │ Quarantine: Bad rows     │
# MAGIC                │ dq_demo.dq_quarantine    │
# MAGIC                │ (reason codes per row)   │
# MAGIC                └─────────────┬────────────┘
# MAGIC                              │ (aggregate)
# MAGIC                              ▼
# MAGIC                ┌──────────────────────────┐
# MAGIC                │ KPIs: 1-min + 5-min      │
# MAGIC                │ dq_demo.dq_kpi_1m / 5m   │
# MAGIC                └─────────────┬────────────┘
# MAGIC                              │ (threshold logic)
# MAGIC                              ▼
# MAGIC                ┌──────────────────────────┐
# MAGIC                │ Alerts (logic/demo)      │
# MAGIC                │ breached windows/tags    │
# MAGIC                └──────────────────────────┘
# MAGIC
# MAGIC The ingestion never stops — we isolate bad data, measure quality continuously,
# MAGIC and trigger alerts when quality drops.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window
import uuid

# -----------------------

SCHEMA = "dq_demo"

RAW_TABLE     = f"{SCHEMA}.bronze_iiot_raw"
SILVER_TABLE  = f"{SCHEMA}.silver_iiot_events"
RULES_TABLE   = f"{SCHEMA}.dq_rules"
QUAR_TABLE    = f"{SCHEMA}.dq_quarantine"
KPI_1M_TABLE  = f"{SCHEMA}.dq_kpi_1m"
KPI_5M_TABLE  = f"{SCHEMA}.dq_kpi_5m"
SUMMARY_TABLE = f"{SCHEMA}.dq_run_summary"

# Timestamp detection
TS_COL_CANDIDATES = ["timestamp", "time", "datetime", "date_time", "event_time"]
TS_PATTERN = "M/d/yyyy H:mm"   # matches the demo (e.g. "1/1/2024 0:00")

# Defaults used when generating rules
DEFAULT_EXPECTED_TYPE = "double"
DEFAULT_EXPECTED_FREQ_SEC = 60      # expected 1 reading/min (demo)
DEFAULT_MAX_LATENESS_SEC = 300      # 5 minutes late allowed

# Unique run id (helps tracking multiple executions)
RUN_ID = str(uuid.uuid4())
print("RUN_ID:", RUN_ID)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Read raw data (Bronze)
# MAGIC
# MAGIC We read the raw ingestion table and detect the timestamp column automatically.

# COMMAND ----------

raw = spark.table(RAW_TABLE)

time_col = None
for c in TS_COL_CANDIDATES:
    if c in raw.columns:
        time_col = c
        break

if time_col is None:
    raise ValueError(f"No timestamp column found. Tried: {TS_COL_CANDIDATES}")

print("timestamp column:", time_col)
display(raw.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Normalize into an event stream (Silver)
# MAGIC
# MAGIC We convert wide sensor columns into a long format:
# MAGIC
# MAGIC - `sensor_id` (machine id)
# MAGIC - `tag` (sensor name)
# MAGIC - `event_time` (timestamp from raw)
# MAGIC - `ingest_time` (when record arrived; in demo initially same as event_time)
# MAGIC - `value` (raw measurement)

# COMMAND ----------

# MAGIC %md
# MAGIC # Parse timestamp safely

# COMMAND ----------

base = raw.withColumn("event_time", F.to_timestamp(F.col(time_col), TS_PATTERN))

bad_ts = base.where(F.col("event_time").isNull()).count()
print("rows with unparseable timestamps:", bad_ts)

display(base.select(time_col, "event_time").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC # Unpivot to long format

# COMMAND ----------

# Identify sensor columns (everything except timestamp + known non-sensor columns if any)
# Treat machine_failure as label, not sensor signal
LABEL_COLS = {"machine_failure"}  # add others if needed (id, target, etc.)

exclude = {time_col, "event_time"} | LABEL_COLS
sensor_cols = [c for c in raw.columns if c not in exclude]

print("sensor cols count:", len(sensor_cols))
print("sensor cols:", sensor_cols)
print("sensor cols count:", len(sensor_cols))
print("sensor cols:", sensor_cols)

# Unpivot using stack
stack_expr = "stack({}, {}) as (tag, value)".format(
    len(sensor_cols),
    ", ".join([f"'{c}', `{c}`" for c in sensor_cols])
)

events = (
    base
    .select("event_time", F.expr(stack_expr))
    .withColumn("sensor_id", F.lit("machine_1"))
    .withColumn("ingest_time", F.col("event_time"))
    .select("sensor_id", "tag", "event_time", "ingest_time", "value")
    .where(F.col("event_time").isNotNull())
)

(events.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TABLE)
)

print("silver rows:", spark.table(SILVER_TABLE).count())
display(spark.table(SILVER_TABLE).limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Inject realistic data issues 
# MAGIC
# MAGIC To prove the framework catches problems, we intentionally inject:
# MAGIC - Missing values
# MAGIC - Out-of-range spikes
# MAGIC - Late arrivals (ingest_time delayed)
# MAGIC - Duplicates
# MAGIC
# MAGIC > Of cource we would **not** do this in production — this is only to create failures in a clean demo dataset.

# COMMAND ----------

events_clean = spark.table(SILVER_TABLE)

# deterministic randomness
e = events_clean.withColumn("r", F.rand(seed=42))

# Missing values (~3%)
e = e.withColumn(
    "value",
    F.when(F.col("r") < 0.03, F.lit(None)).otherwise(F.col("value"))
)

# Out-of-range spikes (~1%) - simple: multiply by 2 for a small slice
e = e.withColumn(
    "value",
    F.when((F.col("r") >= 0.03) & (F.col("r") < 0.04),
           (F.col("value").cast("double") * 2.0)
    ).otherwise(F.col("value"))
)

# Late arrivals (~3%) - delay ingest_time by 10 minutes (exceeds 300 sec)
e = e.withColumn(
    "ingest_time",
    F.when((F.col("r") >= 0.04) & (F.col("r") < 0.07),
           F.col("event_time") + F.expr("INTERVAL 10 MINUTES")
    ).otherwise(F.col("ingest_time"))
)

# Duplicates (~2%) - duplicate a subset
dups = e.where((F.col("r") >= 0.07) & (F.col("r") < 0.09))
e_bad = e.drop("r").unionByName(dups.drop("r"), allowMissingColumns=True)

(e_bad.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TABLE)
)

print("silver rows after injection:", spark.table(SILVER_TABLE).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Build rules table (expectations per tag)
# MAGIC
# MAGIC In this demo, we generate baseline min/max ranges per tag using **p01/p99 percentiles**.
# MAGIC This is a practical first step when engineering specs are not available yet.
# MAGIC
# MAGIC Rule fields:
# MAGIC - `expected_type`
# MAGIC - `min_value`, `max_value`
# MAGIC - `expected_frequency_sec`
# MAGIC - `max_lateness_sec`
# MAGIC - `enabled`, `severity`

# COMMAND ----------

events_for_rules = spark.table(SILVER_TABLE)

stats = (
    events_for_rules
    .where(F.col("value").isNotNull())
    .withColumn("value_double", F.col("value").cast("double"))
    .where(F.col("value_double").isNotNull())
    .groupBy("tag")
    .agg(
        F.expr("percentile_approx(value_double, 0.01)").alias("min_value"),
        F.expr("percentile_approx(value_double, 0.99)").alias("max_value"),
    )
)

dq_rules = (
    stats
    .withColumn("rule_id", F.concat(F.lit("rule_"), F.col("tag")))
    .withColumn("expected_type", F.lit(DEFAULT_EXPECTED_TYPE))
    .withColumn("expected_frequency_sec", F.lit(DEFAULT_EXPECTED_FREQ_SEC))
    .withColumn("max_lateness_sec", F.lit(DEFAULT_MAX_LATENESS_SEC))
    .withColumn("enabled", F.lit(True))
    .withColumn("severity", F.lit("warn"))
    .select(
        "rule_id","tag","expected_type","min_value","max_value",
        "expected_frequency_sec","max_lateness_sec","enabled","severity"
    )
)

(dq_rules.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(RULES_TABLE)
)

print("rules rows:", spark.table(RULES_TABLE).count())
display(spark.table(RULES_TABLE).orderBy("tag"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Apply DQ checks 
# MAGIC
# MAGIC Checks implemented:
# MAGIC - `NULL_VALUE`
# MAGIC - `CAST_FAILED`
# MAGIC - `OUT_OF_RANGE`
# MAGIC - `DUPLICATE` (same sensor_id/tag/event_time; later ingest_time)
# MAGIC - `TOO_LATE` (ingest_time - event_time > max_lateness_sec)
# MAGIC
# MAGIC Output:
# MAGIC - **Quarantine table** `dq_quarantine` = rows with one or more failure reasons
# MAGIC - Each quarantined row includes `failure_reason` as an array of reason codes

# COMMAND ----------

# MAGIC %md
# MAGIC # Join + compute flags + failure reasons

# COMMAND ----------

events_silver = spark.table(SILVER_TABLE)
rules = spark.table(RULES_TABLE).where(F.col("enabled") == True)

dfc = events_silver.join(rules, on="tag", how="left")

# Casting + lateness
dfc = dfc.withColumn("value_double", F.col("value").cast("double"))
dfc = dfc.withColumn("lateness_sec", F.unix_timestamp("ingest_time") - F.unix_timestamp("event_time"))

# Dedup rank
w = Window.partitionBy("sensor_id", "tag", "event_time").orderBy(F.col("ingest_time").asc_nulls_last())
dfc = dfc.withColumn("dup_rank", F.row_number().over(w))

# Flags
dfc = dfc.withColumn("is_null_value", F.col("value").isNull())
dfc = dfc.withColumn("cast_failed", F.col("value").isNotNull() & F.col("value_double").isNull())
dfc = dfc.withColumn("is_duplicate", F.col("dup_rank") > 1)
dfc = dfc.withColumn("too_late", F.col("lateness_sec") > F.col("max_lateness_sec"))

dfc = dfc.withColumn(
    "out_of_range",
    (F.col("value_double").isNotNull()) &
    ((F.col("value_double") < F.col("min_value")) | (F.col("value_double") > F.col("max_value")))
)

# Robust failure_reason array (no null artifacts)
failure_reason = F.expr("""
filter(
  array(
    IF(is_null_value, 'NULL_VALUE', NULL),
    IF(cast_failed,  'CAST_FAILED', NULL),
    IF(out_of_range, 'OUT_OF_RANGE', NULL),
    IF(is_duplicate, 'DUPLICATE', NULL),
    IF(too_late,     'TOO_LATE', NULL)
  ),
  x -> x IS NOT NULL
)
""")

dfc = dfc.withColumn("failure_reason", failure_reason)
dfc = dfc.withColumn("reason_count", F.size("failure_reason"))
dfc = dfc.withColumn("is_valid", F.col("reason_count") == 0)

# COMMAND ----------

# MAGIC %md
# MAGIC #Quarantine write — schema safe

# COMMAND ----------

dq_quarantine = (
    dfc.where(F.col("reason_count") > 0)
       .select(
           F.lit(RUN_ID).alias("run_id"),
           "rule_id","sensor_id","tag","event_time","ingest_time",
           "value","value_double","lateness_sec",
           "failure_reason",
           F.lit("warn").alias("severity")
       )
)

(dq_quarantine.write
    .mode("overwrite")
    .option("overwriteSchema", "true")   # prevents schema mismatch issue
    .saveAsTable(QUAR_TABLE)
)

print("quarantine rows:", spark.table(QUAR_TABLE).count())
display(spark.table(QUAR_TABLE).orderBy(F.desc("event_time")).limit(50))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — KPI tables (1-min + 5-min)
# MAGIC
# MAGIC **two window size**
# MAGIC - **1-minute KPIs** give high resolution: we can see sudden drops immediately (useful for debugging + real-time detection).
# MAGIC - **5-minute KPIs** smooth noise: better for operational monitoring and alerting (fewer false alarms).
# MAGIC
# MAGIC Just like in production, we typically keep:
# MAGIC - a **fine** window for fast diagnosis (1m)
# MAGIC - a **coarser** window for stable alerting (5m / 10m)

# COMMAND ----------

# MAGIC %md
# MAGIC #KPI function

# COMMAND ----------

def compute_kpis(window_size: str, out_table: str):
    events = spark.table(SILVER_TABLE)
    q = spark.table(QUAR_TABLE)

    total = (
        events
        .withColumn("w", F.window("event_time", window_size))
        .groupBy("w", "tag")
        .agg(F.count("*").alias("total_rows"))
    )

    bad = (
        q
        .withColumn("w", F.window("event_time", window_size))
        .groupBy("w", "tag")
        .agg(F.count("*").alias("bad_rows"))
    )

    kpi = (
        total.join(bad, on=["w","tag"], how="left")
             .fillna({"bad_rows": 0})
             .withColumn("dq_pass_rate", (F.col("total_rows") - F.col("bad_rows")) / F.col("total_rows"))
             .select(
                 F.col("w.start").alias("window_start"),
                 F.col("w.end").alias("window_end"),
                 "tag","total_rows","bad_rows","dq_pass_rate"
             )
             .withColumn("run_id", F.lit(RUN_ID))
             .orderBy(F.desc("window_start"), "tag")
    )

    (kpi.write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(out_table)
    )

    return kpi

# COMMAND ----------

# MAGIC %md
# MAGIC # Compute KPI tables

# COMMAND ----------

kpi_1m = compute_kpis("1 minute", KPI_1M_TABLE)
kpi_5m = compute_kpis("5 minutes", KPI_5M_TABLE)

display(spark.table(KPI_1M_TABLE).limit(50))
display(spark.table(KPI_5M_TABLE).limit(50))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 — Threshold breaches (alerting logic)
# MAGIC
# MAGIC We flag windows where quality is below a minimum pass rate
# MAGIC or where the number of bad rows is above a maximum.
# MAGIC
# MAGIC This is intentionally simple:
# MAGIC - In production you would push breaches to an alerts table, monitoring system, or incident workflow.

# COMMAND ----------

MIN_PASS_RATE_5M = 0.98
MAX_BAD_ROWS_5M = 40

kpi5 = spark.table(KPI_5M_TABLE)

breaches = (
    kpi5
    .withColumn("breach_pass_rate", F.col("dq_pass_rate") < F.lit(MIN_PASS_RATE_5M))
    .withColumn("breach_bad_rows", F.col("bad_rows") > F.lit(MAX_BAD_ROWS_5M))
    .withColumn("threshold_breached", F.col("breach_pass_rate") | F.col("breach_bad_rows"))
)

breach_count = breaches.where("threshold_breached").count()
print("breached windows:", breach_count)

display(breaches.where("threshold_breached").orderBy(F.desc("window_start")).limit(50))

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ## Step 8 — Executive summary (single-row table)
# MAGIC
# MAGIC

# COMMAND ----------

events = spark.table(SILVER_TABLE)
q = spark.table(QUAR_TABLE)

total_rows = events.count()
bad_rows = q.count()

reason_counts = (
    q.withColumn("reason", F.explode("failure_reason"))
     .groupBy("reason")
     .count()
)

rc = {r["reason"]: r["count"] for r in reason_counts.collect()}

summary = spark.createDataFrame([{
    "run_id": RUN_ID,
    "total_rows": total_rows,
    "quarantine_rows": bad_rows,
    "dq_pass_rate_overall": (total_rows - bad_rows) / total_rows if total_rows else None,
    "null_value_rows": rc.get("NULL_VALUE", 0),
    "out_of_range_rows": rc.get("OUT_OF_RANGE", 0),
    "duplicate_rows": rc.get("DUPLICATE", 0),
    "too_late_rows": rc.get("TOO_LATE", 0),
}])

(summary.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SUMMARY_TABLE)
)

display(spark.table(SUMMARY_TABLE))

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ## What would be productionized next (Roadmap)
# MAGIC
# MAGIC In a production pipeline, the next upgrades are straightforward:
# MAGIC
# MAGIC ### 1) Streaming + incremental runs
# MAGIC - Replace batch reads with **Auto Loader / Structured Streaming**
# MAGIC - Evaluate DQ checks **incrementally** (new data only)
# MAGIC - Use checkpoints + watermarking for late data
# MAGIC
# MAGIC ### 2) Partitioning + performance
# MAGIC - Partition Silver/Quarantine/KPIs by `event_date = to_date(event_time)`
# MAGIC - Optimize/ZORDER on `(tag, event_time)` for fast time slicing
# MAGIC
# MAGIC ### 3) Rules governance (versioning / SCD)
# MAGIC - Manage rules as a governed table with:
# MAGIC   - rule versioning (SCD Type 2)
# MAGIC   - `effective_from` / `effective_to`
# MAGIC   - approvals + audit trail (who changed what, when)
# MAGIC
# MAGIC ### 4) Alerting integration
# MAGIC - Persist breaches into an alerts table
# MAGIC - Connect to **Databricks SQL Alerts**, Email/Slack/Teams, or incident workflows (Jira)
# MAGIC
# MAGIC ### 5) Monitoring dashboard
# MAGIC A simple dashboard for operators/managers:
# MAGIC - pass rate over time
# MAGIC - top failure reasons
# MAGIC - worst tags in last 24h
# MAGIC - lateness compliance (SLA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final summary (auto-generated)
# MAGIC
# MAGIC We just prints a short executive paragraph based on the latest row in `dq_run_summary`.
# MAGIC It can be pasted directly into an email or report.

# COMMAND ----------

# COMMAND ----------
from pyspark.sql import functions as F

def row_value(r, key, default=0):
    d = r.asDict(recursive=True)
    return d[key] if key in d else default

summary = spark.table(SUMMARY_TABLE).orderBy(F.col("run_id").desc()).limit(1)
row = summary.collect()[0]

run_id = row_value(row, "run_id", "N/A")
total = row_value(row, "total_rows", 0)
qrows = row_value(row, "quarantine_rows", 0)
pass_rate = row_value(row, "dq_pass_rate_overall", None)

nulls = row_value(row, "null_value_rows", 0)
oor   = row_value(row, "out_of_range_rows", 0)
dups  = row_value(row, "duplicate_rows", 0)
late  = row_value(row, "too_late_rows", 0)

def pct(x):
    return round(x * 100, 2) if x is not None else None

if pass_rate is None:
    health = "unknown"
elif pass_rate >= 0.98:
    health = "strong"
elif pass_rate >= 0.95:
    health = "acceptable"
else:
    health = "needs attention"

reasons = [("NULL_VALUE", nulls), ("OUT_OF_RANGE", oor), ("DUPLICATE", dups), ("TOO_LATE", late)]
top_reason = max(reasons, key=lambda x: x[1])[0]

print("-" * 100)
print(f"EXECUTIVE SUMMARY (Run: {run_id})")
print("-" * 100)

print(
    f"In this run, we processed {total:,} sensor events through the IIoT data-quality framework. "
    f"The overall DQ pass rate was {pct(pass_rate)}% ({health}). "
    f"A total of {qrows:,} records were quarantined (bad rows isolated without blocking ingestion). "
    f"The most frequent failure category was {top_reason}. "
    f"Breakdown: NULL_VALUE={nulls:,}, OUT_OF_RANGE={oor:,}, DUPLICATE={dups:,}, TOO_LATE={late:,}. "
    f"We also computed KPIs at 1-minute and 5-minute resolution for monitoring and flagged threshold breaches to demonstrate alerting logic."
)

print("-" * 100)

# COMMAND ----------


from pyspark.sql import functions as F

row = spark.table(SUMMARY_TABLE).orderBy(F.col("run_id").desc()).limit(1).collect()[0]
d = row.asDict(recursive=True)

total = d["total_rows"]
qrows = d["quarantine_rows"]
pass_rate = d["dq_pass_rate_overall"]

# In case some columns aren't present, default to 0 safely
nulls = d["null_value_rows"] if "null_value_rows" in d else 0
oor   = d["out_of_range_rows"] if "out_of_range_rows" in d else 0
dups  = d["duplicate_rows"] if "duplicate_rows" in d else 0
late  = d["too_late_rows"] if "too_late_rows" in d else 0

reasons = [("NULL_VALUE", nulls), ("OUT_OF_RANGE", oor), ("DUPLICATE", dups), ("TOO_LATE", late)]
top_reason = max(reasons, key=lambda x: x[1])[0]

print(f"✅ DQ PASS {pass_rate*100:.2f}% | quarantined {qrows:,}/{total:,} | top issue: {top_reason}")