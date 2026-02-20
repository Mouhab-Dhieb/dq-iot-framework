\# DQ IoT Framework (PySpark / Databricks-style)



A config-driven data quality framework for industrial IoT sensor data.



\## Why

Manufacturing / IoT data often contains missing values, duplicates, late/out-of-order events, and sensor spikes/drift.

This project implements an end-to-end approach to:

\- validate data at ingestion time,

\- quarantine invalid records with reason codes,

\- compute KPI-based quality metrics per time window,

\- flag threshold breaches for automation/alerting,

\- produce a quality-gated dataset suitable for downstream analytics and GenAI applications.



\## Outputs (Delta-style tables)

\- `dq\_rules`: rule expectations per tag/sensor (type, range, expected frequency, max lateness)

\- `dq\_quarantine`: invalid rows + `failure\_reason\[]`

\- `dq\_results`: KPIs per 5-min window + `threshold\_breached`

\- `dq\_run\_log`: run metadata (counts, duration, config version)



\## Repo structure

\- `src/dq\_framework/` core framework code (rules loader, checks, KPI computation)

\- `conf/` rule + threshold configuration

\- `notebooks/` Databricks notebooks (bronze → silver → dq)

\- `tests/` unit tests

\- `.github/workflows/ci.yml` CI: lint + tests



\## Quick start (local)

```bash

pip install -r requirements.txt

pytest -q

