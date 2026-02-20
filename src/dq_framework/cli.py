from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import SparkSession

from dq_framework.checks import apply_row_level_checks
from dq_framework.rules import load_rules_yaml, rules_to_dicts


def build_spark(app_name: str = "dq-iot-framework") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        # keep it simple for local; Databricks overrides this anyway
        .getOrCreate()
    )


def main() -> int:
    parser = argparse.ArgumentParser(prog="dq-iot", description="Run IoT data-quality checks.")
    parser.add_argument(
        "--input", required=True, help="Input path (parquet/delta) with IoT events."
    )
    parser.add_argument("--rules", default="conf/rules.yml", help="Rules YAML path.")
    parser.add_argument("--out", default="out", help="Output folder.")
    args = parser.parse_args()

    spark = build_spark()

    df = spark.read.format("parquet").load(args.input)

    # Load YAML rules and convert to Spark DataFrame
    rules = load_rules_yaml(args.rules)
    rules_df = spark.createDataFrame(rules_to_dicts(rules))

    checked = apply_row_level_checks(df, rules_df)

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    checked.write.mode("overwrite").parquet(str(out / "dq_checked"))

    print(f"✅ Wrote: {out / 'dq_checked'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
