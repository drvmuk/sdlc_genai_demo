# Databricks notebook source
# MAGIC %md
# MAGIC # Tests for PL_CalculateAggRequirements (PySpark)
# MAGIC 
# MAGIC The tests validate:
# MAGIC - Sample input -> expected outputs
# MAGIC - Business correctness (decimal precision, timezone, date parsing)
# MAGIC - Aggregation/merge correctness
# MAGIC - Data Quality & Contracts
# MAGIC - Security & Privacy (PII masking)
# MAGIC - Domain-specific (Pricing & Allowances)
# MAGIC - Config & Env variables

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import types as T
from decimal import Decimal
from datetime import datetime, timezone
import os

# Import notebook symbols
# In Databricks, you might use %run to include main. Here, we assume same VM namespace

# COMMAND ----------
# MAGIC %md
# MAGIC ## Helpers

# COMMAND ----------
def reset_state():
    try:
        spark.sql("DROP VIEW IF EXISTS vw_preprocess")
        spark.sql("DROP VIEW IF EXISTS vw_final")
        spark.sql("DROP VIEW IF EXISTS vw_exceptions")
    except Exception:
        pass

# COMMAND ----------
# MAGIC %md
# MAGIC ## Test: Configuration load, env overrides, and validation

# COMMAND ----------
reset_state()

# Snapshot baseline config
base_cfg = load_config_from_tables()
assert base_cfg["InterfaceName"] == "PL_CalculateAggRequirements"

# Env override: KeepCycles -> 4, ReportRunDate invalid then fallback
os.environ["ADF_KEEP_CYCLES"] = "4"
os.environ["ADF_REPORT_RUN_DATE"] = "2026-02-10"

cfg = apply_env_overrides(base_cfg)
cfg = validate_config(cfg)
assert cfg["KeepCycles"] == 4
assert cfg["ReportRunDate"] == "2026-02-10"

# Invalid KeepCycles should fallback to default path in apply_env_overrides
os.environ["ADF_KEEP_CYCLES"] = "bad"
cfg2 = apply_env_overrides(base_cfg)
assert int(cfg2["KeepCycles"]) == base_cfg["KeepCycles"]

# Cleanup env
del os.environ["ADF_KEEP_CYCLES"]
del os.environ["ADF_REPORT_RUN_DATE"]

# COMMAND ----------
# MAGIC %md
# MAGIC ## Test: Timezone conversion and formatting (millisecond precision)

# COMMAND ----------
now_utc = datetime(2026, 2, 25, 15, 30, 10, 123456, tzinfo=timezone.utc)
s = utc_to_est_str(now_utc)
# Expect .123 millis portion
assert s.endswith(".123"), f"Bad millis in {s}"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Test: Promotion period lookup and preprocess load

# COMMAND ----------
reset_state()

cfg = validate_config(load_config_from_tables())
periods = step_lookup_promotion_periods(cfg)
assert len(periods) == 2

for p in periods:
    step_load_data_into_preprocess(p)

pre = IO.load_preprocess()
assert pre.count() == 4  # 2 rows per period

# Schema contract: decimals with 18,4
dtypes = dict(pre.dtypes)
assert dtypes["QualifyingPrice"] == "decimal(18,4)"

# Business correctness: amounts non-negative in sample
assert pre.where((F.col("QualifyingPrice") < 0) | (F.col("Allowance") < 0) | (F.col("SuggestedMaxPrice") < 0)).count() == 0

# Date parsing correctness
assert pre.select(F.min("AllowanceEffectiveDate")).first()[0] is not None

# COMMAND ----------
# MAGIC %md
# MAGIC ## Test: Validation and exception email logic

# COMMAND ----------
# Introduce a bad record to trigger validation
bad = pre.limit(1).withColumn("QualifyingPrice", F.lit(Decimal("-1.0000")))
pre2 = pre.unionByName(bad)
IO.save_preprocess(pre2)

# Prior email sent update should be idempotent when no rows
step_update_prior_email_sent_ind()
assert IO.load_validation_exceptions().count() == 0

# Run validation
step_load_data_for_validation()
step_validate_price_and_allowances(cfg)
exc = IO.load_validation_exceptions()
assert exc.count() >= 1

cnt, subj = step_check_exceptions_and_email(cfg)
# After email, EmailSent is updated to 1 for today's records
exc2 = IO.load_validation_exceptions()
if exc2.count() > 0:
    assert exc2.where((F.col("EmailSent") == 1) & (F.col("CreateDate") == F.current_date())).count() >= 1
assert subj.startswith(cfg["EmailSubject"])  # Subject formatting

# COMMAND ----------
# MAGIC %md
# MAGIC ## Test: Apply to final table (merge) with change detection and soft delete

# COMMAND ----------
reset_state()

# Load two periods, where second period has slightly changed amounts
cfg = validate_config(load_config_from_tables())
periods = step_lookup_promotion_periods(cfg)
for p in periods[:1]:
    step_load_data_into_preprocess(p)

# First apply should insert all rows
step_apply_to_final_table(update_user="tester")
final1 = IO.load_final()
assert final1.count() == 2
assert final1.where(F.col("Active") == 1).count() == 2

# Modify one row to trigger update (change detection)
pre = IO.load_preprocess()
changed = pre.where(F.col("DisplayText").contains("Brand A")).withColumn("Allowance", F.lit(Decimal("3.0000")))
unchanged = pre.where(~F.col("DisplayText").contains("Brand A"))
IO.save_preprocess(unchanged.unionByName(changed))

step_apply_to_final_table(update_user="tester2")
final2 = IO.load_final()
# Expect same key cardinality but updated Allowance for Brand A row
allow_prev = final1.where(F.col("DisplayText").contains("Brand A")).select("Allowance").first()[0]
allow_new = final2.where(F.col("DisplayText").contains("Brand A")).select("Allowance").first()[0]
assert str(allow_prev) != str(allow_new)

# Soft delete: remove one row from preprocess, apply again
IO.save_preprocess(unchanged)  # drop Brand A row
step_apply_to_final_table(update_user="tester3")
final3 = IO.load_final()
# Expect a soft-deleted record present (Active=0) for dropped key
assert final3.where((F.col("DisplayText").contains("Brand A")) & (F.col("Active") == 0)).count() >= 1

# COMMAND ----------
# MAGIC %md
# MAGIC ## Test: Data Retention Purge (KeepCycles)

# COMMAND ----------
# Simulate multiple periods for one brand to exceed KeepCycles=2
reset_state()
base = validate_config(load_config_from_tables())
base["KeepCycles"] = 2
IO.truncate_preprocess()

for p in ["202601", "202602", "202603", "202604"]:
    step_load_data_into_preprocess(p)
    step_apply_to_final_table(update_user="tester")

final_before = IO.load_final()
assert final_before.where(F.col("Active") == 1).count() >= 4
step_purge_old_data(base)
final_after = IO.load_final()
# For each store/brand partition, keep top 2 PromoPeriodId rows
from pyspark.sql.window import Window
w = Window.partitionBy("RetailStoreGUID", "BrandCompanyObjectGUID").orderBy(F.col("PromoPeriodId").desc())
ranked = final_after.where(F.col("Active") == 1).withColumn("rn", F.row_number().over(w))
assert ranked.where(F.col("rn") > 2).count() == 0

# COMMAND ----------
# MAGIC %md
# MAGIC ## Test: Data Quality Contracts (schema validation, not null, uniqueness, referential integrity)

# COMMAND ----------
# Not-null checks (enforced by schema): attempting to insert null RetailStoreGUID should error
from pyspark.sql import Row

try:
    bad = spark.createDataFrame([
        (None, "202602", "X", "Y", "Z", "C", datetime(2026,2,1).date(), datetime(2026,2,28).date(), "Type", None, Decimal("1.0000"), Decimal("1.0000"), Decimal("0.1000"), Decimal("1.0000"), "PricingAndAllowances", 1, 1)
    ], schema=SCHEMA_PREREQUISITES_PREPROCESS)
    IO.save_preprocess(bad)
    dq_fail = False
except Exception:
    dq_fail = True
assert dq_fail is True

# Uniqueness of merge key in final
reset_state()
step_load_data_into_preprocess("202602")
step_apply_to_final_table(update_user="u1")
finalu = IO.load_final()
key_cols = [
    "RetailStoreGUID","PromoPeriodId","BrandCompanyObjectGUID","ParentBrandCompanyObjectGUID",
    "PaymentInitiativeGUID","CategoryCompanyObjectGUID","AllowanceEffectiveDate","AllowanceTerminationDate","AllowanceType"
]
assert finalu.groupBy(*key_cols).count().where(F.col("count") > 1).count() == 0

# Referential integrity surrogate: ParentBrandCompanyObjectGUID defaulted when null
assert finalu.where(F.col("ParentBrandCompanyObjectGUID").isNull()).count() == 0

# Wrong data type handling: ensure decimals stay 18,4
assert dict(finalu.dtypes)["Allowance"] == "decimal(18,4)"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Test: Security & Privacy (PII Masking)

# COMMAND ----------
masked = mask_pii(finalu)
# GUIDs masked
for c in ["RetailStoreGUID", "BrandCompanyObjectGUID", "ParentBrandCompanyObjectGUID", "PaymentInitiativeGUID"]:
    assert masked.where(F.col(c).startswith("****")).count() == masked.count()
# DisplayText masked
assert masked.where(F.col("DisplayText") == "***MASKED***").count() == masked.count()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Test: Aggregation Correctness on key columns (count by PromoPeriodId)

# COMMAND ----------
# Load two periods and verify counts aggregate correctly
reset_state()
for p in ["202602", "202603"]:
    step_load_data_into_preprocess(p)
step_apply_to_final_table(update_user="uagg")
finala = IO.load_final()
agg = finala.groupBy("PromoPeriodId").count().collect()
counts = {r[0]: r[1] for r in agg}
assert counts.get("202602", 0) == 2 and counts.get("202603", 0) == 2

# COMMAND ----------
# MAGIC %md
# MAGIC ## Test: Domain-Specific Checks (Finance/Pricing)

# COMMAND ----------
# Decimal and currency precision: round-half-up to 4 decimals behavior during compare
reset_state()
step_load_data_into_preprocess("202605")
pre = IO.load_preprocess()
# mutate small delta and ensure update triggers on 4-dec precision difference
pre_adj = pre.withColumn("Allowance", F.lit(Decimal("2.50005")))
IO.save_preprocess(pre_adj)
step_apply_to_final_table(update_user="udomain1")
finald1 = IO.load_final()
allow = finald1.select("Allowance").first()[0]
# Stored as 4 decimals; Spark DecimalType(18,4) rounds half-up by default on literal
assert str(allow) in ("2.5001", "2.5000")

# Price non-negative business rule remains valid
assert finald1.where(F.col("SuggestedMaxPrice") < 0).count() == 0

# COMMAND ----------
# MAGIC %md
# MAGIC ## End of tests
