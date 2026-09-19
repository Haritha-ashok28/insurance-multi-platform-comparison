# Tests the silver + gold logic that doesn't need Delta (row hashing, dim/fact
# shape, joins). The scd2_merge() function itself needs a real Delta table and
# can't run in this sandbox (no Maven access for the JVM jar) - first real test
# of that is on Databricks/Fabric.
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from shared.local_spark import get_local_spark
from shared.transforms.bronze import clean_policyholders
from shared.transforms.silver import clean_policyholders_silver
from shared.transforms.gold import (
    build_dim_policyholder, build_dim_vehicle, build_fact_claims, build_fact_telematics,
)
from shared.generators.claim_event_generator import generate_claim_events

spark = get_local_spark("gold-logic-test")
spark.sparkContext.setLogLevel("ERROR")

print("=== silver layer on real data ===")
raw = spark.read.csv("data/raw/car_insurance_claim.csv", header=True, inferSchema=True)
bronze = clean_policyholders(raw)
silver = clean_policyholders_silver(bronze)

from pyspark.sql import functions as F

# check: z_ prefix gone
z_leftover = silver.filter(F.col("MSTATUS").rlike("^z_")).count()
print("rows still showing z_ prefix (should be 0):", z_leftover)

# check: no more nulls in imputed cols
nulls_after = silver.filter(F.col("INCOME").isNull() | F.col("AGE").isNull()).count()
print("rows with null INCOME/AGE after impute (should be 0):", nulls_after)
print("rows flagged as imputed (INCOME):", silver.filter("INCOME_IMPUTED = true").count())

print()
print("=== gold dims ===")
dim_policyholder = build_dim_policyholder(silver)
dim_vehicle = build_dim_vehicle(silver)
print("dim_policyholder rows (should be < policy count, person grain):", dim_policyholder.count())
print("dim_vehicle rows (should equal policy count):", dim_vehicle.count())
print("silver policy rows:", silver.count())
dim_policyholder.select("ID", "row_hash").show(3, truncate=30)

print()
print("=== gold facts ===")
claim_events = generate_claim_events(spark, silver)
fact_claims = build_fact_claims(claim_events, dim_vehicle)
print("fact_claims rows:", fact_claims.count())
fact_claims.show(5)

# quick sanity: every fact_claims row should have matched a real POLICY_ID
unmatched = fact_claims.filter(F.col("POLICY_ID").isNull()).count()
print("fact_claims rows with no matching POLICY_ID (should be 0):", unmatched)

spark.stop()
print()
print("GOLD LOGIC TEST DONE - no errors")
