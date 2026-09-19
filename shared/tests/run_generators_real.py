# Runs both generators against the REAL Kaggle CSVs (not mock data).
# Writes synthetic output to data/synthetic/ (gitignored) and prints sanity checks.
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from pyspark.sql import functions as F
from shared.local_spark import get_local_spark
from shared.transforms.bronze import clean_policyholders, parse_telematics_events
from shared.generators.claim_event_generator import generate_claim_events
from shared.generators.telematics_fleet_generator import generate_synthetic_fleet

spark = get_local_spark("real-data-run")
spark.sparkContext.setLogLevel("ERROR")

print("=== loading + cleaning real data ===")
policyholders_raw = spark.read.csv("data/raw/car_insurance_claim.csv", header=True, inferSchema=True)
policyholders = clean_policyholders(policyholders_raw)
print("policyholders:", policyholders.count(), "rows")

telematics_raw = spark.read.csv("data/raw/telematics_data.csv", header=True, inferSchema=True)
telematics = parse_telematics_events(telematics_raw)
print("telematics events (after PID cleanup + POSITION split):", telematics.count(), "rows")
print("distinct PIDs after cleanup:", telematics.select("PID").distinct().count())

print()
print("=== claim event generator on real data ===")
claim_events = generate_claim_events(spark, policyholders)
claim_events.cache()
print("synthetic claim events:", claim_events.count())

# sanity check: total historical $ per POLICY should equal real OLDCLAIM
# (grain is POLICY_ID, not ID - a person can hold multiple policies, see bronze.py)
check = (
    claim_events.filter("claim_type = 'historical'")
    .groupBy("POLICY_ID")
    .agg({"claim_amount": "sum"})
    .withColumnRenamed("sum(claim_amount)", "generated_total")
    .join(policyholders.select("POLICY_ID", "OLDCLAIM"), on="POLICY_ID")
    .withColumn("diff", F.abs(F.col("generated_total") - F.col("OLDCLAIM")))
)
max_diff = check.agg({"diff": "max"}).collect()[0][0]
print("max $ difference between generated total and real OLDCLAIM (should be ~0):", max_diff)

claim_events.write.mode("overwrite").parquet("data/synthetic/claim_events")
print("wrote data/synthetic/claim_events")

print()
print("=== telematics fleet generator on real data ===")
fleet = generate_synthetic_fleet(spark, telematics, n_vehicles=500, readings_per_vehicle=150)
print("synthetic fleet rows:", fleet.count())
print("distinct synthetic devices:", fleet.select("device_id").distinct().count())
fleet.groupBy("alarm_class").count().orderBy("alarm_class").show()
fleet.write.mode("overwrite").parquet("data/synthetic/telematics_fleet")
print("wrote data/synthetic/telematics_fleet")

spark.stop()
print()
print("REAL DATA RUN DONE - no errors")
