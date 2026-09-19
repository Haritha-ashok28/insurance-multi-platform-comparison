# Smoke test only - runs both generators against small mock rows to catch
# bugs before the real Kaggle CSVs are loaded. Not a full correctness test.
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from shared.local_spark import get_local_spark
from shared.generators.claim_event_generator import generate_claim_events
from shared.generators.telematics_fleet_generator import generate_synthetic_fleet
from shared.tests.mock_data import (
    POLICYHOLDER_COLUMNS, MOCK_POLICYHOLDERS, TELEMATICS_COLUMNS, MOCK_TELEMATICS,
)

spark = get_local_spark("smoke-test")
spark.sparkContext.setLogLevel("ERROR")

print("=== claim event generator ===")
policyholders_df = spark.createDataFrame(MOCK_POLICYHOLDERS, POLICYHOLDER_COLUMNS)
claim_events_df = generate_claim_events(spark, policyholders_df)
claim_events_df.orderBy("ID", "claim_type").show(truncate=False)
print("row count:", claim_events_df.count())

# check: total historical $ per policyholder should equal OLDCLAIM (rounding aside)
totals = claim_events_df.filter("claim_type = 'historical'").groupBy("ID").sum("claim_amount")
totals.show()

print("=== telematics fleet generator ===")
telematics_df = spark.createDataFrame(MOCK_TELEMATICS, TELEMATICS_COLUMNS)
fleet_df = generate_synthetic_fleet(spark, telematics_df, n_vehicles=5, readings_per_vehicle=3)
fleet_df.show(20, truncate=False)
print("row count:", fleet_df.count())
print("distinct synthetic devices:", fleet_df.select("device_id").distinct().count())

spark.stop()
print("SMOKE TEST DONE - no errors")
