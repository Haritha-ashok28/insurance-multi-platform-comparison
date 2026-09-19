"""
Turns each policyholder's claim HISTORY (CLM_FREQ = past claim count, OLDCLAIM =
past claim $ total) into individual dated claim events, plus one "current" event
for policyholders where CLAIM_FLAG=1 (using CLM_AMT as that event's amount).

Assumption to confirm against the real file: CLM_FREQ/OLDCLAIM = past 5 years of
claims, CLM_AMT/CLAIM_FLAG = a separate, more recent claim outcome. Worth a
sanity check once we load the real data (e.g. check for CLM_FREQ=0, CLAIM_FLAG=1 rows).
"""
import dbldatagen as dg
from pyspark.sql import SparkSession, functions as F, Window


def generate_claim_events(spark: SparkSession, policyholders_df, seed: int = 42):
    # only policyholders with past claims
    with_history = policyholders_df.filter(F.col("CLM_FREQ") > 0)

    max_freq = with_history.agg(F.max("CLM_FREQ")).collect()[0][0]
    if not max_freq or max_freq < 1:
        max_freq = 1

    # claim slots 1..max_freq, joined to policyholders below
    slots_df = (
        dg.DataGenerator(spark, name="claim_slots", rows=max_freq, partitions=2, randomSeed=seed)
        .withColumn("slot_no", "int", minValue=1, maxValue=max_freq, uniqueValues=max_freq)
        .build()
    )

    # cross join, keep only slots <= actual claim count
    events = with_history.crossJoin(slots_df).filter(F.col("slot_no") <= F.col("CLM_FREQ"))

    # random skewed weight - claims aren't equal size
    events = events.withColumn("raw_weight", F.pow(F.rand(seed), 2) + 0.05)

    # normalize weights per policyholder, split OLDCLAIM by weight
    per_policy = Window.partitionBy("ID")
    events = events.withColumn("weight_sum", F.sum("raw_weight").over(per_policy))
    events = events.withColumn(
        "claim_amount", F.round(F.col("OLDCLAIM") * F.col("raw_weight") / F.col("weight_sum"), 2)
    )

    # spread claims over past 5 years
    events = events.withColumn("days_ago", (F.rand(seed + 1) * 1825).cast("int"))
    events = events.withColumn("claim_date", F.date_sub(F.current_date(), F.col("days_ago")))
    events = events.withColumn("claim_id", F.concat_ws("-", F.col("ID"), F.col("slot_no")))
    events = events.withColumn("claim_type", F.lit("historical"))

    historical = events.select("ID", "claim_id", "claim_date", "claim_amount", "claim_type")

    # current claim from CLM_AMT, dated within last ~90 days
    current = (
        policyholders_df.filter(F.col("CLAIM_FLAG") == 1)
        .withColumn("claim_id", F.concat_ws("-", F.col("ID"), F.lit("current")))
        .withColumn("claim_date", F.date_sub(F.current_date(), (F.rand(seed + 2) * 90).cast("int")))
        .withColumn("claim_amount", F.round(F.col("CLM_AMT"), 2))
        .withColumn("claim_type", F.lit("current"))
        .select("ID", "claim_id", "claim_date", "claim_amount", "claim_type")
    )

    return historical.unionByName(current)
