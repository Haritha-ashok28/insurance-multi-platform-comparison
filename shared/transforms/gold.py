# Gold layer: star schema. dim_policyholder + dim_vehicle use SCD Type 2
# (MERGE against a Delta table), fact_claims + fact_telematics are the fact
# tables. Shared by Databricks + Fabric (both Spark + Delta).
#
# NOTE: this sandbox can't reach Maven Central, so the actual `DeltaTable.merge`
# call below is untested here - only the row/hash logic feeding it is (see
# shared/tests/test_gold_logic.py). First real test of the MERGE itself has to
# happen on Databricks/Fabric.
from pyspark.sql import DataFrame, functions as F
from delta.tables import DeltaTable

POLICYHOLDER_TRACKED_COLS = [
    "GENDER", "EDUCATION", "OCCUPATION", "INCOME", "HOME_VAL", "MSTATUS",
    "PARENT1", "HOMEKIDS", "KIDSDRIV", "URBANICITY", "YOJ",
]

VEHICLE_TRACKED_COLS = ["CAR_TYPE", "CAR_USE", "CAR_AGE", "BLUEBOOK", "RED_CAR", "TIF", "TRAVTIME"]


def _with_change_hash(df: DataFrame, tracked_cols: list) -> DataFrame:
    # one hash per row of the tracked columns - cheap way to spot changes for SCD2
    return df.withColumn("row_hash", F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in tracked_cols]), 256))


def build_dim_policyholder(silver_df: DataFrame) -> DataFrame:
    # person grain, not policy grain - dedupe by ID since person attributes
    # repeat across a person's multiple policies (see bronze.py POLICY_ID note)
    person_cols = ["ID", "BIRTH_DATE", "AGE"] + POLICYHOLDER_TRACKED_COLS
    dim = silver_df.select(*person_cols).dropDuplicates(["ID"])
    dim = _with_change_hash(dim, POLICYHOLDER_TRACKED_COLS)
    return dim


def build_dim_vehicle(silver_df: DataFrame) -> DataFrame:
    # policy/vehicle grain - one row per POLICY_ID
    vehicle_cols = ["POLICY_ID", "ID"] + VEHICLE_TRACKED_COLS
    dim = silver_df.select(*vehicle_cols)
    dim = _with_change_hash(dim, VEHICLE_TRACKED_COLS)
    return dim


def scd2_merge(spark, delta_table_path: str, new_df: DataFrame, business_key: str):
    """
    Standard SCD2 upsert: close out changed rows (set end_date, is_current=false),
    insert the new version, insert brand new keys. Run this on every load, not
    just the first one - a single first-time load has nothing to compare against
    so everything just inserts as version 1.
    """
    if not DeltaTable.isDeltaTable(spark, delta_table_path):
        # first load - everything is a new "version 1" row
        first_load = (
            new_df.withColumn("effective_start", F.current_timestamp())
            .withColumn("effective_end", F.lit(None).cast("timestamp"))
            .withColumn("is_current", F.lit(True))
            .withColumn("version", F.lit(1))
        )
        first_load.write.format("delta").mode("overwrite").save(delta_table_path)
        return

    target = DeltaTable.forPath(spark, delta_table_path)

    # step 1: close out rows whose hash changed (new version incoming)
    (
        target.alias("t")
        .merge(new_df.alias("s"), f"t.{business_key} = s.{business_key} AND t.is_current = true")
        .whenMatchedUpdate(
            condition="t.row_hash <> s.row_hash",
            set={"is_current": F.lit(False), "effective_end": F.current_timestamp()},
        )
        .execute()
    )

    # step 2: insert new versions for changed rows + brand new keys
    current = target.toDF().filter("is_current = true")
    changed_or_new = new_df.alias("s").join(
        current.alias("t"), on=business_key, how="left_anti"
    )
    to_insert = (
        changed_or_new.withColumn("effective_start", F.current_timestamp())
        .withColumn("effective_end", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
        .withColumn("version", F.lit(1))  # real version increment needs a read of t.version first
    )
    to_insert.write.format("delta").mode("append").save(delta_table_path)


def build_fact_claims(claim_events_df: DataFrame, dim_vehicle: DataFrame) -> DataFrame:
    # join to dim_vehicle for the surrogate key (dim_vehicle grain = POLICY_ID, same as fact)
    fact = claim_events_df.join(
        dim_vehicle.select("POLICY_ID", "ID"), on=["POLICY_ID", "ID"], how="left"
    )
    return fact.select("POLICY_ID", "ID", "claim_id", "claim_date", "claim_amount", "claim_type")


def build_fact_telematics(fleet_df: DataFrame) -> DataFrame:
    # already at the right grain (device_id, timestamp, PID) - no dimension join needed,
    # synthetic devices aren't linked back to a real policyholder in this dataset
    return fleet_df.withColumn("event_date", F.to_date(F.from_unixtime(F.col("timestamp") / 1000)))
