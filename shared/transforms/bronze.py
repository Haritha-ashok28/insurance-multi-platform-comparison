# Bronze-layer cleanup shared by Databricks + Fabric (both Spark). Turns the
# raw Kaggle CSVs into clean, typed tables before anything downstream touches them.
from pyspark.sql import DataFrame, functions as F

CURRENCY_COLS = ["INCOME", "HOME_VAL", "BLUEBOOK", "OLDCLAIM", "CLM_AMT"]


def clean_policyholders(df: DataFrame) -> DataFrame:
    # currency columns come in as "$67,349" strings - strip $ and , then cast
    for c in CURRENCY_COLS:
        df = df.withColumn(c, F.regexp_replace(F.col(c), r"[\$,]", "").cast("double"))

    # ID is NOT a unique key - 1,332 policyholders show up on 2+ rows, each with
    # different vehicle/claim data (CAR_TYPE, BLUEBOOK, CLM_FREQ, CLM_AMT all
    # differ), same person-level fields (INCOME, GENDER, etc). Real pattern: one
    # person can hold multiple insured vehicles/policies. So we do NOT dedupe by
    # ID - that would silently drop ~2,000 real policy/claim records. Instead add
    # a surrogate POLICY_ID as the true row grain; ID stays as the person link.
    df = df.withColumn("POLICY_ID", F.monotonically_increasing_id())
    return df


def parse_telematics_events(df: DataFrame) -> DataFrame:
    # drop the raw human-readable "timestamp" string col first - timeMili (ms
    # epoch) becomes the canonical "timestamp", and the name would collide otherwise
    df = df.drop("timestamp")

    # rename to the canonical event schema used everywhere downstream
    df = (
        df.withColumnRenamed("deviceId", "device_id")
        .withColumnRenamed("timeMili", "timestamp")
        .withColumnRenamed("variable", "PID")
        .withColumnRenamed("alarmClass", "alarm_class")
    )

    # bad/unlabeled PID, <0.03% of rows - drop, not worth cleaning
    df = df.filter(F.col("PID") != "PIDNameNotAvailable")

    # POSITION's value is "lat,lon,alt" - split into 3 scalar PID rows so every
    # row in the output has one numeric value, same as every other PID
    position = df.filter(F.col("PID") == "POSITION")
    parts = F.split(F.col("value"), ",")
    position_lat = position.withColumn("PID", F.lit("POSITION_LAT")).withColumn(
        "value", parts.getItem(0).cast("double")
    )
    position_lon = position.withColumn("PID", F.lit("POSITION_LON")).withColumn(
        "value", parts.getItem(1).cast("double")
    )
    position_alt = position.withColumn("PID", F.lit("POSITION_ALT")).withColumn(
        "value", parts.getItem(2).cast("double")
    )

    scalar = df.filter(F.col("PID") != "POSITION").withColumn("value", F.col("value").cast("double"))

    events = scalar.unionByName(position_lat).unionByName(position_lon).unionByName(position_alt)
    events = events.withColumn("timestamp", F.col("timestamp").cast("long"))
    events = events.select("device_id", "timestamp", "PID", "value", "alarm_class")

    # null handling - drop rows where the cast failed (bad source value)
    events = events.dropna(subset=["value"])
    return events
