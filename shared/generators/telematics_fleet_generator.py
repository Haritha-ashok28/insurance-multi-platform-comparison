"""
Only 3 real device_id values exist in Kumar's telematics dataset (52 real PIDs,
after POSITION is split into POSITION_LAT/LON/ALT by shared/transforms/bronze.py -
run that first). This expands the 3 devices into a synthetic fleet (~500 vehicles),
using each PID's real value range/mean/stddev and real alarm_class boundaries so
the synthetic readings still behave like real OBD-II telemetry, not random noise.
"""
import dbldatagen as dg
from pyspark.sql import SparkSession, functions as F, Window


def generate_synthetic_fleet(
    spark: SparkSession,
    real_df,
    n_vehicles: int = 500,
    readings_per_vehicle: int = 150,
    seed: int = 42,
):
    # real per-PID stats - mean/stddev/min/max
    pid_stats = real_df.groupBy("PID").agg(
        F.mean("value").alias("mean_val"),
        F.stddev("value").alias("std_val"),
        F.min("value").alias("min_val"),
        F.max("value").alias("max_val"),
    )
    # null stddev handling - happens when a PID has only 1 real value
    pid_stats = pid_stats.fillna({"std_val": 0.01})

    # real alarm_class boundaries per PID
    alarm_bounds = (
        real_df.groupBy("PID", "alarm_class")
        .agg(F.min("value").alias("class_min"), F.max("value").alias("class_max"))
        .withColumnRenamed("PID", "b_PID")
    )

    pids = [row["PID"] for row in real_df.select("PID").distinct().collect()]
    pid_df = spark.createDataFrame([(p,) for p in pids], ["PID"])

    # synthetic device ids: SYN-0001 .. SYN-0500
    # plain Spark, not dbldatagen template= (needs a JVM Arrow feature this JDK lacks)
    devices_df = spark.range(1, n_vehicles + 1).select(
        F.concat(F.lit("SYN-"), F.lpad(F.col("id").cast("string"), 4, "0")).alias("device_id")
    )

    # reading slots, crossed with devices x PIDs below
    readings_df = (
        dg.DataGenerator(spark, name="readings", rows=readings_per_vehicle, partitions=4, randomSeed=seed + 1)
        .withColumn("reading_no", "int", minValue=1, maxValue=readings_per_vehicle, uniqueValues=readings_per_vehicle)
        .build()
    )

    events = devices_df.crossJoin(pid_df).crossJoin(readings_df)
    events = events.join(pid_stats, on="PID", how="left")

    # normal distribution around real mean/stddev, clipped to real min/max
    events = events.withColumn("raw_value", F.col("mean_val") + F.col("std_val") * F.randn(seed + 2))
    events = events.withColumn(
        "value", F.round(F.greatest(F.col("min_val"), F.least(F.col("max_val"), F.col("raw_value"))), 3)
    )

    # timestamps over last 24h, ms epoch - matches real dataset's unit
    events = events.withColumn(
        "timestamp",
        (F.unix_timestamp(F.current_timestamp()) * 1000 - (F.rand(seed + 3) * 86400000).cast("long")),
    )

    # row id before the range join below - real class ranges can overlap, which
    # would otherwise duplicate rows (one row per matching class)
    events = events.withColumn("event_row_id", F.monotonically_increasing_id())

    # alarm_class lookup - match generated value into real per-PID ranges
    events = events.join(
        alarm_bounds,
        on=(events["PID"] == alarm_bounds["b_PID"])
        & (events["value"] >= alarm_bounds["class_min"])
        & (events["value"] <= alarm_bounds["class_max"]),
        how="left",
    )

    # dedup - overlapping ranges can match a row to 2+ classes, keep the highest
    # (higher alarm_class = more specific/severe, real dataset also skews this way)
    per_event = Window.partitionBy("event_row_id").orderBy(F.desc("alarm_class"))
    events = events.withColumn("rn", F.row_number().over(per_event)).filter("rn = 1").drop("rn")

    # null handling - value outside every real range (rare, from clipping), default no alarm
    events = events.fillna({"alarm_class": 0})

    return events.select("device_id", "timestamp", "PID", "value", "alarm_class")
