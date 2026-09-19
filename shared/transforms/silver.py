# Silver layer: clean, conform, dedupe. Takes bronze output and fixes known
# data quality issues in this dataset. Shared by Databricks + Fabric (both Spark).
from pyspark.sql import DataFrame, functions as F

# these categorical columns have a "z_" prefix on one value (SAS reference-
# category artifact, e.g. MSTATUS has "z_No"/"Yes") - strip it everywhere
Z_PREFIX_COLS = ["MSTATUS", "GENDER", "EDUCATION", "OCCUPATION", "CAR_TYPE", "URBANICITY"]

# median-impute these when null, and flag that they were imputed (never impute silently)
NUMERIC_IMPUTE_COLS = ["AGE", "YOJ", "INCOME", "HOME_VAL", "CAR_AGE"]


def clean_policyholders_silver(df: DataFrame) -> DataFrame:
    # strip z_ prefix
    for c in Z_PREFIX_COLS:
        df = df.withColumn(c, F.regexp_replace(F.col(c), r"^z_", ""))

    # case standardization - RED_CAR is lowercase yes/no, rest are Yes/No
    df = df.withColumn("RED_CAR", F.initcap(F.col("RED_CAR")))

    # birth date parse - "16MAR39" -> date
    df = df.withColumn("BIRTH_DATE", F.to_date(F.col("BIRTH"), "ddMMMyy"))

    # CAR_AGE can't be negative (1 known bad row) - treat as null before impute
    df = df.withColumn("CAR_AGE", F.when(F.col("CAR_AGE") < 0, None).otherwise(F.col("CAR_AGE")))

    # null handling - median impute numeric cols, flag which rows were touched
    for c in NUMERIC_IMPUTE_COLS:
        median = df.approxQuantile(c, [0.5], 0.01)[0]
        df = df.withColumn(f"{c}_IMPUTED", F.col(c).isNull())
        df = df.withColumn(c, F.when(F.col(c).isNull(), median).otherwise(F.col(c)))

    # null handling - OCCUPATION is categorical, fill with Unknown not a guess
    df = df.withColumn("OCCUPATION", F.when(F.col("OCCUPATION").isNull(), "Unknown").otherwise(F.col("OCCUPATION")))

    return df
