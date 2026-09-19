# Helper to build a local Spark session for testing pipeline code on a
# laptop/sandbox, before anything touches Databricks or Fabric.
#
# In local mode there's only one JVM (driver + executor together), and it's
# started by py4j before Spark config is read - so spark.driver.extraJavaOptions
# is too late. JAVA_TOOL_OPTIONS is read by the JVM at startup instead, so we
# set that env var before importing pyspark. Needed on Java 17+, which blocks
# the reflective access PyArrow's pandas_udf path uses.
import os

_ADD_OPENS = " ".join(
    f"--add-opens=java.base/{pkg}=ALL-UNNAMED"
    for pkg in [
        "java.lang", "java.lang.invoke", "java.lang.reflect", "java.io",
        "java.net", "java.nio", "java.util", "java.util.concurrent",
        "java.util.concurrent.atomic", "sun.nio.ch", "sun.nio.cs",
        "sun.security.action", "sun.util.calendar",
    ]
)
# netty (used by Arrow's memory manager) also needs this on Java 9+, same reason
_ADD_OPENS += " -Dio.netty.tryReflectionSetAccessible=true"

os.environ["JAVA_TOOL_OPTIONS"] = (os.environ.get("JAVA_TOOL_OPTIONS", "") + " " + _ADD_OPENS).strip()

from pyspark.sql import SparkSession  # noqa: E402 - import after env var is set


def get_local_spark(app_name: str = "project4-local-test") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")  # keep local runs fast
        .getOrCreate()
    )
