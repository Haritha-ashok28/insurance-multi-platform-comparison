# Loads the 2 RAW Kaggle CSVs into Azure SQL Database, as-is - no cleaning
# here on purpose, bronze layers downstream should mirror the real source.
# Needs SQL_CONNECTION_STRING env var set (see docs/azure_provisioning_runbook.md).
import os
import sys
import pandas as pd
from sqlalchemy import create_engine

conn_string = os.environ.get("SQL_CONNECTION_STRING")
if not conn_string:
    print("SQL_CONNECTION_STRING env var not set - see docs/azure_provisioning_runbook.md")
    sys.exit(1)

# sqlalchemy wants an odbc-prefixed URL, not the raw ADO.NET-style string
engine_url = f"mssql+pyodbc:///?odbc_connect={conn_string}"
engine = create_engine(engine_url, fast_executemany=True)

print("loading car_insurance_claim.csv ...")
claims = pd.read_csv("data/raw/car_insurance_claim.csv")
claims.to_sql("car_insurance_claim", engine, if_exists="replace", index=False, chunksize=1000)
print(f"  loaded {len(claims)} rows")

print("loading telematics_data.csv ...")
telematics = pd.read_csv("data/raw/telematics_data.csv")
telematics.to_sql("telematics_data", engine, if_exists="replace", index=False, chunksize=1000)
print(f"  loaded {len(telematics)} rows")

print("done")
