# Project 4 - Insurance Multi-Platform Benchmark

Same auto insurance batch + streaming pipeline built on 3 platforms
(Databricks, Fabric, Synapse) to compare tradeoffs.

## Folders

- `databricks/` - Databricks build (Spark)
- `fabric/` - Microsoft Fabric build (Spark)
- `synapse/` - Synapse build (T-SQL, no shared code with the other two)
- `shared/` - transform + generator code reused by Databricks and Fabric
- `docs/` - runbooks and notes

## Local dev setup

```
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Run tests locally before touching any Azure resource. See `shared/tests/`.
