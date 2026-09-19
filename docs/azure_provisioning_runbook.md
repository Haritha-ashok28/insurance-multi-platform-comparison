# Azure Provisioning Runbook

Covers the shared foundation, then Databricks and Fabric (the 2 platforms
targeted for Monday). Synapse steps are deferred - not needed for the current
target. Everything below is Azure CLI, copy-paste and run in order.

Budget: $127 shared credit across Synapse + Databricks. Fabric runs on its
own separate free trial capacity, doesn't touch this budget. Every choice
below (serverless, auto-pause, smallest SKUs) is picking cheap over "best
practice" on purpose - that tradeoff is itself worth a line in your
interview notes.

## 0. Prerequisites

```bash
az login
az account set --subscription "<your subscription name or id>"
```

## 1. Resource group + tagging + budget alert

```bash
RG_NAME="rg-project4-insurance"
LOCATION="canadacentral"   # closest region to Toronto

az group create --name $RG_NAME --location $LOCATION \
  --tags project=project4-insurance owner=haritha budget=127usd

# budget alert - fires at 50/80/100% of the $127 credit, emails you
az consumption budget create \
  --budget-name "project4-budget" \
  --amount 127 \
  --category Cost \
  --time-grain Monthly \
  --start-date $(date -u +%Y-%m-01) \
  --end-date $(date -u -d "+3 months" +%Y-%m-01) \
  --resource-group $RG_NAME
```

If `az consumption budget create` isn't available in your CLI version, set
this one up in the portal instead: Cost Management + Billing > Budgets >
Add, scoped to $RG_NAME, $127, alert at 50/80/100%. Don't skip this - it's
the only thing standing between you and an unpleasant surprise.

## 2. Key Vault (shared secrets)

```bash
KV_NAME="kv-project4-$RANDOM"   # must be globally unique
az keyvault create --name $KV_NAME --resource-group $RG_NAME --location $LOCATION \
  --sku standard
```

Secrets go in here as they're created in the steps below (SQL connection
string, Event Hub connection string once that's built). Never put connection
strings in notebooks or the git repo.

## 3. Azure SQL Database (serverless, auto-pause)

This is the shared source both platforms' batch pipelines read from - the
two RAW Kaggle CSVs land here, nothing synthetic (synthetic generation runs
on-platform, in the Databricks/Fabric notebooks, reading from this table).

```bash
SQL_SERVER="sql-project4-$RANDOM"   # must be globally unique
SQL_ADMIN="project4admin"
SQL_PASSWORD="<pick a strong password - save it, you'll need it below>"

az sql server create --name $SQL_SERVER --resource-group $RG_NAME \
  --location $LOCATION --admin-user $SQL_ADMIN --admin-password "$SQL_PASSWORD"

# allow Azure services (Databricks, Fabric) to reach this server
az sql server firewall-rule create --resource-group $RG_NAME --server $SQL_SERVER \
  --name AllowAzureServices --start-ip-address 0.0.0.0 --end-ip-address 0.0.0.0

# your own IP, so you can load data from your machine
MY_IP=$(curl -s ifconfig.me)
az sql server firewall-rule create --resource-group $RG_NAME --server $SQL_SERVER \
  --name AllowMyIP --start-ip-address $MY_IP --end-ip-address $MY_IP

# serverless, auto-pause after 1 hour idle - this is what keeps the $ down
az sql db create --resource-group $RG_NAME --server $SQL_SERVER \
  --name project4db --edition GeneralPurpose --family Gen5 --capacity 1 \
  --compute-model Serverless --auto-pause-delay 60 --min-capacity 0.5
```

Save the connection string to Key Vault:

```bash
CONN_STRING="Server=tcp:${SQL_SERVER}.database.windows.net,1433;Database=project4db;User ID=${SQL_ADMIN};Password=${SQL_PASSWORD};Encrypt=true;"

az keyvault secret set --vault-name $KV_NAME --name "sql-connection-string" --value "$CONN_STRING"
```

### Load the raw data

Needs the ODBC Driver 18 for SQL Server installed locally first:
- Mac: `brew install msodbcsql18 mssql-tools18`
- Linux: see Microsoft's apt/yum instructions for your distro
- Windows: [download the MSI](https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server)

Then, from the repo root:

```bash
source .venv/bin/activate
pip install pyodbc sqlalchemy
export SQL_CONNECTION_STRING="$CONN_STRING"   # the same string saved to Key Vault above
python3 scripts/load_to_azure_sql.py
```

That script (already in the repo) loads both raw CSVs as-is - no cleaning,
that happens downstream in each platform's bronze layer, on purpose (bronze
should always mirror the source).

## 4. Databricks workspace + Unity Catalog

```bash
az extension add --name databricks --upgrade

DATABRICKS_WORKSPACE="databricks-project4"
az databricks workspace create --resource-group $RG_NAME --name $DATABRICKS_WORKSPACE \
  --location $LOCATION --sku standard
```

`standard` SKU, not `premium` - premium adds cost for RBAC/compliance
features this project doesn't need to demonstrate. Unity Catalog metastore
setup and cluster creation are portal steps (Databricks account console,
not `az` CLI) - once the workspace opens:

1. Databricks account console > Catalog > Create metastore (same region as
   the workspace), assign it to this workspace
2. Inside the workspace: Compute > Create cluster - pick the smallest
   node type available (e.g. Standard_DS3_v2), single node, auto-terminate
   after 30 min idle
3. Repos > Add Repo > paste your GitHub repo URL, clone the `databricks/`
   folder scope
4. Catalog > create a catalog + schema for this project (e.g.
   `project4.insurance`), grant yourself owner

## 5. Microsoft Fabric workspace

Fabric runs on its own trial capacity (separate from the $127 budget), set
up entirely in the portal - no CLI for workspace creation:

1. [app.fabric.microsoft.com](https://app.fabric.microsoft.com) > start
   (or confirm) your Fabric trial capacity
2. Workspaces > New workspace > name it `project4-insurance`, assign it to
   the trial capacity
3. Workspace Settings > Git Integration > connect to your GitHub repo,
   folder `fabric/`
4. Create a Lakehouse inside the workspace (this is your Bronze/Silver/Gold
   OneLake target) - name it `project4_lakehouse`

## Order to actually do this in

1. Resource group + budget alert (5 min, do this first, no reason to skip)
2. Key Vault (2 min)
3. SQL Database + load raw data (15-20 min, mostly waiting on provisioning)
4. Databricks workspace + cluster + Unity Catalog (20-30 min, most fiddly step)
5. Fabric workspace (10 min, straightforward)

Ping me once each is up and I'll help wire the actual bronze notebooks
against them.
