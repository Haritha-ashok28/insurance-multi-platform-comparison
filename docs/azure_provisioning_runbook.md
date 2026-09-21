# Azure Provisioning Runbook (Portal)

Covers the shared foundation, then Databricks and Fabric (the 2 platforms
targeted for Monday). Synapse steps are deferred - not needed for the
current target. Region: **Southeast Asia** throughout - originally targeted
East US (cheap, high-capacity), but the subscription hit a capacity
restriction creating a SQL server there ("subscription does not have access
to create a server in the selected region"). This is a common gate on
free/trial subscriptions for high-demand regions, not a mistake - Southeast
Asia is where the subscription actually has quota, so the whole project
standardizes there instead of ending up split across regions. Worth a line
in interview prep: a real constraint the free-tier subscription forced.

Budget: $127 shared credit across Synapse + Databricks. Fabric runs on its
own separate free trial capacity, doesn't touch this budget. Every choice
below (serverless, auto-pause, smallest SKUs) is picking cheap over "best
practice" on purpose - that tradeoff is itself worth a line in your
interview notes.

Portal labels shift slightly between Azure updates, but the flow below
should stay recognizable even if a button moved.

## 1. Resource group

1. portal.azure.com > search bar > "Resource groups" > **+ Create**
2. Subscription: your subscription
3. Resource group name: `rg-project4-insurance`
4. Region: **Southeast Asia**
5. **Review + create** > **Create**
6. Once it's created, open it > **Tags** (left nav) > add:
   - `project` = `project4-insurance`
   - `owner` = `haritha`
   - `budget` = `127usd`

Tagging costs nothing and makes every cost report downstream instantly
filterable - worth doing now while it's one extra click.

## 2. Budget alert (do this before anything else costs money)

1. Search bar > "Cost Management + Billing"
2. Left nav > **Cost Management** > **Budgets**
3. **+ Add** > scope it to `rg-project4-insurance` (so it only tracks this
   project, not your whole subscription)
4. Name: `project4-budget`
5. Reset period: Monthly
6. Amount: `127`
7. Under **Alert conditions**, add 3 alerts at 50%, 80%, and 100% of budget,
   email = your own email at each
8. **Create**

## 3. Key Vault (shared secrets)

1. Search bar > "Key Vault" > **+ Create**
2. Resource group: `rg-project4-insurance`
3. Key vault name: `kv-project4-<add a few random digits>` (must be
   globally unique across all of Azure, so plain `kv-project4` will likely
   be taken)
4. Region: **Southeast Asia**
5. Pricing tier: **Standard**
6. **Review + create** > **Create**

You'll come back here after the SQL Database step to store the connection
string. Never put connection strings in notebooks or the git repo - always
pull from Key Vault at runtime.

## 4. Azure SQL Database (serverless, auto-pause)

This is the shared source both platforms' batch pipelines read from. The
two RAW Kaggle CSVs land here as-is, nothing synthetic (synthetic
generation runs later, on-platform, in the Databricks/Fabric notebooks,
reading from this table).

### 4a. Create the SQL server (the logical container) first

1. Search bar > "SQL servers" > **+ Create**
2. Resource group: `rg-project4-insurance`
3. Server name: `sql-project4-<a few random digits>` (globally unique)
4. Region: **Southeast Asia**
5. Authentication method: **Use SQL authentication**
6. Server admin login: `project4admin`
7. Password: pick a strong one, **save it somewhere** (a password manager,
   not a text file in the repo) - you'll need it for both Key Vault and the
   loading script
8. **Review + create** > **Create**

### 4b. Create the database inside that server

Take the **free offer** if the portal shows it (100,000 vCore-seconds,
32GB data, 32GB backup, free for the subscription's lifetime, one per
subscription) - it already auto-pauses when idle, no manual serverless
config needed, and this project's data (10K + 174K rows) stays well inside
those limits.

1. Search bar > "SQL databases" > **+ Create**
2. Resource group: `rg-project4-insurance`
3. Database name: `project4db` (the portal may pre-fill a random name like
   `free-sql-db-xxxxxxx` - overwrite it, the loading script and connection
   string below both assume `project4db`)
4. Server: the one you just created
5. Leave the free offer applied - **Review + create** > **Create**

(If the free offer isn't offered - e.g. you already have one free database
on this subscription - fall back to manual config: Compute + storage >
Configure database > Service tier **General Purpose** > Compute tier
**Serverless** > Auto-pause delay **60 minutes** > Min vCores **0.5** > Max
vCores **1**.)

### 4c. Firewall - let your machine and Azure services reach it

1. Open the SQL server (not the database) > left nav > **Networking**
2. Under **Firewall rules**, toggle **Allow Azure services and resources to
   access this server** to **Yes** (Databricks and Fabric need this)
3. Click **+ Add your client IPv4 address** (so you can load data from your
   own machine)
4. **Save**

### 4d. Save the connection string to Key Vault

1. Open the SQL **database** (`project4db`) > **Overview** > **Connection
   strings** (top of the page, or left nav) > copy the **ADO.NET** string
2. In that copied string, replace `{your_password}` with your real password
3. Go to your Key Vault > left nav > **Objects** (or **Secrets** directly) >
   **Secrets** > **+ Generate/Import**
4. Name: `sql-connection-string`
5. Value: paste the completed connection string
6. **Create**

### 4e. Load the raw data

This part still runs from your machine (no portal equivalent for "run a
script"). Needs the ODBC Driver 18 for SQL Server installed first:
- Mac: `brew install msodbcsql18 mssql-tools18`
- Linux: see Microsoft's apt/yum instructions for your distro
- Windows: [download the MSI](https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server)

Then, from the repo root:

```bash
source .venv/bin/activate
pip install pyodbc sqlalchemy
export SQL_CONNECTION_STRING="<the connection string you saved to Key Vault>"
python3 scripts/load_to_azure_sql.py
```

That script (already in the repo) loads both raw CSVs as-is - no cleaning,
that happens downstream in each platform's bronze layer, on purpose (bronze
should always mirror the source).

## 5. Databricks workspace + Unity Catalog

### 5a. Create the workspace

1. Search bar > "Azure Databricks" > **+ Create**
2. Resource group: `rg-project4-insurance`
3. Workspace name: `databricks-project4`
4. Region: **Southeast Asia**
5. Pricing tier: **Standard** (not Premium - Premium adds cost for
   RBAC/compliance features this project doesn't need to demonstrate)
6. **Review + create** > **Create** (takes a few minutes)
7. Once done, **Go to resource** > **Launch Workspace**

### 5b. Unity Catalog metastore

1. From inside the workspace, click your name (top right) > **Manage
   Account** - this opens the Databricks **Account Console** (separate from
   the workspace UI)
2. Left nav > **Catalog** > **Create metastore**
3. Region: must match the workspace region (**Southeast Asia**)
4. Name it `project4-metastore`, create an ADLS storage location when
   prompted (or let it create a default one)
5. After creation, **Assign to workspace** > pick `databricks-project4`

### 5c. Cluster

1. Back in the workspace UI > left nav > **Compute** > **Create compute**
2. Single node, smallest available node type (e.g. Standard_DS3_v2)
3. Terminate after **30 minutes** of inactivity - this is the main lever
   against runaway cost, don't skip it
4. **Create compute**

### 5d. Connect the repo

1. Left nav > **Repos** > **Add Repo**
2. Paste your GitHub repo's clone URL
3. Git provider: GitHub (you'll need to link your GitHub account the first
   time, via a personal access token or the OAuth prompt)
4. This clones the whole repo into the workspace - you'll work out of the
   `databricks/` folder inside it

### 5e. Catalog + schema for this project

1. Left nav > **Catalog** > **Create catalog** > name it `project4`
2. Inside it, **Create schema** > name it `insurance`
3. Confirm you're the owner (should be automatic since you created it)

## 6. Microsoft Fabric workspace

Fabric is entirely portal-based - no separate CLI story here anyway.

1. [app.fabric.microsoft.com](https://app.fabric.microsoft.com)
2. Top right, gear icon > **Admin portal** (or the trial banner if one
   shows) > start/confirm your **Fabric trial capacity** if you haven't
   already - this is separate from your $127 Azure credit
3. Left nav > **Workspaces** > **+ New workspace**
4. Name: `project4-insurance`
5. Under **Advanced** > **License mode**, assign it to your **Trial
   capacity**
6. **Apply**
7. Inside the workspace > **Workspace settings** (gear icon) > **Git
   integration** > connect to your GitHub repo, point it at the `fabric/`
   folder
8. Back in the workspace > **+ New item** > **Lakehouse** > name it
   `project4_lakehouse` - this is your Bronze/Silver/Gold OneLake target

## Order to actually do this in

1. Resource group + budget alert (5 min, do this first, no reason to skip)
2. Key Vault (2 min)
3. SQL server + database + firewall + load raw data (15-20 min, mostly
   waiting on provisioning)
4. Databricks workspace + Unity Catalog + cluster + Repos (20-30 min, most
   fiddly step, the metastore step trips people up most often)
5. Fabric workspace (10 min, straightforward)

Ping me once each is up and I'll help wire the actual bronze notebooks
against them.
