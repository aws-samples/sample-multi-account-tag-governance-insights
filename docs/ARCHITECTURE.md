# TagInsights Architecture

## Overview

TagInsights is a serverless, multi-account AWS resource tagging governance platform. It uses **AWS Resource Explorer**
as the foundation for tag discovery, enabling scalable tag collection across hundreds of accounts and regions from a
single hub Lambda invocation using the org-wide aggregator view.

---

## Core Design Principles

1. **Resource Explorer org view** — The Worker Lambda queries the org-wide RE aggregator view (`/org/` view ARN) with
   `accountid:X region:Y` filters, scanning all member accounts from the hub account without per-account credentials
2. **Hub-only Worker** — The Aggregator Worker runs only in the hub account; no StackSet deployment to member accounts
   needed for tag scanning
3. **CloudFormation-native** — Fully deployed via CloudFormation, no Python environment needed
4. **Delta-based** — Only changed resources are processed each scan, keeping costs and latency low
5. **Parquet on S3** — All analytics data stored as Parquet for efficient Athena querying
6. **Resumable scanning** — Coordinator tracks progress in `scan-checkpoint.json` and resumes across daily Lambda
   invocations

---

## Component Architecture

```
Hub Account (your-hub-account-id)
│
├── Coordinator Lambda  (ti-orches-ENV-ACCOUNT)
│   ├── Discovers ALL accounts via Organizations API (fallback: RE index list)
│   ├── Builds account+region pairs: ALL org accounts × hub regions from RE indexes
│   ├── Maintains scan-checkpoint.json in S3 for resumable multi-day scans
│   ├── Sequential scan: invokes Worker synchronously, checkpoints every 10
│   └── Fan-out mode: async parallel invocation after MAX_SCAN_DAYS exceeded
│
├── Worker Lambda  (ti-worker-ENV-ACCOUNT)  ← runs in hub account only
│   ├── Discovers org-wide RE view ARN (ARN contains '/org/')
│   ├── Queries RE Search with accountid:{account} region:{region} filter
│   ├── Reads previous state from S3 (latest.json.gz)
│   ├── Computes tag deltas (new/changed/removed)
│   ├── Writes delta JSON to central analytics S3
│   ├── Notifies central SQS queue (notify_results_q) → triggers Analytics Writer
│   └── Writes updated state (compressed JSON) back to S3
│
├── Analytics Writer Lambda  (ti-analytics-ENV-ACCOUNT)
│   ├── Triggered by central SQS queue (ESM)
│   ├── Reads delta JSON files from S3
│   ├── Converts to Parquet, writes to tag-deltas/recent/
│   └── Registers Glue partitions for immediate Athena queryability
│
├── Monthly Aggregator Lambda  (ti-monthly-agg-ENV-ACCOUNT)
│   ├── Triggered: day 2 of each month at 3 AM UTC
│   ├── Reads 90-day-old daily partitions via Athena
│   ├── Writes compressed monthly summaries to tag-deltas/historical/
│   └── Deletes old daily partitions (90x partition reduction)
│
├── Central Analytics S3 Bucket  (ti-central-analytics-REGION-ENV-ACCOUNT)
│   ├── aggregator-state/account_id={account}/region={region}/latest.json.gz
│   ├── aggregator-state/scan-checkpoint.json
│   ├── aggregator-state/completed/{scan_id}.json
│   ├── deltas/account_id={account}/region={region}/{timestamp}-{scan_id}.json
│   ├── tag-deltas/recent/       (Parquet, 90-day rolling, daily partitions)
│   ├── tag-deltas/historical/   (Parquet, aggregated monthly)
│   └── tag-costs/               (Parquet, CUR-matched cost data)
│
├── Glue Database  (ti_REGION_ENV_ACCOUNT)
│   ├── resource_tag_deltas_recent   (external table)
│   ├── resource_tag_deltas_historical (external table)
│   └── tag_costs                    (external table)
│
├── Athena Workgroup  (ti-REGION-ENV-ACCOUNT)
├── Athena Results Bucket  (ti-athena-results-REGION-ENV-ACCOUNT)
├── Central SQS Queue  (ti-central-results-ENV-ACCOUNT)
│
├── Cost Analysis Pipeline (optional)
│   ├── Cost Coordinator Lambda  (ti-cost-orches-ENV-ACCOUNT)
│   │   └── Queries Athena for changed resources → batches to FIFO SQS
│   ├── Cost Worker Lambda  (ti-cost-worker-ENV-ACCOUNT)
│   │   └── Triggered by SQS → matches CUR costs → writes tag-costs/ Parquet
│   └── Cost Deduplicator Lambda  (ti-cost-dedup-ENV-ACCOUNT)
│       └── Triggered by SNS (queue empty alarm) → deduplicates daily → monthly
│
└── Hub Event Bus  (ti-hub-ENV)
    └── Receives forwarded tag change events from member accounts (real-time path)

EventBridge StackSet → member accounts
    └── Event Forwarder Rules (per account × region)
        └── Forwards CloudTrail tag change events → Hub Event Bus → SQS → Worker
```

---

## Data Flow

```
1. DAILY SCHEDULE (EventBridge cron) → Coordinator Lambda
   - Reads/creates scan-checkpoint.json in S3
   - Discovers ALL accounts via Organizations API
   - Builds pairs: all_org_accounts × hub_regions (e.g. 5 accounts × 17 regions = 85 pairs)
   - Sequentially invokes Worker per pair (checkpoints every 10)
   - If MAX_SCAN_DAYS exceeded → parallel fan-out mode
   ↓

2. Coordinator → Worker Lambda (hub account, synchronous invoke per pair)
   - Discovers org-wide RE view ARN (list_views, finds ARN with '/org/')
   - Queries RE: resource-explorer-2:Search with ViewArn=org_view_arn,
     QueryString="accountid:{account} region:{region}"
   - Loads previous state from S3 (latest.json.gz, gzip compressed)
   - Computes deltas: new_resource / tags_changed / resource_removed
   - Writes delta JSON to: deltas/account_id={X}/region={Y}/{ts}-{scan_id}.json
   - Calls notify_results_q() → sends SQS message with {s3_bucket, s3_key, ...}
   - Writes updated state (gzip) to: aggregator-state/account_id={X}/region={Y}/latest.json.gz
   ↓

3. SQS Queue → Analytics Writer Lambda
   - Reads delta JSON file from S3 location in SQS message
   - Converts records to Parquet with snappy compression
   - Writes to: tag-deltas/recent/account_id={X}/year={Y}/month={M}/day={D}/
   - Registers Glue partition for immediate Athena queryability
   ↓

4. Athena Query (on-demand)
   SELECT account_id, resource_type, change_type, COUNT(*) as count
   FROM "ti_REGION_ENV_ACCOUNT".resource_tag_deltas_recent
   WHERE year = YEAR(CURRENT_DATE) AND month = MONTH(CURRENT_DATE)
   GROUP BY 1, 2, 3 ORDER BY count DESC;

5. REAL-TIME PATH (parallel to scan path)
   CloudTrail tag event → EventBridge rule (member account)
   → Hub Event Bus → SQS buffer → Worker Lambda
   → Same delta/state/SQS notification flow as scan path

6. MONTHLY COMPRESSION (day 2 of each month at 3 AM UTC)
   Monthly Aggregator Lambda
   → Queries 90-day-old daily data via Athena
   → Writes monthly Parquet to tag-deltas/historical/
   → Deletes old daily partitions (90x partition count reduction)
```

---

## Deployment Architecture

```
deployment/
├── core-stack.yaml            # S3 buckets, Glue DB + tables, Athena, SQS, EventBus
├── aggregator-stack.yaml      # Coordinator + Worker + Analytics Writer + Monthly Aggregator
├── cost-analysis-stack.yaml   # Optional: CUR-based cost analysis (coordinator + worker + deduplicator)
├── deploy.yaml                # Root orchestration (CoreStack → AggregatorStack → optional CostAnalysisStack)
└── templates/
    └── event-forwarder-stack.yaml  # StackSet template for member account event forwarding
```

**Stack deployment order** (handled automatically by `DependsOn` in `deploy.yaml`):

1. `core-stack` — creates all shared infrastructure (S3, Glue, Athena, SQS, EventBus)
2. `aggregator-stack` — imports from core via `!ImportValue`, creates Lambda functions
3. Optional stack — `cost-analysis` (depends on core)
4. `event-forwarder-stack` — deployed as a CloudFormation StackSet to all member accounts

---

## Scan Lifecycle (Coordinator)

The Coordinator implements a **resumable checkpoint pattern** that handles organisations of any size:

```
Daily EventBridge trigger → Coordinator Lambda
    ↓
Read scan-checkpoint.json from S3
    ├── No checkpoint → start new scan cycle (scan_id = today's date YYYY-MM-DD)
    └── Existing checkpoint → resume in-progress cycle

Discover accounts: Organizations API → RE index list fallback
Build pairs: ALL org accounts × hub regions from RE list_indexes
    (e.g. 5 accounts × 17 regions = 85 account-region pairs)

Check scan age:
    ├── age < MAX_SCAN_DAYS (default 3) → sequential scan
    │       ├── Invoke Worker synchronously per pair
    │       ├── Checkpoint every 10 completions (S3 write)
    │       └── Stop and save checkpoint if Lambda timeout < 2 min
    └── age ≥ MAX_SCAN_DAYS → fan-out mode
            └── Invoke all remaining Workers asynchronously in parallel

On completion:
    ├── Write completion marker: aggregator-state/completed/{scan_id}.json
    └── Delete scan-checkpoint.json
```

---

## Worker: Resource Explorer Org-Wide Query

The Worker Lambda queries the **org-wide Resource Explorer view** from the hub account — no StackSet deployment needed
in member accounts:

```python
# Discover org-wide view (cached per Lambda container lifetime)
def get_org_view_arn() -> Optional[str]:
    re_client = boto3.client('resource-explorer-2', region_name=HUB_REGION)
    response = re_client.list_views()
    for view_arn in response.get('Views', []):
        if '/org/' in view_arn:   # org-scoped view returns all accounts
            return view_arn
    return None  # falls back to default view (hub account only)

# Query scoped to specific account+region
org_view_arn = get_org_view_arn()
search_kwargs = {'QueryString': f"accountid:{account_id} region:{region}"}
if org_view_arn:
    search_kwargs['ViewArn'] = org_view_arn  # must explicitly pass org view

paginator = re_client.get_paginator('search')
for page in paginator.paginate(**search_kwargs):
    for resource in page.get('Resources', []):
        arn = resource.get('Arn')
        tags = {}
        for prop in resource.get('Properties', []):
            if prop.get('Name') == 'tags':
                for tag in prop.get('Data', []):
                    tags[tag['Key']] = tag['Value']
```

Tags are extracted inline from the RE response — no secondary per-resource API calls.

**Prerequisite:** The hub account must have an org-level RE aggregator with an `/org/` view. Member accounts must have
RE local indexes enabled and linked to the aggregator.

---

## Athena Schema

Four tables backed by Parquet on S3:

| Table                            | S3 Prefix                | Partitions             | Retention                             |
|----------------------------------|--------------------------|------------------------|---------------------------------------|
| `resource_tag_deltas_recent`     | `tag-deltas/recent/`     | account/year/month/day | 95 days                               |
| `resource_tag_deltas_historical` | `tag-deltas/historical/` | account/year/month     | Indefinite (IA after 90d)             |
| `tag_costs`                      | `tag-costs/`             | account/year/month     | Per CUR data retention                |

**To run queries after first deployment**, register partitions. Select database `ti_REGION_ENV_ACCOUNT` in the Athena query editor first (these DDL
statements use the selected database; the name is not double-quoted here):

```sql
MSCK REPAIR TABLE resource_tag_deltas_recent;
MSCK REPAIR TABLE resource_tag_deltas_historical;
MSCK REPAIR TABLE tag_costs;
```

---

## Resource Naming Convention

All resources use the `ti-` prefix (short for TagInsights):

| Resource         | Pattern                                   | Example                                            |
|------------------|-------------------------------------------|----------------------------------------------------|
| S3 buckets       | `ti-{component}-{region}-{env}-{account}` | `ti-central-analytics-us-east-1-prod-123456789012` |
| Lambda functions | `ti-{component}-{env}-{account}`          | `ti-orches-prod-123456789012`                      |
| IAM roles        | `ti-{component}-{region}-{env}`           | `ti-worker-us-east-1-prod`                         |
| SQS queues       | `ti-{component}-{env}-{account}`          | `ti-central-results-prod-123456789012`             |
| Glue database    | `ti_{region}_{env}_{account}`              | `ti_us-east-1_prod_123456789012`                    |
| EventBridge bus  | `ti-hub-{env}`                            | `ti-hub-prod`                                      |

---

## Key Technology Choices

| Decision             | Choice                                                      | Rationale                                                                                 |
|----------------------|-------------------------------------------------------------|-------------------------------------------------------------------------------------------|
| Tag discovery        | AWS Resource Explorer org view                              | Single API covers all 300+ resource types across all org accounts; query from hub only    |
| Multi-account access | RE org aggregator view + accountid: filter                  | No cross-account role assumption needed; RE handles authorization                         |
| Scan coordination    | Checkpoint-based sequential scan                            | Resumable across Lambda invocations; handles any number of account+region pairs           |
| Storage format       | Apache Parquet (snappy compressed)                          | 10-100x compression vs JSON, columnar for Athena cost efficiency                          |
| Analytics            | AWS Athena + Glue                                           | Serverless, no cluster management, pay-per-query                                          |
| Deployment           | CloudFormation nested stacks + StackSet for event forwarder | Native multi-account, no additional tooling                                               |
| Cost analysis        | CUR + Athena (primary); Cost Explorer (optional fallback)   | CUR preferred for cost and accuracy; CE disabled by default to prevent unexpected charges |

---

## Related Documents

- [ORCHESTRATION.md](ORCHESTRATION.md) — Lambda data flow diagrams and trigger commands
- [DEPLOYMENT.md](DEPLOYMENT.md) — How to deploy
- [OPERATIONS.md](OPERATIONS.md) — Day-to-day operations
- [cost-analysis/README.md](cost-analysis/README.md) — Cost analysis feature
