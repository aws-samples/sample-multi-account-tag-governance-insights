# TagInsights Software Design Document

**Version:** 4.1 (Hub-Only Multi-Account Scanning via Resource Explorer Org View)
**Last Updated:** June 2026

---

## 1. Purpose

TagInsights provides AWS resource tagging governance at scale. It collects tag data from all resources across all
accounts and regions in an AWS Organization, stores it as queryable analytics data, and enables cost attribution and
tagging gap analysis.

---

## 2. System Overview

```
┌─────────────────────────────────────────────────────────────┐
│                      Hub Account                             │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Coordinator Lambda                                  │   │
│  │  • Checkpoint-based sequential scan                  │   │
│  │  • Discovers accounts via Organizations / RE indexes │   │
│  │  • Resumes across daily Lambda invocations           │   │
│  │  • Fan-out when scan falls behind (MAX_SCAN_DAYS)    │   │
│  └──────────────────────────────────────────────────────┘   │
│         ↓ Invokes Worker per account+region                  │
└─────────────────────────────────────────────────────────────┘
                      ↓
┌─────────────────────────────────────────────────────────────┐
│                      Hub Account                             │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Worker Lambda                                       │   │
│  │  • Queries RE org-wide view (accountid: + region:)   │   │
│  │  • Delta detection vs previous state (S3)            │   │
│  │  • Writes delta JSON + updated state to central S3   │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                      ↓
┌─────────────────────────────────────────────────────────────┐
│                      Hub Account                             │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Central Analytics S3 Bucket                         │   │
│  │  Analytics Writer Lambda (SQS-triggered)             │   │
│  │  Glue Database + Athena Workgroup                    │   │
│  │  Monthly Aggregator Lambda                           │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

---

## 3. Components

### 3.1 Coordinator Lambda

**File:** `src/lambda_handlers/aggregator_coordinator.py`

Manages the full scan lifecycle across all accounts and regions. Runs daily in the hub account.

**Algorithm:**

1. Read `aggregator-state/scan-checkpoint.json` from S3; start new cycle if none exists (`scan_id` = today's date)
2. Discover accounts: try `organizations:ListAccounts` first; fall back to extracting account IDs from RE
   `list_indexes()` ARNs
3. Build account+region pairs: call `resource-explorer-2:ListIndexes` — each local index represents an account+region
   with discoverable resources
4. **Sequential mode** (default): invoke Worker synchronously per pair; save checkpoint every 10 completions; stop and
   checkpoint if approaching Lambda timeout (2-minute safety buffer)
5. **Fan-out mode** (if scan age ≥ `MAX_SCAN_DAYS`): invoke all remaining Workers asynchronously in parallel (brief
   pause every 50 invocations to avoid Lambda invoke throttling)
6. On full completion: write completion marker to `aggregator-state/completed/{scan_id}.json`, delete checkpoint

**Key environment variables:**

- `CENTRAL_ANALYTICS_BUCKET` — S3 bucket for state and deltas
- `WORKER_FUNCTION_NAME` — Worker Lambda function name to invoke
- `HUB_REGION` — Hub region for RE index queries and Lambda invocations
- `MAX_SCAN_DAYS` — Days before switching sequential scan to fan-out (default: 3)

### 3.2 Worker Lambda

**File:** `src/lambda_handlers/aggregator_worker.py`

The primary data collection component. Invoked by Coordinator (scan path) or EventBridge CloudTrail rules (real-time
path).

**Algorithm (scan path):**

1. Call `resource-explorer-2:Search` directly via boto3 with `accountid:{account} region:{region}` query
2. Extract tags inline from `Properties[name=tags].Data` — no secondary per-resource API calls
3. Load previous state from `aggregator-state/account_id={account}/region={region}/latest.json.gz` (empty dict on first
   scan)
4. Compute delta: new resources, changed tags, removed resources
5. Write delta JSON to `deltas/account_id={account}/region={region}/{timestamp}-{scan_id}.json`
6. Write updated state (compressed JSON) to `latest.json.gz`
7. **Rate-limit SQS notifications** — per-account sliding window (20 msgs/min); rate-limited deltas stay in S3

**Algorithm (real-time path):**
EventBridge CloudTrail rule detects tag operations (TagResource, CreateTags, etc.); Worker extracts ARNs from the event
payload, updates only the affected resources in state.

**Key environment variables:**

- `CENTRAL_ANALYTICS_BUCKET` — Central S3 bucket for state and deltas
- `HUB_REGION` — Region of the RE aggregator index
- `STATE_SHARD_THRESHOLD` — Warn if state file exceeds this many resources (default: 50,000)
- `MAX_SQS_MESSAGES_PER_ACCOUNT_PER_MINUTE` — SQS rate limit (default: 20)

### 3.3 Analytics Writer Lambda

**File:** `src/lambda_handlers/analytics_writer_handler.py`

Hub-side Lambda that processes delta files into Athena-queryable Parquet.

**Trigger:** Central SQS queue (event source mapping)

**Algorithm:**

1. Read SQS message containing S3 delta file location
2. **Validate delta JSON** — JSON schema (`src/schemas/delta_schema.json`), 10MB size limit; quarantine rejected files
   to `quarantine/` prefix
3. Read delta JSON from central analytics S3 bucket
4. Write Parquet (SNAPPY-compressed) to `tag-deltas/recent/account_id=X/year=Y/month=M/day=D/`
5. Use SHA-256 content hash as filename for idempotent deduplication
6. Register Glue partitions via `BatchCreatePartition`
7. Handle S3 eventual consistency via progressive SQS re-queue (5 min → 10 min → 15 min)

### 3.4 Monthly Aggregator Lambda

**File:** `src/lambda_handlers/monthly_aggregator_handler.py`

Compresses 90+ days of daily delta records into monthly aggregates.

**Trigger:** EventBridge cron — `0 3 2 * ? *` (3 AM day 2 of each month)

**Algorithm:**

1. Discover accounts with data for the target month (S3 listing; Athena fallback)
2. Query `resource_tag_deltas_recent` for previous month; group by ARN → compute `first_seen`, `last_seen`,
   `change_count`
3. Write aggregated Parquet to `tag-deltas/historical/account_id=X/year=Y/month=M/`
4. Delete aggregated daily Glue partitions and S3 data

### 3.5 Cost Analysis Lambdas

**Files:** `src/lambda_handlers/cost/` (deployed from `deployment/cost-analysis-stack.yaml`)  
**Source library:** `src/lambda_handlers/cost/cur_direct_matcher.py`, `cur_provider.py`, `allocation_manager.py`

Matches AWS resource ARNs directly against CUR (Cost & Usage Report) line items via Athena.

**Flow:**

1. **Coordinator** (every 12h): reads ARNs from tag delta data, dispatches batches to FIFO SQS work queue
2. **Worker** (SQS-triggered, max 50 concurrent): normalizes ARNs to CUR `line_item_resource_id` format (
   service-specific per `CURDirectMatcher`), queries CUR via Athena (`CURCostProvider`), writes Parquet to `tag-costs/`
3. **Deduplicator** (SNS-triggered when queue empties): removes duplicate records using Athena CTAS

**v2 key differences:**

- Work queue is **FIFO** with content-based deduplication
- Deduplication triggered via **SNS topic** (not CloudWatch Alarm direct invocation)
- **`EnableCostExplorerFallback=false`** by default — prevents unexpected CE API charges
- **`CurAccountRoleArn`** parameter for cross-account CUR access via STS AssumeRole
- `analytics_table_reader.py`, `resource_cost_analyzer.py`, `smart_cost_provider.py` removed from source;
  `CURDirectMatcher` called directly

**Prerequisite:** CUR must be enabled in AWS Billing with resource IDs and Athena integration.

## 4. Data Model

### 4.1 Tag Delta Record (resource_tag_deltas_recent)

`resource_tag_deltas_historical` has the same columns except it replaces `timestamp`, `scan_id`, and the `day`
partition with `first_seen_timestamp`, `last_seen_timestamp`, and `change_count`.

| Field                    | Type               | Description                                            |
|--------------------------|--------------------|--------------------------------------------------------|
| `arn`                    | STRING             | AWS resource ARN                                       |
| `resource_type`          | STRING             | e.g., `ec2` (extracted from ARN)                       |
| `tags`                   | MAP<STRING,STRING> | Current tags                                           |
| `previous_tags`          | MAP<STRING,STRING> | Tags from previous scan                                |
| `change_type`            | STRING             | `new_resource` / `tags_changed` / `resource_removed`   |
| `timestamp`              | BIGINT             | Unix epoch seconds                                     |
| `source_system`          | STRING             | `aggregator_worker` / `aggregator_worker_realtime`     |
| `region`                 | STRING             | AWS region                                             |
| `scan_id`                | STRING             | Scan run identifier (date string or event name)        |
| `account_id`             | STRING             | AWS account ID (partition key)                         |
| `year` / `month` / `day` | INT                | Partition keys                                         |

### 4.2 Tag Cost Record (tag_costs)

One row per tag on each costed resource; resource-level cost columns repeat on each tag row.

| Field                     | Type      | Description                                   |
|---------------------------|-----------|-----------------------------------------------|
| `tag_key` / `tag_value`   | STRING    | One tag on the resource                       |
| `is_cost_allocation_tag`  | BOOLEAN   | Whether the key is an active cost allocation tag |
| `resource_arn`            | STRING    | AWS resource ARN                              |
| `resource_type`           | STRING    | Resource type                                 |
| `region`                  | STRING    | AWS region                                    |
| `last_month_cost`         | DOUBLE    | Previous month cost (USD); not populated yet (0) |
| `current_month_mtd`       | DOUBLE    | Current month-to-date cost (USD)              |
| `current_month_projected` | DOUBLE    | `avg_daily_cost` × days in month (USD)        |
| `avg_daily_cost`          | DOUBLE    | `current_month_mtd` / day of month (USD)      |
| `analysis_date`           | DATE      | Cost Analysis run date                        |
| `last_updated`            | TIMESTAMP | Write time                                    |
| `account_id`              | STRING    | Partition key                                 |
| `year` / `month`          | INT       | Partition keys                                |

------------------|---------|----------------------------------|
| `resource_arn`   | STRING  | AWS resource ARN                 |
| `resource_type`  | STRING  | Resource type                    |
| `region`         | STRING  | AWS region                       |
| `monthly_cost`   | DOUBLE  | Monthly cost (USD) from CUR      |
| `usage_records`  | INT     | Number of CUR line items matched |
| `avg_daily_cost` | DOUBLE  | Average daily cost               |
| `matched_in_cur` | BOOLEAN | Whether ARN matched in CUR       |
| `normalized_arn` | STRING  | ARN format used for CUR matching |
| `account_id`     | STRING  | Partition key                    |
| `year` / `month` | INT     | Partition keys                   |

---

## 5. Deployment Design

### 5.1 CloudFormation Templates

| Template                                          | Purpose                                                                     |
|---------------------------------------------------|-----------------------------------------------------------------------------|
| `deployment/core-stack.yaml`                      | Hub infrastructure (S3, Glue, Athena, SQS, EventBus, DLQs)                  |
| `deployment/aggregator-stack.yaml`                | Coordinator + Worker + Analytics Writer + Monthly Aggregator Lambdas        |
| `deployment/cost-analysis-stack.yaml`             | Optional: Cost Coordinator + Worker + Deduplicator + FIFO SQS + SNS trigger |
| `deployment/deploy.yaml`                          | Root deployment orchestration                                               |
| `deployment/templates/event-forwarder-stack.yaml` | StackSet template for member account CloudTrail event forwarding            |

### 5.2 Key Parameters

| Parameter                    | Default         | Description                                                     |
|------------------------------|-----------------|-----------------------------------------------------------------|
| `DeploymentMode`             | `SingleAccount` | `SingleAccount` / `Organization` / `SpecificAccounts`           |
| `HubAccountId`               | —               | **Required for multi-account** — 12-digit account ID            |
| `OrganizationId`             | —               | Required for Organization/SpecificAccounts modes                |
| `DeploymentRegions`          | `us-east-1`     | Regions to deploy workers to                                    |
| `EnableCostAnalysis`         | `false`         | Enables CUR cost matching                                       |
| `CurDatabaseName`            | `""`            | Glue database name for CUR table                                |
| `CurTableName`               | `""`            | Glue table name for CUR data                                    |
| `CurAccountRoleArn`          | `""`            | IAM role ARN for cross-account CUR access                       |
| `EnableCostExplorerFallback` | `false`         | Opt-in CE fallback when CUR unavailable (charges $0.01/request) |

---

## 6. Source Code Structure

```
src/
├── lambda_handlers/
│   ├── aggregator_coordinator.py    # Checkpoint-based multi-account scan coordinator (HMAC checkpoint signing M8)
│   ├── aggregator_worker.py         # RE direct query + delta computation (SQS rate limiting M5)
│   ├── analytics_writer_handler.py  # SQS → Parquet writer + input validation
│   ├── monthly_aggregator_handler.py
│   └── cost/
│       ├── allocation_manager.py    # Cost allocation tag management + CE fallback
│       ├── cur_direct_matcher.py    # ARN → CUR line_item_resource_id normalization
│       ├── cur_provider.py          # Athena-based CUR query execution (cross-account support)
│       ├── query_builder.py
│       ├── schema_manager.py
│       ├── athena_query_executor.py
│       ├── parquet_writer.py
│       ├── s3_cost_writer.py
│       └── rate_limiter.py
├── schemas/
│   └── delta_schema.json            # JSON schema for delta file validation
├── tag_manager.py                   # Tag retrieval orchestration (library use)
├── tag_retrievers/
│   ├── resource_explorer.py         # RE retriever
│   ├── resource_groups.py           # ResourceGroups Tagging API fallback
│   ├── registry.py
│   ├── multi_region_registry.py
│   └── base.py
└── utils/
    ├── arn_parser.py
    ├── resource_naming.py
    └── logging_config.py

deployment/
├── core-stack.yaml
├── aggregator-stack.yaml
├── cost-analysis-stack.yaml
├── deploy.yaml
├── package_lambdas.py               # Builds + signs Lambda ZIPs (AWS Signer for prod)
├── requirements-lambda.txt
├── templates/
│   └── event-forwarder-stack.yaml   # StackSet for member account event forwarding
└── scripts/
    ├── cleanup-orphaned-resources.sh
    └── collect-resource-counts.py
```

---

## 7. Related Documents

- [ARCHITECTURE.md](ARCHITECTURE.md) — System architecture overview
- [ORCHESTRATION.md](ORCHESTRATION.md) — Lambda data flow and trigger commands
- [OPERATIONS.md](OPERATIONS.md) — Operational procedures
- [SECURITY.md](SECURITY.md) — IAM, encryption, and compliance
- [DEPLOYMENT.md](DEPLOYMENT.md) — Deployment guide
- [cost-analysis/README.md](cost-analysis/README.md) — Cost analysis feature
