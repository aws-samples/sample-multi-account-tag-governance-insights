# TagInsights Software Design Document

**Version:** 4.1 (Hub-Only Multi-Account Scanning via Resource Explorer Org View)
**Last Updated:** June 2026

---

## 1. Purpose

TagInsights provides AWS resource tagging governance at scale. It collects tag data from all resources across all
accounts and regions in an AWS Organization, stores it as queryable analytics data, and enables cost attribution and
tag compliance reporting.

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
2. Discover accounts with `organizations:ListAccounts`. There is no fallback: if the hub is not the management account
   or a delegated administrator, the Coordinator raises an error (a fallback would scan only the hub and report the
   cycle complete)
3. Build account+region pairs: call `resource-explorer-2:ListIndexes` — each local index represents an account+region
   with discoverable resources
4. **Sequential mode** (default): invoke Worker synchronously per pair; save checkpoint every 10 completions; stop and
   checkpoint if approaching Lambda timeout (2-minute safety buffer)
5. **Fan-out mode** (if scan age ≥ `MAX_SCAN_DAYS`): invoke all remaining Workers asynchronously in parallel (brief
   pause every 50 invocations to avoid Lambda invoke throttling), then close the cycle so the next run starts a fresh
   one. Async invocations that fail after retries go to the `ti-worker-failures-ENV` SQS queue, which has an alarm of
   the same name
6. On full completion: write completion marker to `aggregator-state/completed/{scan_id}.json`, delete checkpoint

**Key environment variables:**

- `CENTRAL_ANALYTICS_BUCKET` — S3 bucket for state and deltas
- `WORKER_FUNCTION_NAME` — Worker Lambda function name to invoke
- `HUB_REGION` — Hub region for RE index queries and Lambda invocations
- `MAX_SCAN_DAYS` — Days before switching sequential scan to fan-out (default: 3)
- `CHECKPOINT_SECRET_ARN` — Secrets Manager ARN of the checkpoint HMAC secret (read once per container; the value is
  never an environment variable)

### 3.2 Worker Lambda

**File:** `src/lambda_handlers/aggregator_worker.py`

The primary data collection component. Invoked by Coordinator (scan path) or forwarded EventBridge tag events (real-time
path).

**Algorithm (scan path):**

1. Call `resource-explorer-2:ListResources` directly via boto3 with filter `accountid:{account} region:{region}`
   (not `Search`, which caps results at 1,000 per query) on the org-scoped view (`OrgViewArn`, or a view whose ARN
   contains `/org/`). Without an org view, member-account scans raise an error instead of returning empty results
2. Extract tags inline from `Properties[name=tags].Data` — no secondary per-resource API calls
3. Load previous state from `aggregator-state/account_id={account}/region={region}/latest.json.gz` (empty dict on first
   scan)
4. Compute delta: new resources, changed tags, removed resources
5. Write delta JSON to `deltas/account_id={account}/region={region}/{timestamp}-{scan_id}.json`
6. Write updated state (compressed JSON) to `latest.json.gz`
7. **Rate-limit SQS notifications** — per-account sliding window (20 msgs/min); rate-limited deltas stay in S3

**Algorithm (real-time path):**
EventBridge rules match the native `aws.tag` "Tag Change on Resource" event (covers every tagging service, including
tags set at creation, with the full tag set) and CloudTrail tag API events (TagResource, CreateTags, etc., which carry
the caller identity); Worker extracts ARNs from the event payload, updates only the affected resources in state.
CloudTrail deltas carry `changed_by` (the `userIdentity` ARN) and `event_name`; native `aws.tag` deltas carry
`event_name` `TagChangeOnResource` and a NULL `changed_by`. Every CloudTrail tag API call also writes one row per resource to `tag_change_events` (`tag-change-events/` prefix: caller ARN, API name, tags set or removed), even when the native event already updated state, so who made a change is kept apart from the change-only delta history.

**Event ordering.** EventBridge delivery is best-effort and unordered, so two changes seconds apart can arrive out of order. The delta history then briefly shows a reverted value, and a resource's `previous_tags` can be the state as read rather than the true previous set when events for one resource are processed concurrently. The daily scan reconciles state, and the cost allocator's intervals are affected only for the seconds between the out-of-order events.

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

Compacts one finished calendar month of daily delta records into `resource_tag_deltas_historical`.

**Trigger:** EventBridge cron `0 3 2 * ? *` (3 AM day 2 of each month), or a manual event `{"year": Y, "month": M}`

**Algorithm:**

1. Target month: the most recent calendar month that ended at least 28 days before the run (a run on November 2
   compacts September)
2. List the month's partitions in `resource_tag_deltas_recent` (Glue) and skip accounts that already have registered
   historical partitions for the month
3. Run Athena `UNLOAD` (up to 100 accounts per query, in parallel) into
   `tag-deltas/historical/account_id=X/year=Y/month=M/`: one row per resource with `first_seen_timestamp`,
   `last_seen_timestamp`, `change_count`, and the latest `changed_by` and `event_name`
4. Only after every `UNLOAD` succeeds and its partitions are registered, delete the month's daily Glue partitions and
   S3 objects. Any failure raises an error (`ti-monthly-agg-errors-ENV`)

So the recent table holds roughly the last one to two months. Objects under `tag-deltas/recent/` expire after 130 days
as a safety net. A failed month must be rerun by hand with `{"year": Y, "month": M}` before then.

### 3.5 Cost Allocator Lambda

**Function:** `ti-cost-alloc-ENV-ACCOUNT` (deployed from `deployment/cost-analysis-stack.yaml`)  
**Files:** `src/lambda_handlers/cost/cost_allocator_handler.py`, `allocation_query.py`, `query_builder.py`,
`schema_manager.py`, `athena_query_executor.py`  
**Trigger:** EventBridge Scheduler, daily at 06:00 UTC (`cron(0 6 * * ? *)`)

**Algorithm:**

1. Pick the months to compute: current and previous UTC month, or one `{"year", "month"}` from the event
2. Build each resource's tag history as time intervals from `resource_tag_deltas_recent` and
   `resource_tag_deltas_historical`
3. Join hourly CUR `Usage`, `SavingsPlanCoveredUsage`, and `DiscountedUsage` line items to the tags on the resource
   at that hour (full ARN, or bare resource ID with a service, account, and Region guard; Region from
   `product_region_code` when present). `Usage` uses unblended cost; the other two use their effective cost, or
   unblended cost when the CUR table lacks that column
4. Write Parquet with one Athena `UNLOAD` per month to `tag-cost-allocation-staging/year=Y/month=M/<run-id>/`, then,
   after it succeeds, replace the live `tag-cost-allocation/year=Y/month=M/` prefix with the staged files

Usage before TagInsights first saw a resource uses the first-observed tags and sets `tags_assumed = true`. When
`CurCatalogAccountId` is set, the allocator reads CUR through the billing account's Glue Data Catalog, registered in
Athena as `<ProjectName>_cur_<Environment>`.

**Prerequisite:** CUR must be enabled in AWS Billing with resource IDs and Athena integration.

---

## 4. Data Model

### 4.1 Tag Delta Record (resource_tag_deltas_recent)

`resource_tag_deltas_historical` has the same columns except it replaces `timestamp`, `scan_id`, and the `day`
partition with `first_seen_timestamp`, `last_seen_timestamp`, and `change_count`. Its `changed_by` and `event_name` hold
the latest non-NULL values in the month.

| Field                    | Type               | Description                                            |
|--------------------------|--------------------|--------------------------------------------------------|
| `arn`                    | STRING             | AWS resource ARN                                       |
| `resource_type`          | STRING             | e.g., `ec2` (extracted from ARN)                       |
| `tags`                   | MAP<STRING,STRING> | Current tags                                           |
| `previous_tags`          | MAP<STRING,STRING> | Tags from previous scan                                |
| `change_type`            | STRING             | `new_resource` / `tags_changed` / `resource_removed`   |
| `timestamp`              | BIGINT             | Unix epoch seconds                                     |
| `source_system`          | STRING             | `aggregator_worker` / `aggregator_worker_realtime` / `aggregator_worker_tag_event` |
| `region`                 | STRING             | AWS region                                             |
| `scan_id`                | STRING             | Scan run identifier (date string or event name)        |
| `changed_by`             | STRING             | CloudTrail `userIdentity` ARN; NULL for native `aws.tag` events and scans |
| `event_name`             | STRING             | API or event name (for example `TagResource`, `TagChangeOnResource`); NULL for scans |
| `account_id`             | STRING             | AWS account ID (partition key)                         |
| `year` / `month` / `day` | INT                | Partition keys                                         |

### 4.2 Tag cost allocation record (tag_cost_allocation)

One row per resource, day, and tag. Cost repeats on each tag key, so filter to one `tag_key` before summing.
Partition projection on `year` and `month`; no partition maintenance.

| Field                                          | Type    | Description                                                    |
|------------------------------------------------|---------|----------------------------------------------------------------|
| `account_id` / `resource_arn` / `resource_type` / `region` | STRING | Resource identifiers                          |
| `usage_date`                                   | DATE    | Day the cost was incurred                                      |
| `tag_key` / `tag_value`                        | STRING  | Tag on the resource at that time; NULL for untagged resources  |
| `tags_assumed`                                 | BOOLEAN | Cost predates first observation; first-observed tags used      |
| `cost`                                         | DOUBLE  | Unblended cost for `Usage`; effective cost for `SavingsPlanCoveredUsage` and `DiscountedUsage` (USD) |
| `year` / `month`                               | INT     | Partition keys                                                 |

---

## 5. Deployment Design

### 5.1 CloudFormation Templates

| Template                                          | Purpose                                                                     |
|---------------------------------------------------|-----------------------------------------------------------------------------|
| `deployment/core-stack.yaml`                      | Hub infrastructure (S3, Glue, Athena, SQS, EventBus, DLQs)                  |
| `deployment/aggregator-stack.yaml`                | Coordinator + Worker + Analytics Writer + Monthly Aggregator Lambdas        |
| `deployment/cost-analysis-stack.yaml`             | Optional: Cost Allocator Lambda + daily schedule + optional cross-account CUR catalog |
| `deployment/deploy.yaml`                          | Root deployment orchestration                                               |
| `deployment/templates/event-forwarder-stack.yaml` | StackSet template for member account tag-change event forwarding (native `aws.tag` + CloudTrail) |
| `deployment/tag-compliance-stack.yaml`            | Optional, deployed separately in the AWS Config delegated administrator account: tag compliance Config rule + daily SES report |
| `deployment/templates/tag-compliance-member-role.yaml` | StackSet template for the per-account Config evaluation role used by the tag compliance rule |

### 5.2 Key Parameters

Root stack (`deploy.yaml`) parameters. The full reference is in
[DEPLOYMENT.md > Parameter Reference](DEPLOYMENT.md#parameter-reference).

| Parameter                 | Default          | Description                                                     |
|---------------------------|------------------|-----------------------------------------------------------------|
| `Environment`             | `prod`           | `dev` / `test` / `cert` / `uat` / `staging` / `prod`            |
| `LambdaCodeBucket`        | (required)       | Code and template bucket (no dots in the name)                  |
| `DeploymentVersion`       | (required)       | Immutable version for templates and packages                    |
| `CheckpointSigningSecret` | (required)       | HMAC secret, at least 32 characters                             |
| `AccessLogsBucketName`    | (required)       | Existing S3 server access-log bucket                            |
| `OrganizationId`          | `""`             | Lets member accounts forward events to the hub event bus        |
| `HubRegion`               | `us-east-1`      | Region of the Resource Explorer aggregator index                |
| `OrgViewArn`              | `""`             | Org-scoped view ARN; empty auto-discovers a view with `/org/`   |
| `PandasLayerArn`          | `""`             | AWS SDK for pandas layer; empty uses version 12 in the stack Region |
| `EnableCodeSigning`       | `false`          | Opt-in AWS Signer enforcement; packages from `signed/v<version>/` |
| `EnableCostAnalysis`      | `false`          | Enables the cost allocator (requires `CurBucketName`)           |
| `CurDatabaseName`         | `""`             | Glue database name for CUR table                                |
| `CurTableName`            | `""`             | Glue table name for CUR data                                    |
| `CurBucketName`           | `not-configured` | S3 bucket holding CUR data                                      |
| `CurCatalogAccountId`     | `""`             | Billing account ID for cross-account CUR                        |
| `CurKmsKeyArn`            | `""`             | KMS key for an SSE-KMS CUR bucket                               |

---

## 6. Source Code Structure

```
src/
├── lambda_handlers/
│   ├── aggregator_coordinator.py    # Checkpoint-based multi-account scan coordinator (HMAC checkpoint signing M8)
│   ├── aggregator_worker.py         # RE direct query + delta computation (SQS rate limiting M5)
│   ├── analytics_writer_handler.py  # SQS → Parquet writer + input validation
│   ├── monthly_aggregator_handler.py
│   ├── tag_compliance_rule.py       # AWS Config custom rule evaluator
│   ├── tag_compliance_report.py     # Daily compliance report via Amazon SES
│   └── cost/
│       ├── allocation_query.py      # tag_cost_allocation Athena UNLOAD query
│       ├── cost_allocator_handler.py # Daily tag_cost_allocation recompute
│       ├── query_builder.py
│       ├── schema_manager.py
│       └── athena_query_executor.py
├── schemas/
│   └── delta_schema.json            # JSON schema for delta file validation
└── utils/
    ├── arn_parser.py
    ├── resource_naming.py
    └── logging_config.py

deployment/
├── core-stack.yaml
├── aggregator-stack.yaml
├── cost-analysis-stack.yaml
├── deploy.yaml                      # Root stack: nests core, aggregator, cost-analysis
├── tag-compliance-stack.yaml        # Deployed separately (AWS Config delegated administrator)
├── package_lambdas.py               # Builds Lambda ZIPs; --sign signs them with AWS Signer (opt-in)
├── requirements-lambda.txt
├── templates/
│   ├── event-forwarder-stack.yaml   # StackSet for member account event forwarding
│   └── tag-compliance-member-role.yaml  # StackSet: Config evaluation role in every account
└── scripts/
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
