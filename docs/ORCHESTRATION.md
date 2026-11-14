# TagInsights - Data Orchestration Guide

This document describes the complete data flow through TagInsights, what each Lambda function does, and how to trigger
them manually.

---

## Overview: End-to-End Data Flow

```mermaid
sequenceDiagram
    participant EB as EventBridge Schedule
    participant CO as Coordinator Lambda<br/>(hub account)
    participant WK as Worker Lambda<br/>(hub account)
    participant RE as AWS Resource Explorer<br/>(org-wide view)
    participant S3 as Central Analytics S3
    participant SQS as Central SQS Queue
    participant AW as Analytics Writer<br/>(hub account)
    participant AT as Athena Tables

    Note over CO: Triggered daily by EventBridge

    CO->>S3: Read scan-checkpoint.json
    CO->>CO: Discover all org accounts × hub regions
    CO->>WK: Invoke Worker per account+region pair

    WK->>RE: ListResources (filter accountid:{id} region:{region})
    RE-->>WK: Resources + inline tags (org-wide view)
    WK->>S3: Read previous state (latest.json.gz)
    WK->>WK: Compute delta
    WK->>S3: Write delta JSON
    WK->>S3: Write updated state (latest.json.gz)
    WK->>SQS: Notify results queue

    CO->>S3: Update scan-checkpoint.json (every 10 completions)

    SQS->>AW: Trigger Analytics Writer
    AW->>S3: Read delta JSON
    AW->>S3: Write Parquet to tag-deltas/recent/
    AW->>AT: Update Glue partitions

    Note over AT: Data queryable in Athena<br/>resource_tag_deltas_recent table
```

---

## Tag Collection Pipeline

```mermaid
flowchart TD
    subgraph HubAccount["Hub Account"]
        EB_SCHED[EventBridge Daily Schedule]
        CO[Coordinator Lambda\nti-orches-ENV-ACCOUNT\nCheckpoint-based sequential scan]
        CKPT[scan-checkpoint.json\nin Central Analytics S3]
    end

    subgraph Worker["Hub Account - Worker"]
        EB_TAG[EventBridge Rule\nTag change detected via CloudTrail]
        WK[Worker Lambda\nti-worker-ENV-ACCOUNT\nOrg-wide RE view queries]
        S3_STATE[Central Analytics S3\naggregator-state/ + deltas/]
    end

    subgraph HubAnalytics["Hub Account - Analytics"]
        SQS[Central SQS Queue\nti-central-results-ENV-ACCOUNT]
        AW[Analytics Writer Lambda\nti-analytics-ENV-ACCOUNT]
        MA[Monthly Aggregator Lambda\nti-monthly-agg-ENV-ACCOUNT]
        subgraph Athena["Athena Tables"]
            T1[resource_tag_deltas_recent\nDaily data, about the last 1-2 months]
            T2[resource_tag_deltas_historical\nAggregated monthly data]
        end
    end

    EB_SCHED -->|Daily trigger| CO
    CO -->|Read/write checkpoint| CKPT
    CO -->|Invoke Worker per account+region| WK
    EB_TAG -->|Real-time tag change| WK
    WK -->|Write delta + state| S3_STATE
    WK -->|Notify| SQS
    SQS -->|Trigger| AW
    AW -->|Write Parquet| T1
    MA -->|Monthly aggregation| T2
```

---

## Cost Analysis Pipeline

```mermaid
flowchart TD
    subgraph Inputs["Inputs"]
        CUR[CUR table\nCost & Usage Report data]
        TDA[Tag history\nresource_tag_deltas_recent + _historical]
    end

    subgraph CostStack["Cost Analysis (deployment/cost-analysis-stack.yaml)"]
        SCH[EventBridge Scheduler\ncron 0 6 * * ? *]
        CA[Cost Allocator Lambda\nti-cost-alloc-ENV-ACCOUNT]
    end

    subgraph Output["Output"]
        S3[tag-cost-allocation/year=Y/month=M/\nParquet in Central Analytics S3]
        AT[tag_cost_allocation Athena Table\npartition projection]
    end

    SCH -->|Daily| CA
    CA -->|One Athena UNLOAD per month| CUR
    CA --> TDA
    CUR -->|Join usage hour to tags valid then| S3
    TDA --> S3
    S3 --> AT
```

**Key design notes:**

- One Lambda, no queues. Each run recomputes the current and previous month, or one `{"year", "month"}` from the event.
- Each month is one Athena `UNLOAD` that replaces `tag-cost-allocation/year=Y/month=M/`.
- **`CurCatalogAccountId` parameter** — cross-account CUR through the billing account's Glue Data Catalog,
  registered in Athena as `<ProjectName>_cur_<Environment>`

---

## Monthly Data Lifecycle

```mermaid
flowchart LR
    subgraph Daily["Daily (about the last 1-2 months)"]
        R[tag-deltas/recent/\nPartitioned by\naccount/year/month/day]
    end

    subgraph Monthly["Monthly (long-term)"]
        H[tag-deltas/historical/\nPartitioned by\naccount/year/month]
    end

    subgraph Lifecycle["Lifecycle Rules"]
        D130[Safety-net expiry\nafter 130 days]
        IA[Transition to IA\nafter 90 days]
    end

    MA[Monthly Aggregator\nRuns day 2 of each month]

    R -->|Month that ended 28+ days ago| MA
    MA -->|Athena UNLOAD| H
    MA -->|Deletes compacted daily partitions| R
    R -.->|Auto-expire| D130
    H -.->|Cost optimization| IA
```

---

## Lambda Reference

### Coordinator

**Function:** `ti-orches-{ENV}-{ACCOUNT_ID}`  
**Account:** Hub account  
**Trigger:** EventBridge daily schedule

**What it does:**

1. Reads `aggregator-state/scan-checkpoint.json` from S3 — resumes in-progress cycle or starts new one (`scan_id` =
   today's date)
2. Discovers all org accounts with `organizations:ListAccounts`. The hub must be the management account or a delegated
   administrator; otherwise the Coordinator raises an error instead of scanning only the hub.
3. Cross-products all org accounts × hub regions to build the work list
4. **Sequential scan** (default): invokes Worker synchronously per pair, saves checkpoint every 10 completions; stops
   and saves checkpoint before Lambda timeout
5. **Fan-out mode** (if scan age ≥ `MAX_SCAN_DAYS`): invokes all remaining Workers asynchronously in parallel, then
   closes the cycle so the next run starts a fresh one. Async invocations that fail after retries go to the
   `ti-worker-failures-ENV` queue, which has an alarm of the same name.
6. On full completion: writes completion marker, deletes checkpoint

**Manual trigger:**

```bash
aws lambda invoke \
  --function-name ti-orches-prod-ACCOUNT_ID \
  --cli-binary-format raw-in-base64-out \
  --payload '{"trigger_source": "manual"}' \
  /tmp/response.json --profile ${AWS_PROFILE} && cat /tmp/response.json
```

---

### Worker

**Function:** `ti-worker-{ENV}-{ACCOUNT_ID}`  
**Account:** Hub account only (uses org-wide Resource Explorer view)  
**Trigger:** Coordinator invocation (scan path) or EventBridge tag change rule (real-time path)

**What it does:**

1. Locates the org-wide Resource Explorer view (`OrgViewArn`, or a view whose ARN contains `/org/`). If there is none,
   member-account scans raise an error instead of returning empty results.
2. Lists every resource in the account+Region with Resource Explorer `ListResources` (filter `accountid:{account} region:{region}`; Search would cap at 1,000 results)
3. Extracts tags inline from RE response `Properties[name=tags].Data` — no secondary API calls
4. Reads previous state from `aggregator-state/account_id={account}/region={region}/latest.json.gz`
5. Computes delta (new resources, changed tags, removed resources)
6. Writes delta JSON to `deltas/account_id={account}/region={region}/{timestamp}-{scan_id}.json`
7. Writes updated state (compressed JSON) to `latest.json.gz`
8. Sends notification to `ti-central-results-ENV-ACCOUNT` SQS queue to trigger Analytics Writer

**Real-time path** (EventBridge CloudTrail rule): extracts ARNs from CloudTrail event payload, updates only affected
resources. These deltas have `change_type` `tags_changed` (or `new_resource`), `event_name` (the API name), and
`changed_by` (the CloudTrail `userIdentity` ARN). Native `aws.tag` events have `event_name` `TagChangeOnResource` and a
NULL `changed_by`; scan deltas have both NULL. Every CloudTrail tag API call also writes one row per resource to `tag_change_events` (`tag-change-events/` prefix: caller ARN, API name, tags set or removed), even when the native event already updated state, so who made a change is kept apart from the change-only delta history.

**Manual trigger (scan path):**

```bash
aws lambda invoke \
  --function-name ti-worker-prod-ACCOUNT_ID \
  --cli-binary-format raw-in-base64-out \
  --payload '{"account_id": "ACCOUNT_ID", "region": "us-east-1", "scan_id": "manual", "trigger_source": "manual"}' \
  /tmp/response.json --profile ${AWS_PROFILE} && cat /tmp/response.json
```

---

### Analytics Writer

**Function:** `ti-analytics-{ENV}-{ACCOUNT_ID}`  
**Account:** Hub account  
**Trigger:** Central SQS queue (triggered automatically after Worker writes deltas and notifies queue)

**What it does:**

1. Reads delta JSON from central analytics S3 bucket
2. Writes Parquet to `tag-deltas/recent/account_id=X/year=Y/month=M/day=D/`
3. Updates Glue table partitions for immediate Athena queryability
4. Handles SQS retries with progressive delay for S3 eventual consistency
5. Deduplicates writes using SHA-256 content hash as filename

**Manual trigger (reprocess pending):**

```bash
aws lambda invoke \
  --function-name ti-analytics-prod-ACCOUNT_ID \
  --cli-binary-format raw-in-base64-out \
  --payload '{"trigger_source": "manual"}' \
  /tmp/response.json --profile ${AWS_PROFILE}
```

---

### Monthly Aggregator

**Function:** `ti-monthly-agg-{ENV}-{ACCOUNT_ID}`  
**Account:** Hub account  
**Trigger:** EventBridge cron — `0 3 2 * ? *` (3 AM day 2 of each month)

**What it does:**

1. Picks the target month: the most recent calendar month that ended at least 28 days before the run (a run on
   November 2 compacts September). An event with `{"year": Y, "month": M}` picks that month instead.
2. Lists the month's daily partitions in Glue and skips accounts already compacted
3. Runs Athena `UNLOAD` (up to 100 accounts per query) into `tag-deltas/historical/account_id=X/year=Y/month=M/`,
   one row per resource with `first_seen_timestamp`, `last_seen_timestamp`, `change_count`, `changed_by`, `event_name`
4. Only after every `UNLOAD` succeeds, deletes the month's daily Glue partitions and S3 objects. Any failure raises an
   error and fires `ti-monthly-agg-errors-ENV`.

A failed month must be rerun by hand before its `tag-deltas/recent/` objects expire at 130 days.

**Manual trigger:**

```bash
aws lambda invoke \
  --function-name ti-monthly-agg-prod-ACCOUNT_ID \
  --cli-binary-format raw-in-base64-out \
  --payload '{"year": 2026, "month": 9}' \
  /tmp/response.json --profile ${AWS_PROFILE}
```

---

### Cost Allocator

**Function:** `ti-cost-alloc-{ENV}-{ACCOUNT_ID}` (if `EnableCostAnalysis=true`)  
**Account:** Hub account  
**Trigger:** EventBridge Scheduler, daily at 06:00 UTC (`cron(0 6 * * ? *)`)

**What it does:**

1. Picks the months to compute: current and previous UTC month, or the `{"year", "month"}` in the event
2. Builds each resource's tag history as time intervals from the tag delta tables
3. Joins hourly CUR `Usage` line items to the tags valid at that hour, in one Athena `UNLOAD` per month
4. Writes Parquet to `tag-cost-allocation/year=Y/month=M/`, read by the `tag_cost_allocation` table

**Manual trigger:**

```bash
aws lambda invoke \
  --function-name ti-cost-alloc-prod-ACCOUNT_ID \
  --cli-binary-format raw-in-base64-out \
  --payload '{"year": 2026, "month": 9}' \
  /tmp/response.json --profile ${AWS_PROFILE}
```

Omit `--payload` to recompute the current and previous month.

**Prerequisites:**

- CUR configured in AWS Billing with resource IDs and Athena integration
- `CurDatabaseName` + `CurTableName` stack parameters set
- For cross-account CUR: `CurCatalogAccountId` parameter set and billing-account grants applied (see [cost-analysis/README.md](cost-analysis/README.md#cross-account-cur-setup))

---

## Trigger Sequence for Initial Data Population

```bash
ENV="prod"
REGION="us-east-1"
HUB_ACCOUNT=$(aws sts get-caller-identity --query Account --output text --profile ${AWS_PROFILE})

# Step 1: Trigger the Coordinator to start the first scan cycle
aws lambda invoke \
  --function-name ti-orches-${ENV}-${HUB_ACCOUNT} \
  --region ${REGION} \
  --cli-binary-format raw-in-base64-out \
  --payload '{"trigger_source": "manual"}' \
  /tmp/step1.json --profile ${AWS_PROFILE} && echo "Coordinator started" && cat /tmp/step1.json

# Step 2: Wait for scan to complete (Coordinator invokes Workers sequentially)
# For large orgs this may span multiple daily invocations
# Check progress:
aws s3 cp s3://ti-central-analytics-${REGION}-${ENV}-${HUB_ACCOUNT}/aggregator-state/scan-checkpoint.json -

# Step 3: Wait for Analytics Writer to process (auto-triggered via SQS, ~2-5 min)
echo "Waiting 5 minutes for Analytics Writer to process..."
sleep 300

# Step 4: Partitions are registered by the Analytics Writer as files land.
# Run this only to recover from a failed registration (results DLQ).
aws athena start-query-execution \
  --query-string "MSCK REPAIR TABLE resource_tag_deltas_recent" \
  --query-execution-context "Database=ti_${REGION}_${ENV}_${HUB_ACCOUNT}" \
  --work-group "ti-${REGION}-${ENV}-${HUB_ACCOUNT}" \
  --region ${REGION} \
  --profile ${AWS_PROFILE}

# Step 5: Verify data
aws athena start-query-execution \
  --query-string "SELECT COUNT(*), account_id FROM resource_tag_deltas_recent GROUP BY account_id" \
  --query-execution-context "Database=ti_${REGION}_${ENV}_${HUB_ACCOUNT}" \
  --work-group "ti-${REGION}-${ENV}-${HUB_ACCOUNT}" \
  --region ${REGION} \
  --profile ${AWS_PROFILE}

# Step 6: Run the cost allocator (if EnableCostAnalysis=true)
aws lambda invoke \
  --function-name ti-cost-alloc-${ENV}-${HUB_ACCOUNT} \
  --region ${REGION} \
  /tmp/step6.json --profile ${AWS_PROFILE} && echo "Cost allocation finished" && cat /tmp/step6.json
```

---

## Monitoring

Check CloudWatch logs for each Lambda:

```bash
# Coordinator (hub account)
aws logs tail /aws/lambda/ti-orches-prod-ACCOUNT --since 1h --profile ${AWS_PROFILE}

# Worker (hub account — scans all org accounts via org-wide RE view)
aws logs tail /aws/lambda/ti-worker-prod-ACCOUNT --since 1h --profile ${AWS_PROFILE}

# Analytics Writer (hub account)
aws logs tail /aws/lambda/ti-analytics-prod-ACCOUNT --since 1h --profile ${AWS_PROFILE}

# Monthly Aggregator (hub account)
aws logs tail /aws/lambda/ti-monthly-agg-prod-ACCOUNT --since 24h --profile ${AWS_PROFILE}

# Cost Allocator (hub account)
aws logs tail /aws/lambda/ti-cost-alloc-prod-ACCOUNT --since 1d --profile ${AWS_PROFILE}
```

Check scan checkpoint progress:

```bash
aws s3 cp s3://ti-central-analytics-REGION-prod-ACCOUNT/aggregator-state/scan-checkpoint.json - \
  | python3 -m json.tool
# Shows: scan_id, completed_count/total_count, errors
```

Check SQS queue depth (should drain to 0 after Analytics Writer processes):

```bash
aws sqs get-queue-attributes \
  --queue-url https://sqs.us-east-1.amazonaws.com/ACCOUNT/ti-central-results-prod-ACCOUNT \
  --attribute-names ApproximateNumberOfMessages,ApproximateNumberOfMessagesNotVisible \
  --profile ${AWS_PROFILE}
```
