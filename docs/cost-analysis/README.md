# TagInsights Cost Analysis

Tag-based cost analysis using direct CUR (Cost & Usage Report) matching via Athena.

## Overview

The cost analysis feature matches AWS resource ARNs directly against CUR line items — no cost allocation tags required.
This means you get per-resource cost data regardless of whether resources are tagged for billing.

**What you get:**

- Per-resource cost attribution matched to tag data
- Cost analysis by any tag key/value combination
- Last month actual cost, current month MTD, and projected spend
- Queryable via the `tag_costs` Athena table

## Prerequisites

1. **CUR must be configured** in AWS Billing Console:
    - Go to **Billing → Cost & Usage Reports → Create report**
    - Enable resource IDs
    - Set S3 delivery with Parquet format (preferred) or CSV
    - Enable Athena integration (creates Glue database + table)
    - Note the Glue database name and table name

2. **Stack parameters** must be set in `deployment/cost-analysis-stack.yaml`:
    - `CurDatabaseName` — Glue database name for the CUR table
    - `CurTableName` — Glue table name for CUR data
    - `CurBucketName` — S3 bucket containing CUR data (required)
    - `CurAccountRoleArn` — Leave empty if CUR is in same account;
      see [Cross-Account CUR Setup](#cross-account-cur-setup) if in a separate billing account

## Architecture

See [ORCHESTRATION.md](../ORCHESTRATION.md) for the full data flow diagram.

**Summary:**

```
CUR S3 Bucket + Tag Delta Data
        ↓
Cost Coordinator Lambda (every 12h)
        ↓ dispatches batches to FIFO SQS queue
Cost Worker Lambdas (up to 50 concurrent, SQS-triggered)
        ↓ ARN → cost matching via Athena
tag-costs/ Parquet in Central Analytics S3
        ↓ when queue empties → SNS → 
Cost Deduplicator Lambda
        ↓
tag_costs Athena Table
```

**Key v2 differences from v1:**

- Work queue is **FIFO** (`taginsights-cost-analysis-work-ENV.fifo`) with content-based deduplication
- **Deduplicator triggered via SNS topic** when work queue empties (not CloudWatch Alarm direct)
- **`EnableCostExplorerFallback=false` by default** — Cost Explorer charges $0.01/request; opt-in required
- **Cross-account CUR** supported via `CurAccountRoleArn` (STS AssumeRole)
- `analytics_table_reader.py` and `resource_cost_analyzer.py` removed; `CURDirectMatcher` used directly

## Cost Explorer Fallback

When CUR is not configured, the cost analysis stack can optionally fall back to AWS Cost Explorer APIs via
`CostAllocationManager`. This is **disabled by default**.

```yaml
# In deployment/cost-analysis-stack.yaml parameters:
EnableCostExplorerFallback: "true"   # opt-in only
```

> ⚠️ **Cost warning:** Cost Explorer
> charges $0.01 per API request. With 50+ tag values queried on a 12-hourly schedule, this can add $5–20/month in
> unexpected charges. Only enable if you have explicitly accepted this cost.

## Source Code

| File                                    | Purpose                                                           |
|-----------------------------------------|-------------------------------------------------------------------|
| `src/lambda_handlers/cost/cur_direct_matcher.py` | ARN → `line_item_resource_id` normalization (service-specific)    |
| `src/lambda_handlers/cost/cur_provider.py`       | Athena query execution against CUR tables (cross-account support) |
| `src/lambda_handlers/cost/query_builder.py`      | CUR Athena query construction                                     |
| `src/lambda_handlers/cost/schema_manager.py`     | CUR schema detection + STS session management for cross-account   |
| `src/lambda_handlers/cost/allocation_manager.py` | Cost allocation tag management + CE fallback (opt-in)             |

## Triggering

```bash
# Find the coordinator function name
aws lambda list-functions \
  --query 'Functions[?contains(FunctionName, `taginsights-cost-coordinator`)].FunctionName' \
  --output text --region us-east-1

# Trigger it
aws lambda invoke \
  --function-name taginsights-cost-coordinator-prod-ACCOUNT_ID \
  --payload '{"trigger_source": "manual", "lookback_hours": 24}' \
  /tmp/cost-response.json && cat /tmp/cost-response.json
```

Monitor the FIFO work queue:

```bash
aws sqs get-queue-attributes \
  --queue-url https://sqs.REGION.amazonaws.com/ACCOUNT/ti-cost-analysis-work-prod.fifo \
  --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible
```

## Querying Cost Data

Open Athena and select the workgroup `ti-REGION-ENV-ACCOUNT`.

```sql
-- Top 10 most expensive resources this month
-- tag_costs has one row per resource per tag, so collapse to one row per resource
SELECT 
    resource_arn,
    resource_type,
    region,
    MAX(current_month_projected) AS projected_monthly_cost,
    MAX(avg_daily_cost) AS avg_daily_cost
FROM "ti_REGION_ENV_ACCOUNT".tag_costs
WHERE year = YEAR(CURRENT_DATE) AND month = MONTH(CURRENT_DATE)
GROUP BY resource_arn, resource_type, region
ORDER BY projected_monthly_cost DESC
LIMIT 10;

-- Cost by tag value (e.g., by Environment)
SELECT 
    tag_value AS environment,
    SUM(current_month_projected) AS total_projected_cost,
    COUNT(DISTINCT resource_arn) AS resource_count
FROM "ti_REGION_ENV_ACCOUNT".tag_costs
WHERE tag_key = 'Environment'
  AND year = YEAR(CURRENT_DATE) AND month = MONTH(CURRENT_DATE)
GROUP BY tag_value
ORDER BY total_projected_cost DESC;

-- Resources with cost but no Environment tag
-- (tag_costs only contains resources that have at least one tag)
SELECT 
    resource_arn,
    resource_type,
    region,
    MAX(current_month_projected) AS projected_monthly_cost
FROM "ti_REGION_ENV_ACCOUNT".tag_costs
WHERE year = YEAR(CURRENT_DATE) AND month = MONTH(CURRENT_DATE)
GROUP BY resource_arn, resource_type, region
HAVING count_if(tag_key = 'Environment') = 0
ORDER BY projected_monthly_cost DESC
LIMIT 20;
```

For more query examples see [QUERY_EXAMPLES.md](QUERY_EXAMPLES.md).

## Data Notes

- CUR data has a **24-48 hour lag** — today's costs won't appear until tomorrow
- The coordinator runs every 12 hours by default
- `monthly_cost` is the actual matched cost from CUR line items
- `matched_in_cur` indicates whether the ARN was found in CUR (some resource types may not appear)
- ARN normalization is service-specific — see `cur_direct_matcher.py` for full mapping

---

## Cross-Account CUR Setup

Most AWS customers deliver their Cost & Usage Report (CUR) to a dedicated
**billing account** (often the AWS Organizations management account or a
centralised finance account) — separate from the hub account where TagInsights
runs. This section explains how to connect TagInsights to a CUR table in a
different account.

### How it works

TagInsights uses **STS AssumeRole** to reach into the CUR account. The hub
Lambda calls `sts:AssumeRole` on a role you create in the billing account,
then uses the resulting temporary credentials for all Glue and Athena API calls
against the CUR table. When `CurAccountRoleArn` is **not set** the feature
uses same-account credentials.

```
Hub Account (TagInsights Lambda)
    │
    │  sts:AssumeRole ──────────────────────────────────────┐
    │                                                        ▼
    │                                          Billing Account
    │                                          taginsights-cur-reader role
    │                                              │
    │◄─── temporary credentials ──────────────────┘
    │
    │  glue:GetTable / athena:StartQueryExecution
    └──────────────────────────────────────────────► Billing Account
                                                    Glue catalog + Athena + CUR S3
```

### Step 1 — Deploy the cost analysis stack with all cross-account CUR parameters

```bash
aws cloudformation update-stack \
  --stack-name TagInsights \
  --use-previous-template \
  --parameters \
      ParameterKey=EnableCostAnalysis,ParameterValue=true \
      ParameterKey=CurAccountRoleArn,ParameterValue=arn:aws:iam::<BILLING_ACCOUNT_ID>:role/taginsights-cur-reader \
      ParameterKey=CurExternalId,ParameterValue=<EXTERNAL_ID> \
      ParameterKey=CurDatabaseName,ParameterValue=<GLUE_DATABASE_NAME> \
      ParameterKey=CurTableName,ParameterValue=<GLUE_TABLE_NAME> \
      ParameterKey=CurBucketName,ParameterValue=<CUR_BUCKET_NAME> \
      ParameterKey=Environment,UsePreviousValue=true \
      ParameterKey=LambdaCodeBucket,UsePreviousValue=true \
      ParameterKey=DeploymentVersion,UsePreviousValue=true \
      ParameterKey=HubRegion,UsePreviousValue=true \
      ParameterKey=OrganizationId,UsePreviousValue=true \
      ParameterKey=CheckpointSigningSecret,UsePreviousValue=true \
  --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND \
  --region ${REGION} --profile ${AWS_PROFILE}
```

Parameters not listed keep their defaults; add `UsePreviousValue=true` for any
others you set at creation.

| Parameter           | Description                                                 |
|---------------------|-------------------------------------------------------------|
| `CurAccountRoleArn` | ARN of the role you will create in Step 2 (billing account) |
| `CurDatabaseName`   | Glue database name (e.g. `athenacurcfn_cur`)                |
| `CurTableName`      | Glue table name (e.g. `cur`)                                |

### Step 2 — Create the IAM role in the billing account

Create a role named `taginsights-cur-reader` in your billing/CUR account.

**Trust policy:**

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "<ARN of the Cost Worker Lambda execution role in hub account>"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

**Permissions policy:**

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "GlueCURRead",
      "Effect": "Allow",
      "Action": ["glue:GetTable", "glue:GetTables", "glue:GetPartition", "glue:GetPartitions"],
      "Resource": [
        "arn:aws:glue:us-east-1:<BILLING_ACCOUNT>:catalog",
        "arn:aws:glue:us-east-1:<BILLING_ACCOUNT>:database/<CUR_DATABASE>",
        "arn:aws:glue:us-east-1:<BILLING_ACCOUNT>:table/<CUR_DATABASE>/<CUR_TABLE>"
      ]
    },
    {
      "Sid": "AthenaQueryExecution",
      "Effect": "Allow",
      "Action": [
        "athena:StartQueryExecution",
        "athena:GetQueryExecution",
        "athena:GetQueryResults",
        "athena:GetWorkGroup"
      ],
      "Resource": "arn:aws:athena:us-east-1:<BILLING_ACCOUNT>:workgroup/<WORKGROUP>"
    },
    {
      "Sid": "CURBucketRead",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::<CUR_BUCKET>",
        "arn:aws:s3:::<CUR_BUCKET>/*"
      ]
    },
    {
      "Sid": "AthenaResultsBucketReadWrite",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:ListBucket", "s3:GetBucketLocation"],
      "Resource": [
        "arn:aws:s3:::<ATHENA_RESULTS_BUCKET>",
        "arn:aws:s3:::<ATHENA_RESULTS_BUCKET>/*"
      ]
    }
  ]
}
```

> **IAM permission notes:**
> - `glue:GetDatabases` (catalog-wide) is **not needed** — `CurDatabaseName` and `CurTableName` are passed directly as
    env vars
> - Athena is scoped to a **specific workgroup ARN** — no wildcard resource

### Step 3 — Grant the hub Lambda permission to assume the role

Add `sts:AssumeRole` to the Cost Worker Lambda's execution role in the hub account:

```json
{
  "Effect": "Allow",
  "Action": "sts:AssumeRole",
  "Resource": "arn:aws:iam::<BILLING_ACCOUNT_ID>:role/taginsights-cur-reader"
}
```

### Step 4 — Verify connectivity

Trigger the cost coordinator and check CloudWatch Logs. A successful cross-account connection logs:

```
INFO  Assumed cross-account CUR role: arn:aws:iam::111122223333:role/taginsights-cur-reader
INFO  CURCostProvider initialized: cur_database.cur_table (region: us-east-1)
INFO  Cross-account mode: CUR role = arn:aws:iam::111122223333:role/taginsights-cur-reader
```

### Troubleshooting

| Symptom                             | Likely cause                                             | Fix                                                        |
|-------------------------------------|----------------------------------------------------------|------------------------------------------------------------|
| `AccessDenied` on `sts:AssumeRole`  | Hub Lambda role missing permission or trust policy wrong | Add permission + verify trust policy Principal             |
| `EntityNotFoundException` in Glue   | Wrong database/table name                                | Verify `CUR_ATHENA_DATABASE` / `CUR_ATHENA_TABLE` env vars |
| Athena `AccessDenied` on S3 results | Results bucket not set or role lacks S3 write permission | Set `CurAthenaResultsBucket` to a writable bucket          |

---

## Teardown — Removing Cross-Account CUR Access

**Step 1 — Redeploy with CUR parameters cleared:**

```bash
aws cloudformation update-stack \
  --stack-name TagInsights \
  --use-previous-template \
  --parameters \
      ParameterKey=EnableCostAnalysis,ParameterValue=true \
      ParameterKey=CurAccountRoleArn,ParameterValue="" \
      ParameterKey=CurExternalId,ParameterValue="" \
      ParameterKey=Environment,UsePreviousValue=true \
      ParameterKey=LambdaCodeBucket,UsePreviousValue=true \
      ParameterKey=DeploymentVersion,UsePreviousValue=true \
      ParameterKey=HubRegion,UsePreviousValue=true \
      ParameterKey=OrganizationId,UsePreviousValue=true \
      ParameterKey=CheckpointSigningSecret,UsePreviousValue=true \
  --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND \
  --region ${REGION} --profile ${AWS_PROFILE}
```

Parameters not listed keep their defaults; add `UsePreviousValue=true` for any
others you set at creation.

**Step 2 — Delete the `taginsights-cur-reader` role in the billing account:**

```bash
aws iam delete-role-policy \
  --role-name taginsights-cur-reader \
  --policy-name TagInsightsCURAccess \
  --profile billing-account-profile

aws iam delete-role \
  --role-name taginsights-cur-reader \
  --profile billing-account-profile
```

> ⚠️ Do not delete the CUR S3 bucket itself — that is managed by AWS Billing.
