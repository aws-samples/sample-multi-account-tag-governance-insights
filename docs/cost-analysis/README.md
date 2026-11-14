# TagInsights Cost Analysis

Cost by tag, computed from the AWS Cost and Usage Report (CUR) with Amazon Athena.

## Overview

The cost allocator matches resources to CUR line items by ARN or resource ID.
You don't need to activate cost allocation tags. Each hour of usage is
attributed to the tags that were on the resource at that hour.

**What you get:**

- Daily cost per resource and tag, for any tag key
- Cost by tag over any period since deployment
- Results in the `tag_cost_allocation` Athena table (see [How cost allocation works](#how-cost-allocation-works))

## Prerequisites

1. **CUR must be configured** in AWS Billing Console:
    - Go to **Billing → Cost & Usage Reports → Create report**
    - Enable resource IDs
    - Set S3 delivery with Parquet format (preferred) or CSV
    - Enable Athena integration (creates Glue database + table)
    - Note the Glue database name and table name

2. **Stack parameters** must be set when you deploy with `EnableCostAnalysis=true`:
    - `CurDatabaseName` — Glue database name for the CUR table
    - `CurTableName` — Glue table name for CUR data
    - `CurBucketName` — S3 bucket containing CUR data (required)
    - `CurCatalogAccountId` — Leave empty if CUR is in the same account;
      see [Cross-Account CUR Setup](#cross-account-cur-setup) if it is in a separate billing account

## Architecture

See [ORCHESTRATION.md](../ORCHESTRATION.md) for the full data flow diagram.

**Summary:**

```
EventBridge Scheduler (daily, cron(0 6 * * ? *))
        ↓
Cost Allocator Lambda (ti-cost-alloc-ENV-ACCOUNT)
        ↓ one Athena UNLOAD per month: CUR ⋈ tag history
s3://<analytics bucket>/tag-cost-allocation-staging/year=Y/month=M/<run-id>/
        ↓ copied after the UNLOAD succeeds
s3://<analytics bucket>/tag-cost-allocation/year=Y/month=M/
        ↓
tag_cost_allocation Athena table (partition projection)
```

## How cost allocation works

The `tag_cost_allocation` table is a set-based cost allocation. Use it for
cost by tag over any period since deployment.

- **Schedule.** The `ti-cost-alloc-ENV-ACCOUNT` Lambda function runs daily at
  06:00 UTC. It recomputes the current and previous billing month with one
  Amazon Athena query each. `UNLOAD` writes Parquet to a run-specific prefix
  under `tag-cost-allocation-staging/` in the central analytics Amazon S3
  bucket. Only after the query succeeds does the allocator replace the live
  `tag-cost-allocation/year=Y/month=M/` prefix with the staged files. A failed
  or timed-out run leaves the previous data in place.
- **Table.** The AWS Glue Data Catalog table `tag_cost_allocation` uses
  partition projection on `year` and `month`, so you don't run `MSCK REPAIR`
  or maintain partitions. For columns and queries, see
  [QUERY_EXAMPLES.md](QUERY_EXAMPLES.md#cost-by-tag-from-tag_cost_allocation).
- **Tag intervals.** The allocator builds each resource's tag history from
  `resource_tag_deltas_recent` and `resource_tag_deltas_historical` as time
  intervals. It joins each hourly AWS Cost and Usage Report (CUR) line item
  of type `Usage`, `SavingsPlanCoveredUsage`, or `DiscountedUsage` to the tags
  that were on the resource at that hour, then sums cost per resource, day,
  and tag.
- **Cost basis.** `Usage` lines use unblended cost. `SavingsPlanCoveredUsage`
  lines use the Savings Plans effective cost, and `DiscountedUsage` lines use
  the reservation effective cost. If the CUR table lacks an effective-cost
  column, that line type falls back to unblended cost.
- **Resource matching.** A resource matches CUR by its full ARN, or by its
  bare resource ID (for example, an instance ID or a bucket name). Bare-ID
  matches also require the same service, the same account, and the same AWS
  Region, so a bucket and an instance with the same name don't collide, and
  identical IDs in two accounts aren't double counted. The Region comes from
  `product_region_code` when the CUR table has it.
- **Assumed tags.** History starts when TagInsights is deployed. Cost incurred
  before TagInsights first observed a resource uses the resource's
  first-observed tags, and those rows have `tags_assumed = true`. For periods
  before deployment with tags that TagInsights never observed, activate
  [AWS cost allocation tags with backfill](https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/activating-tags.html)
  (up to 13 months).
- **Over-counting.** Each row repeats the resource's cost once per tag key.
  When you `SUM(cost)`, filter to one `tag_key`, or deduplicate to one row per
  resource and day first. Summing across all keys over-counts.
- **Multi-value tags.** You can split a value such as
  `CustomerName = ACME+GLOBEX` evenly across its entries in SQL. See
  [Multi-value tag split](QUERY_EXAMPLES.md#multi-value-tag-split).
- **Cross-account CUR.** The allocator reads CUR in another account through
  that account's Glue Data Catalog, registered in Athena when you set
  `CurCatalogAccountId`. The CUR Glue table must be in the hub Region. The
  billing account grants the allocator role read access
  ([Step 2](#step-2--grant-the-cost-allocator-read-access)).

On a test account, the allocation matched 99% of matchable CUR usage cost,
with no CUR line item attributed to more than one resource.

## Source Code

| File                                                 | Purpose                                                            |
|------------------------------------------------------|--------------------------------------------------------------------|
| `src/lambda_handlers/cost/cost_allocator_handler.py` | Daily `tag_cost_allocation` recompute (current and previous month) |
| `src/lambda_handlers/cost/allocation_query.py`       | Athena `UNLOAD` query that builds `tag_cost_allocation`            |
| `src/lambda_handlers/cost/query_builder.py`          | CUR Athena query construction                                      |
| `src/lambda_handlers/cost/schema_manager.py`         | CUR schema detection (cost and Region columns)                     |
| `src/lambda_handlers/cost/athena_query_executor.py`  | Starts, checks, and cancels Athena queries (polling and timeouts are in the handler) |

## Triggering

The allocator runs daily at 06:00 UTC. To run it now for the current and
previous month:

```bash
aws lambda invoke \
  --function-name ti-cost-alloc-prod-<HUB_ACCOUNT_ID> \
  --region ${REGION} --profile ${AWS_PROFILE} \
  /tmp/alloc.json && cat /tmp/alloc.json
```

To recompute one month, pass it in the event:

```bash
aws lambda invoke \
  --function-name ti-cost-alloc-prod-<HUB_ACCOUNT_ID> \
  --cli-binary-format raw-in-base64-out \
  --payload '{"year": 2026, "month": 9}' \
  --region ${REGION} --profile ${AWS_PROFILE} \
  /tmp/alloc.json && cat /tmp/alloc.json
```

Each successful run replaces the month's prefix in S3, so reruns are safe.
`"status": "skipped"` means `CurDatabaseName` or `CurTableName` is empty.

## Querying Cost Data

Open Athena and select the workgroup `ti-REGION-ENV-ACCOUNT`.

```sql
-- Cost by Environment tag value this month
SELECT
    tag_value AS environment,
    ROUND(SUM(cost), 2) AS cost,
    COUNT(DISTINCT resource_arn) AS resource_count
FROM "ti_REGION_ENV_ACCOUNT".tag_cost_allocation
WHERE tag_key = 'Environment'
  AND year = YEAR(CURRENT_DATE) AND month = MONTH(CURRENT_DATE)
GROUP BY tag_value
ORDER BY cost DESC;
```

Filter to one `tag_key` before you `SUM(cost)`. For more queries, see
[QUERY_EXAMPLES.md](QUERY_EXAMPLES.md).

## Data Notes

- CUR data has a **24-48 hour lag**. Today's costs appear in a later run.
- The allocator recomputes the current and previous month each day, so late
  CUR adjustments for the previous month are picked up.
- `cost` is unblended cost for `Usage` lines and effective cost for
  `SavingsPlanCoveredUsage` and `DiscountedUsage` lines (unblended when the
  effective-cost column is missing).
- `tags_assumed = true` marks cost incurred before TagInsights first saw the resource.

---

## Cross-Account CUR Setup

Most AWS customers deliver their Cost & Usage Report (CUR) to a dedicated
**billing account** (often the AWS Organizations management account or a
centralised finance account) — separate from the hub account where TagInsights
runs. This section explains how to connect TagInsights to a CUR table in a
different account.

### How it works

When you set `CurCatalogAccountId`, the stack registers the billing account's
Glue Data Catalog in Athena as `<ProjectName>_cur_<Environment>`. The cost
allocator queries CUR through that catalog with its own role. The billing
account grants that role read access to the catalog and the CUR bucket. When
`CurCatalogAccountId` is empty, the allocator reads CUR from the hub account.

The CUR Glue database and table must be in the hub Region. Athena reads a
cross-account catalog only in its own Region.

```
Hub Account                                   Billing Account
Cost Allocator Lambda                         Glue Data Catalog (CUR table)
  role ti-cost-alloc-<Region>-<Env>  ──────►  CUR S3 bucket
  Athena catalog ti_cur_<Env>                 (resource policies grant read)
```

### Step 1 — Deploy the cost analysis stack with the CUR parameters

```bash
aws cloudformation update-stack \
  --stack-name TagInsights \
  --template-url https://${BUCKET_NAME}.s3.${REGION}.amazonaws.com/templates/v${DEPLOYMENT_VERSION}/deploy.yaml \
  --parameters \
      ParameterKey=DeploymentVersion,ParameterValue=${DEPLOYMENT_VERSION} \
      ParameterKey=EnableCostAnalysis,ParameterValue=true \
      ParameterKey=CurDatabaseName,ParameterValue=<cur-glue-database> \
      ParameterKey=CurTableName,ParameterValue=<cur-glue-table> \
      ParameterKey=CurBucketName,ParameterValue=<cur-bucket-name> \
      ParameterKey=CurCatalogAccountId,ParameterValue=<billing-account-id> \
      ParameterKey=CurKmsKeyArn,ParameterValue=<cur-kms-key-arn> \
      ParameterKey=Environment,UsePreviousValue=true \
      ParameterKey=LambdaCodeBucket,UsePreviousValue=true \
      ParameterKey=AccessLogsBucketName,UsePreviousValue=true \
      ParameterKey=CheckpointSigningSecret,UsePreviousValue=true \
      ParameterKey=OrganizationId,UsePreviousValue=true \
      ParameterKey=HubRegion,UsePreviousValue=true \
      ParameterKey=ScanSchedule,UsePreviousValue=true \
      ParameterKey=MaxScanDays,UsePreviousValue=true \
      ParameterKey=EnableRealTimeEvents,UsePreviousValue=true \
      ParameterKey=EnableEventBridgeExtension,UsePreviousValue=true \
      ParameterKey=EnableCodeSigning,UsePreviousValue=true \
      ParameterKey=LogLevel,UsePreviousValue=true \
      ParameterKey=ForceDeleteOnStackRemoval,UsePreviousValue=true \
      ParameterKey=PandasLayerArn,UsePreviousValue=true \
      ParameterKey=OrgViewArn,UsePreviousValue=true \
  --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND \
  --region ${REGION} --profile ${AWS_PROFILE}
```

For an SSE-S3 CUR bucket, pass `ParameterKey=CurKmsKeyArn,ParameterValue=""`.
`DEPLOYMENT_VERSION` must be the version the stack runs now (or a new version
you have published). Keep `UsePreviousValue=true` for every parameter you do
not change. A parameter you leave out reverts to its default, and
`CheckpointSigningSecret` must always be passed this way. See
[DEPLOYMENT.md > Update Stack](../DEPLOYMENT.md#update-stack).

| Parameter             | Description                                                  |
|-----------------------|--------------------------------------------------------------|
| `CurBucketName`       | S3 bucket holding the CUR data (IAM access is scoped to it)  |
| `CurDatabaseName`     | Glue database name (e.g. `athenacurcfn_cur`)                 |
| `CurTableName`        | Glue table name (e.g. `cur`)                                 |
| `CurCatalogAccountId` | Billing account ID; registers its Glue catalog in Athena (Step 2) |
| `CurKmsKeyArn`        | KMS key ARN if the CUR bucket uses SSE-KMS; empty for SSE-S3 |

### Step 2 — Grant the cost allocator read access

The cost allocator joins CUR to tag history in one Athena query. It reads the
billing account's Glue catalog and CUR bucket with its own role,
`<ProjectName>-cost-alloc-<Region>-<Environment>` (for example
`ti-cost-alloc-us-east-1-prod`). The stack registers the billing catalog in
Athena as `<ProjectName>_cur_<Environment>` and queries
`"ti_cur_prod".<cur_db>.<cur_table>`. Run these in the billing account, in the
hub Region.

**Glue Data Catalog resource policy.** `put-resource-policy` replaces the whole
policy, so check for an existing one first:

```bash
aws glue get-resource-policy --region ${REGION} --profile billing-account-profile
```

If none exists, create it. If one exists, add the statement below to it and
pass `--policy-hash-condition <PolicyHash from get-resource-policy>` instead of
`--policy-exists-condition NOT_EXIST`.

```bash
cat > glue-policy.json <<JSON
{
  "Version": "2012-10-17",
  "Statement": [{
    "Sid": "TagInsightsCostAllocatorRead",
    "Effect": "Allow",
    "Principal": {"AWS": "arn:aws:iam::<HUB_ACCOUNT_ID>:role/ti-cost-alloc-${REGION}-prod"},
    "Action": ["glue:GetDatabase", "glue:GetTable", "glue:GetPartition", "glue:GetPartitions"],
    "Resource": [
      "arn:aws:glue:${REGION}:<BILLING_ACCOUNT_ID>:catalog",
      "arn:aws:glue:${REGION}:<BILLING_ACCOUNT_ID>:database/<GLUE_DATABASE_NAME>",
      "arn:aws:glue:${REGION}:<BILLING_ACCOUNT_ID>:table/<GLUE_DATABASE_NAME>/<GLUE_TABLE_NAME>"
    ]
  }]
}
JSON

aws glue put-resource-policy --policy-in-json file://glue-policy.json \
  --policy-exists-condition NOT_EXIST --region ${REGION} --profile billing-account-profile
```

If AWS Lake Formation manages the CUR database (it is not granted to
`IAMAllowedPrincipals`), grant `DESCRIBE` on the database and `SELECT` on the
table to the allocator role in Lake Formation instead.

**CUR bucket policy.** Add this statement to the CUR bucket's existing policy
(it already allows AWS billing to write reports):

```json
{
  "Sid": "TagInsightsCostAllocatorRead",
  "Effect": "Allow",
  "Principal": {"AWS": "arn:aws:iam::<HUB_ACCOUNT_ID>:role/ti-cost-alloc-<REGION>-prod"},
  "Action": ["s3:GetObject", "s3:ListBucket"],
  "Resource": ["arn:aws:s3:::<CUR_BUCKET_NAME>", "arn:aws:s3:::<CUR_BUCKET_NAME>/*"]
}
```

**KMS.** If the CUR bucket uses SSE-KMS, set `CurKmsKeyArn` to the key ARN.
The stack then grants the allocator role `kms:Decrypt` on that key through S3.
The key policy in the billing account must also allow the allocator role
`kms:Decrypt`. If the Glue catalog is encrypted, Athena can
query it across accounts only with a customer managed key, and the role also
needs `kms:Decrypt` on that key.

### Step 3 — Verify

Invoke the allocator and check the result:

```bash
aws lambda invoke --function-name ti-cost-alloc-prod-<HUB_ACCOUNT_ID> \
  --region ${REGION} --profile ${AWS_PROFILE} /tmp/alloc.json && cat /tmp/alloc.json
```

`"status": "success"` with one entry per month means the grants work.
`AccessDeniedException` from Glue points to the catalog policy;
`Access Denied` on S3 points to the bucket policy or KMS key.

### Troubleshooting

| Symptom                             | Likely cause                                             | Fix                                                        |
|-------------------------------------|----------------------------------------------------------|------------------------------------------------------------|
| `EntityNotFoundException` in Glue   | Wrong database/table name                                | Check `CurDatabaseName` / `CurTableName`                   |
| `glue:GetPartition` ... no resource-based policy allows | Billing Glue catalog policy missing the allocator grant | Apply the Step 2 Glue policy (it includes `glue:GetPartition`) |
| `glue:GetPartition` ... no identity-based policy allows, right after a stack update | New IAM grant still propagating to cross-account Glue checks (took several minutes in testing) | Wait 10 minutes and rerun |
| `Access Denied` on S3               | CUR bucket policy or KMS key policy missing the allocator role | Apply the Step 2 bucket and KMS grants               |

---

## Teardown — Removing Cross-Account CUR Access

**Step 1 — Redeploy with `CurCatalogAccountId` cleared:**

```bash
aws cloudformation update-stack \
  --stack-name TagInsights \
  --template-url https://${BUCKET_NAME}.s3.${REGION}.amazonaws.com/templates/v${DEPLOYMENT_VERSION}/deploy.yaml \
  --parameters \
      ParameterKey=DeploymentVersion,ParameterValue=${DEPLOYMENT_VERSION} \
      ParameterKey=EnableCostAnalysis,ParameterValue=true \
      ParameterKey=CurCatalogAccountId,ParameterValue="" \
      ParameterKey=CurKmsKeyArn,ParameterValue="" \
      ParameterKey=CurDatabaseName,UsePreviousValue=true \
      ParameterKey=CurTableName,UsePreviousValue=true \
      ParameterKey=CurBucketName,UsePreviousValue=true \
      ParameterKey=Environment,UsePreviousValue=true \
      ParameterKey=LambdaCodeBucket,UsePreviousValue=true \
      ParameterKey=AccessLogsBucketName,UsePreviousValue=true \
      ParameterKey=CheckpointSigningSecret,UsePreviousValue=true \
      ParameterKey=OrganizationId,UsePreviousValue=true \
      ParameterKey=HubRegion,UsePreviousValue=true \
      ParameterKey=ScanSchedule,UsePreviousValue=true \
      ParameterKey=MaxScanDays,UsePreviousValue=true \
      ParameterKey=EnableRealTimeEvents,UsePreviousValue=true \
      ParameterKey=EnableEventBridgeExtension,UsePreviousValue=true \
      ParameterKey=EnableCodeSigning,UsePreviousValue=true \
      ParameterKey=LogLevel,UsePreviousValue=true \
      ParameterKey=ForceDeleteOnStackRemoval,UsePreviousValue=true \
      ParameterKey=PandasLayerArn,UsePreviousValue=true \
      ParameterKey=OrgViewArn,UsePreviousValue=true \
  --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND \
  --region ${REGION} --profile ${AWS_PROFILE}
```

`DEPLOYMENT_VERSION` must be the version the stack runs now (or a new version
you have published). Keep `UsePreviousValue=true` for every parameter you do
not change. A parameter you leave out reverts to its default, and
`CheckpointSigningSecret` must always be passed this way. See
[DEPLOYMENT.md > Update Stack](../DEPLOYMENT.md#update-stack). After you clear
`CurCatalogAccountId`, point `CurDatabaseName`, `CurTableName`, and
`CurBucketName` at a CUR table in the hub account, or set
`EnableCostAnalysis=false` to remove cost analysis.

**Step 2 — Remove the cost allocator grants in the billing account:** delete
the `TagInsightsCostAllocatorRead` statements from the Glue catalog resource
policy and the CUR bucket policy (and any KMS key policy grant).

> ⚠️ Do not delete the CUR S3 bucket itself — that is managed by AWS Billing.
