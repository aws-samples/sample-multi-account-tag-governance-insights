# TagInsights Operations Guide

Day-to-day operational procedures for running TagInsights in production.

---

## Initial Data Population

After deployment, trigger the first scan manually:

```bash
ENV="prod"
REGION="us-east-1"
ACCOUNT=$(aws sts get-caller-identity --query Account --output text --profile ${AWS_PROFILE})

# Trigger the Coordinator — it will discover all accounts and start scanning
aws lambda invoke \
  --function-name ti-orches-${ENV}-${ACCOUNT} \
  --region ${REGION} \
  --cli-binary-format raw-in-base64-out \
  --payload '{"trigger_source": "manual"}' \
  /tmp/response.json && cat /tmp/response.json
```

For large organizations the Coordinator may need multiple daily invocations to complete the first scan cycle. Check
progress:

```bash
# Check scan checkpoint (shows scan_id, completed_count, total_count, errors)
aws s3 cp \
  s3://ti-central-analytics-${REGION}-${ENV}-${ACCOUNT}/aggregator-state/scan-checkpoint.json \
  - | python3 -m json.tool
```

Partitions are registered automatically. If a registration failed (records in the results DLQ), repair them. Select database `ti_REGION_ENV_ACCOUNT` in the Athena query editor first (these DDL
statements use the selected database; the name is not double-quoted here):

```sql
MSCK REPAIR TABLE resource_tag_deltas_recent;
MSCK REPAIR TABLE resource_tag_deltas_historical;
```

---

## Ongoing Operations

### Automatic Collection

Data collection runs automatically — no manual intervention required:

- **Tag changes** → EventBridge rules detect changes in real time: the native `aws.tag` "Tag Change on Resource" event (all tagging services, including creation-time tags) plus CloudTrail tag API rules for the services listed in `deployment/templates/event-forwarder-stack.yaml` (CloudTrail rules require a trail; see DEPLOYMENT.md). Delivery is unordered, so changes seconds apart can land out of order and the delta history may briefly show a reverted value; the daily scan reconciles state.
- **Daily scan** → Coordinator runs daily; resumes from checkpoint if scan spans multiple days
- **Fan-out mode** → Coordinator automatically switches to parallel invocation if scan age exceeds `MaxScanDays`
- **Cost allocation** → Runs daily at 06:00 UTC and recomputes the current and previous month (if `EnableCostAnalysis=true`)
- **Monthly aggregation** → Runs on day 2 of each month at 3 AM UTC (see [Monthly aggregation](#monthly-aggregation))

### Monitoring

**CloudWatch Alarms** (created automatically by CloudFormation):

- `ti-orches-errors-ENV`: any Coordinator error, including a hub that cannot call `organizations:ListAccounts`
- `ti-orches-timeout-ENV`: Coordinator duration near its 15-minute timeout
- `ti-worker-errors-ENV`: 5 or more Worker errors, for example member-account scans without an org-scoped Resource Explorer view
- `ti-worker-timeout-ENV`: Worker duration near its 15-minute timeout
- `ti-worker-failures-ENV`: a message in the `ti-worker-failures-ENV` queue (a failed asynchronous Worker invocation from a fan-out)
- `ti-forwarded-events-dlq-ENV`: a message in the forwarded-events dead-letter queue
- `ti-central-results-dlq-ENV`: a message in the central results dead-letter queue
- `ti-monthly-agg-errors-ENV`: any Monthly Aggregator error
- `ti-cost-allocator-errors-ENV`: cost allocator errors (if `EnableCostAnalysis=true`)

**Check Lambda logs:**

```bash
# Coordinator (hub account)
aws logs tail /aws/lambda/ti-orches-prod-ACCOUNT --since 1h --profile ${AWS_PROFILE}

# Worker (hub account)
aws logs tail /aws/lambda/ti-worker-prod-ACCOUNT --since 1h --profile ${AWS_PROFILE}

# Analytics Writer (hub account)
aws logs tail /aws/lambda/ti-analytics-prod-ACCOUNT --since 1h --profile ${AWS_PROFILE}

# Cost Allocator (hub account)
aws logs tail /aws/lambda/ti-cost-alloc-prod-ACCOUNT --since 1d --profile ${AWS_PROFILE}

# Monthly Aggregator (hub account)
aws logs tail /aws/lambda/ti-monthly-agg-prod-ACCOUNT --since 1h --profile ${AWS_PROFILE}
```

**Check scan checkpoint:**

```bash
aws s3 cp \
  s3://ti-central-analytics-REGION-prod-ACCOUNT/aggregator-state/scan-checkpoint.json - \
  | python3 -m json.tool
# Key fields: scan_id, completed_count, total_count, errors
```

**Check SQS queue depth** (should drain to 0 after Analytics Writer runs):

```bash
aws sqs get-queue-attributes \
  --queue-url https://sqs.REGION.amazonaws.com/ACCOUNT/ti-central-results-prod-ACCOUNT \
  --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible \
  --profile ${AWS_PROFILE}
```

---

## Athena Queries

Select workgroup: `ti-REGION-ENV-ACCOUNT`

**Verify data is flowing:**

```sql
SELECT COUNT(*), account_id, MAX(FROM_UNIXTIME(timestamp)) as latest
FROM "ti_REGION_ENV_ACCOUNT".resource_tag_deltas_recent
GROUP BY account_id
ORDER BY latest DESC;
```

**Check for stale data** (no scan in 8+ days):

```sql
SELECT account_id, 
       MAX(FROM_UNIXTIME(timestamp)) as last_scan,
       date_diff('day', MAX(FROM_UNIXTIME(timestamp)), current_timestamp) as days_ago
FROM "ti_REGION_ENV_ACCOUNT".resource_tag_deltas_recent
GROUP BY account_id
HAVING date_diff('day', MAX(FROM_UNIXTIME(timestamp)), current_timestamp) > 7
ORDER BY days_ago DESC;
```

For more query examples see [USAGE.md](USAGE.md) and
[cost-analysis/QUERY_EXAMPLES.md](cost-analysis/QUERY_EXAMPLES.md).

---

## Stack Management

### Update Stack (after template changes)

From console: CloudFormation → Select stack → Update → Replace current template → enter S3 URL → keep all parameters.

Or with the CLI. Use the versioned template URL for the version the stack
should run, and keep `UsePreviousValue=true` for every parameter you do not
change:

```bash
aws cloudformation update-stack \
  --stack-name TagInsights \
  --template-url https://${BUCKET_NAME}.s3.${REGION}.amazonaws.com/templates/v${DEPLOYMENT_VERSION}/deploy.yaml \
  --parameters \
      ParameterKey=DeploymentVersion,ParameterValue=${DEPLOYMENT_VERSION} \
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
      ParameterKey=EnableCostAnalysis,UsePreviousValue=true \
      ParameterKey=EnableCodeSigning,UsePreviousValue=true \
      ParameterKey=LogLevel,UsePreviousValue=true \
      ParameterKey=ForceDeleteOnStackRemoval,UsePreviousValue=true \
      ParameterKey=CurDatabaseName,UsePreviousValue=true \
      ParameterKey=CurTableName,UsePreviousValue=true \
      ParameterKey=CurBucketName,UsePreviousValue=true \
      ParameterKey=CurCatalogAccountId,UsePreviousValue=true \
      ParameterKey=CurKmsKeyArn,UsePreviousValue=true \
      ParameterKey=PandasLayerArn,UsePreviousValue=true \
      ParameterKey=OrgViewArn,UsePreviousValue=true \
  --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND \
  --region ${REGION} --profile ${AWS_PROFILE}
```

A parameter you leave out reverts to its default. Leaving out
`EnableCostAnalysis` sets it to `false` and deletes the cost analysis stack.
Leaving out `CheckpointSigningSecret` fails the update. See
[DEPLOYMENT.md > Update Stack](DEPLOYMENT.md#update-stack) for how to publish a
new version.

### Delete Stack (uninstall)

Follow [DEPLOYMENT.md > Uninstall](DEPLOYMENT.md#uninstall). It removes the
StackSets and the tag compliance stack first, then the root stack.

For `prod` (with `ForceDeleteOnStackRemoval=false`), the data buckets, queues,
event bus, KMS key, and the aggregator functions and roles are retained. The
data buckets have versioning on, so `aws s3 rb --force` fails on them. Delete
every object version first (see the DEPLOYMENT.md Uninstall section).

For non-prod (`dev`/`test`/`cert`/`uat`/`staging`), all buckets are emptied and
deleted automatically.

---

## StackSet Operations

The `TagInsights-EventForwarder` StackSet deploys the EventBridge forwarding rule to member accounts, enabling real-time
tag-change events to be sent to the hub account.

### Check deployment status across member accounts

```bash
aws cloudformation list-stack-instances \
  --stack-set-name TagInsights-EventForwarder \
  --query 'Summaries[*].{Account:Account,Region:StackInstanceRegion,Status:Status}' \
  --output table \
  --profile ${AWS_PROFILE}
```

### Retry failed StackSet instance

```bash
aws cloudformation update-stack-instances \
  --stack-set-name TagInsights-EventForwarder \
  --accounts ACCOUNT_ID \
  --regions us-east-1 \
  --operation-preferences FailureToleranceCount=0 \
  --profile ${AWS_PROFILE}
```

---

## Troubleshooting

### No data in Athena

1. Check if Coordinator ran: `aws logs tail /aws/lambda/ti-orches-prod-ACCOUNT --since 1h --profile ${AWS_PROFILE}`
2. Check checkpoint: `aws s3 cp s3://ti-central-analytics-REGION-prod-ACCOUNT/aggregator-state/scan-checkpoint.json -`
3. Check SQS queue depth — if > 0, Analytics Writer may have failed
4. Check the results DLQ; if records failed partition registration, run `MSCK REPAIR TABLE resource_tag_deltas_recent` in Athena (workgroup: `ti-REGION-prod-ACCOUNT`, database: `ti_REGION_prod_ACCOUNT`)
5. Manually trigger Coordinator:
   ```bash
   aws lambda invoke \
     --function-name ti-orches-prod-ACCOUNT \
     --cli-binary-format raw-in-base64-out \
     --payload '{"trigger_source": "manual"}' \
     /tmp/resp.json --profile ${AWS_PROFILE} && cat /tmp/resp.json
   ```

### Coordinator scan spanning multiple days

This is expected for large organizations. The Coordinator saves progress in `scan-checkpoint.json` and resumes on the
next daily invocation. After `MaxScanDays`, it automatically switches to fan-out mode. No action needed.

### StackSet failed in member account

1. Check CloudFormation events in the member account
2. Common cause: EventBridge rule already exists from a previous rollback
3. Fix: Log into member account, delete the orphaned resource, retry the StackSet operation

### AthenaWorkgroup deletion fails during stack delete

The Athena results bucket is non-empty. Empty it first:

```bash
aws s3 rm s3://ti-athena-results-REGION-ENV-ACCOUNT --recursive --profile ${AWS_PROFILE}
```

Then retry the stack deletion.

### Cost allocator errors

The `ti-cost-allocator-errors-ENV` alarm fired, or `tag_cost_allocation` has no rows for a month. Check the allocator logs:

```bash
aws logs tail /aws/lambda/ti-cost-alloc-prod-ACCOUNT --since 1d --profile ${AWS_PROFILE}
```

Common causes: CUR table not accessible, missing Glue or S3 grants for cross-account CUR, CUR schema changed. See
[cost-analysis/README.md → Troubleshooting](cost-analysis/README.md#troubleshooting).

### Worker not scanning all accounts

The Worker uses the Organization-wide Resource Explorer view (ARN contains `/org/`). Verify the org view exists:

```bash
aws resource-explorer-2 list-views --region ${HUB_REGION} --profile ${AWS_PROFILE} \
  | jq '.Views[] | select(contains("/view/org/"))'
```

If it is missing, create it (see
[DEPLOYMENT.md > AWS Resource Explorer](DEPLOYMENT.md#1-aws-resource-explorer-required))
or set the `OrgViewArn` parameter. Without an org view, the Worker fails every
member-account scan with an error instead of recording empty results.

### Coordinator fails with an Organizations error

The hub must be the management account or a delegated administrator for AWS
Organizations. Otherwise `organizations:ListAccounts` is denied, the
Coordinator raises an error, and `ti-orches-errors-ENV` fires.

### Worker failures queue has messages

In fan-out mode, the Coordinator invokes Workers asynchronously and closes the
cycle, so the next run starts a fresh one. Invocations that fail after retries
go to `ti-worker-failures-ENV` and raise the alarm of the same name. Read the
messages and the Worker logs to find the cause:

```bash
aws sqs receive-message \
  --queue-url https://sqs.${REGION}.amazonaws.com/${ACCOUNT}/ti-worker-failures-${ENV} \
  --max-number-of-messages 10 --region ${REGION} --profile ${AWS_PROFILE}
```

The next scan cycle rescans the affected accounts.

### Monthly aggregation

The Monthly Aggregator runs on day 2 of each month. It compacts the most recent
calendar month that ended at least 28 days before the run. A run on November 2
compacts September. It writes the month to `resource_tag_deltas_historical`
with Athena `UNLOAD`, then deletes those daily partitions from
`resource_tag_deltas_recent`. So the recent table holds roughly the last one to
two months.

If a run fails (`ti-monthly-agg-errors-ENV`), fix the cause and rerun that month
by hand. Objects under `tag-deltas/recent/` expire after 130 days as a safety
net, so rerun before then or the month's daily data is lost.

```bash
aws lambda invoke \
  --function-name ti-monthly-agg-${ENV}-${ACCOUNT} \
  --cli-binary-format raw-in-base64-out \
  --payload '{"year": 2026, "month": 9}' \
  --region ${REGION} --profile ${AWS_PROFILE} /tmp/monthly.json && cat /tmp/monthly.json
```

A rerun is safe. Accounts already compacted for that month are skipped.

---

## Data Retention Policy

| Data                    | Location                                | Retention  | Policy                             |
|-------------------------|-----------------------------------------|------------|------------------------------------|
| Recent tag deltas       | `tag-deltas/recent/`                    | About 1-2 months | Monthly Aggregator compacts and deletes each finished month; S3 lifecycle expires objects after 130 days as a safety net |
| Historical tag deltas   | `tag-deltas/historical/`                | Indefinite | Transitions to S3-IA after 90 days |
| Cost allocation         | `tag-cost-allocation/`                  | Indefinite | Current and previous month rewritten daily |
| Athena query results    | `query-results/`                        | 7 days     | S3 lifecycle auto-deletes          |
| Scan state              | `aggregator-state/*/latest.json.gz`     | Indefinite | Overwritten each scan              |
| Scan checkpoints        | `aggregator-state/scan-checkpoint.json` | Per cycle  | Deleted on scan completion         |
| Scan completion markers | `aggregator-state/completed/`           | Indefinite | Audit trail                        |
