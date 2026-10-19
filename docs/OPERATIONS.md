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

Wait 5 minutes after scan completes, then repair Athena partitions. Select database `ti_REGION_ENV_ACCOUNT` in the Athena query editor first (these DDL
statements use the selected database; the name is not double-quoted here):

```sql
MSCK REPAIR TABLE resource_tag_deltas_recent;
MSCK REPAIR TABLE resource_tag_deltas_historical;
MSCK REPAIR TABLE tag_costs;
```

---

## Ongoing Operations

### Automatic Collection

Data collection runs automatically — no manual intervention required:

- **Tag changes** → EventBridge CloudTrail rules detect changes in real time for the 45 services listed in `deployment/templates/event-forwarder-stack.yaml` (requires a CloudTrail trail; see DEPLOYMENT.md)
- **Daily scan** → Coordinator runs daily; resumes from checkpoint if scan spans multiple days
- **Fan-out mode** → Coordinator automatically switches to parallel invocation if scan age exceeds `MaxScanDays`
- **Cost analysis** → Runs every 12 hours (if `EnableCostAnalysis=true`)
- **Monthly aggregation** → Runs on day 2 of each month at 3 AM UTC

### Monitoring

**CloudWatch Alarms** (created automatically by CloudFormation):

- `ti-orches-errors-ENV` — triggers on Coordinator errors
- `ti-worker-errors-ENV` — triggers if worker errors ≥ 5 in 5 minutes
- `ti-analytics-errors-ENV` — triggers if writer errors ≥ 10 in 5 minutes
- `ti-monthly-agg-errors-ENV` — triggers on any error
- `ti-cost-orches-errors-ENV` — triggers on cost coordinator errors (if `EnableCostAnalysis=true`)
- `ti-cost-worker-errors-ENV` — triggers on cost worker errors (if `EnableCostAnalysis=true`)

**Check Lambda logs:**

```bash
# Coordinator (hub account)
aws logs tail /aws/lambda/ti-orches-prod-ACCOUNT --since 1h --profile ${AWS_PROFILE}

# Worker (hub account)
aws logs tail /aws/lambda/ti-worker-prod-ACCOUNT --since 1h --profile ${AWS_PROFILE}

# Analytics Writer (hub account)
aws logs tail /aws/lambda/ti-analytics-prod-ACCOUNT --since 1h --profile ${AWS_PROFILE}

# Cost Coordinator (hub account)
aws logs tail /aws/lambda/ti-cost-orches-prod-ACCOUNT --since 1h --profile ${AWS_PROFILE}

# Cost Worker (hub account)
aws logs tail /aws/lambda/ti-cost-worker-prod-ACCOUNT --since 1h --profile ${AWS_PROFILE}

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

**Check cost analysis FIFO queue:**

```bash
aws sqs get-queue-attributes \
  --queue-url https://sqs.REGION.amazonaws.com/ACCOUNT/ti-cost-analysis-work-prod.fifo \
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

Or via CLI:

```bash
aws cloudformation update-stack \
  --stack-name TagInsights \
  --template-url https://YOUR-BUCKET.s3.amazonaws.com/templates/deploy.yaml \
  --parameters \
      ParameterKey=Environment,UsePreviousValue=true \
      ParameterKey=HubRegion,UsePreviousValue=true \
      ParameterKey=OrganizationId,UsePreviousValue=true \
      ParameterKey=LambdaCodeBucket,UsePreviousValue=true \
      ParameterKey=EnableCostAnalysis,UsePreviousValue=true \
      ParameterKey=EnableRealTimeEvents,UsePreviousValue=true \
  --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND \
  --profile ${AWS_PROFILE}
```

### Delete Stack (uninstall)

```bash
aws cloudformation delete-stack --stack-name TagInsights --profile ${AWS_PROFILE}

# Monitor deletion
aws cloudformation wait stack-delete-complete --stack-name TagInsights --profile ${AWS_PROFILE}
```

**Note:** For `prod` environments, S3 buckets have `DeletionPolicy: Retain`. Delete them manually if needed:

```bash
aws s3 rb s3://ti-central-analytics-REGION-prod-ACCOUNT --force --profile ${AWS_PROFILE}
aws s3 rb s3://ti-access-logs-REGION-prod-ACCOUNT --force --profile ${AWS_PROFILE}
```

For non-prod (`dev`/`test`/`cert`/`uat`/`staging`) — all buckets are automatically emptied and deleted.

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
4. Run `MSCK REPAIR TABLE resource_tag_deltas_recent` in Athena (workgroup: `ti-REGION-prod-ACCOUNT`, database: `ti_REGION_prod_ACCOUNT`)
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

### Cost analysis DLQ has messages

Cost Worker failed to process some batches. Check the Worker logs:

```bash
aws logs tail /aws/lambda/ti-cost-worker-prod-ACCOUNT --since 2h --profile ${AWS_PROFILE}
```

Common causes: CUR table not accessible, missing Glue permissions, CUR schema changed.

### Worker not scanning all accounts

The Worker uses the Organization-wide Resource Explorer view (ARN contains `/org/`). Verify the org view exists:

```bash
aws resource-explorer-2 list-views --profile ${AWS_PROFILE} | jq '.Views[] | select(contains("org"))'
```

If missing, create a new index and view scoped to the organization root.

---

## Data Retention Policy

| Data                    | Location                                | Retention  | Policy                             |
|-------------------------|-----------------------------------------|------------|------------------------------------|
| Recent tag deltas       | `tag-deltas/recent/`                    | 95 days    | S3 lifecycle auto-deletes          |
| Historical tag deltas   | `tag-deltas/historical/`                | Indefinite | Transitions to S3-IA after 90 days |
| Cost data               | `tag-costs/`                            | 7 days     | S3 lifecycle auto-deletes          |
| Athena query results    | `query-results/`                        | 7 days     | S3 lifecycle auto-deletes          |
| Scan state              | `aggregator-state/*/latest.json.gz`     | Indefinite | Overwritten each scan              |
| Scan checkpoints        | `aggregator-state/scan-checkpoint.json` | Per cycle  | Deleted on scan completion         |
| Scan completion markers | `aggregator-state/completed/`           | Indefinite | Audit trail                        |
| Access logs             | `ti-access-logs-*`                      | 1 year     | Transitions to S3-IA after 90 days |
