# TagInsights - Quick Start Deployment

Fast deployment with pre-configured CloudFormation links.

## Prerequisites

1. ✅ **AWS Resource Explorer** configured with an org-wide aggregator view (
   see [DEPLOYMENT.md](DEPLOYMENT.md#aws-resource-explorer))
2. ✅ **Lambda packages** built and uploaded to S3 bucket (
   see [DEPLOYMENT.md](DEPLOYMENT.md#step-2-upload-lambda-packages))
3. ✅ **IAM permissions** for CloudFormation deployment
4. ✅ **AWS Organizations** — deploying from management account or delegated admin

---

## Manual Deployment (CLI)

### Steps 1-2: Build, upload, and deploy

Follow [DEPLOYMENT.md](DEPLOYMENT.md) Step 1 (build and upload) and Step 2
(create the root `deploy.yaml` stack). Deploy the root stack, not individual
nested templates: `core-stack.yaml` on its own creates storage only, not the
scanner.

### Step 3: Trigger initial scan

```bash
ACCOUNT=$(aws sts get-caller-identity --query Account --output text --profile ${AWS_PROFILE})

aws lambda invoke \
  --function-name ti-orches-${ENV}-${ACCOUNT} \
  --region ${REGION} \
  --cli-binary-format raw-in-base64-out \
  --payload '{"trigger_source": "manual"}' \
  /tmp/response.json --profile ${AWS_PROFILE} && cat /tmp/response.json
# Shows: scan_id, total_count (account+region pairs), completed_count
```

---

## Verification

After deployment completes:

### 1. Check Stack Status

```bash
aws cloudformation describe-stacks \
  --stack-name TagInsights-Core-prod \
  --region us-east-1 \
  --query 'Stacks[0].StackStatus' \
  --profile ${AWS_PROFILE}
# Should return: CREATE_COMPLETE
```

### 2. View Outputs

```bash
aws cloudformation describe-stacks \
  --stack-name TagInsights-Core-prod \
  --region us-east-1 \
  --query 'Stacks[0].Outputs' \
  --profile ${AWS_PROFILE}
```

Key outputs:

- `CentralAnalyticsBucket` — Where tag data is stored (`ti-central-analytics-REGION-ENV-ACCOUNT`)
- `GlueDatabase` — For Athena queries (`ti_REGION_ENV_ACCOUNT`)
- `AthenaWorkgroup` — For analytics (`ti-REGION-ENV-ACCOUNT`)

### 3. Check Scan Progress

```bash
ACCOUNT=$(aws sts get-caller-identity --query Account --output text --profile ${AWS_PROFILE})

# Monitor checkpoint — updates every 10 completed account+region pairs
aws s3 cp \
  s3://ti-central-analytics-us-east-1-prod-${ACCOUNT}/aggregator-state/scan-checkpoint.json \
  - | python3 -m json.tool
```

### 4. Check Data in S3

Wait ~5 minutes after first scan, then check for data:

```bash
# List delta files
aws s3 ls s3://ti-central-analytics-us-east-1-prod-ACCOUNT/deltas/ --recursive --profile ${AWS_PROFILE}

# Should see files like:
# deltas/account_id=123456789012/region=us-east-1/2026-03-12-04-00-00-manual.json
```

### 5. Repair Athena Partitions

```bash
ACCOUNT=$(aws sts get-caller-identity --query Account --output text --profile ${AWS_PROFILE})

aws athena start-query-execution \
  --query-string "MSCK REPAIR TABLE resource_tag_deltas_recent" \
  --query-execution-context "Database=ti_us-east-1_prod_${ACCOUNT}" \
  --work-group "ti-us-east-1-prod-${ACCOUNT}" \
  --region us-east-1 \
  --profile ${AWS_PROFILE}
```

### 6. Query in Athena

Go to Athena console, select workgroup `ti-REGION-ENV-ACCOUNT`, and run:

```sql
-- Check for recent tag changes
SELECT 
    resource_type,
    COUNT(*) as resource_count,
    change_type
FROM "ti_REGION_ENV_ACCOUNT".resource_tag_deltas_recent
WHERE year = YEAR(CURRENT_DATE) AND month = MONTH(CURRENT_DATE)
GROUP BY resource_type, change_type
ORDER BY resource_count DESC
LIMIT 20;
```

---

## Troubleshooting

### Stack Creation Failed

```bash
aws cloudformation describe-stack-events \
  --stack-name TagInsights-Core-prod \
  --region us-east-1 \
  --query 'StackEvents[?ResourceStatus==`CREATE_FAILED`]' \
  --profile ${AWS_PROFILE}
```

**Common issues:**

1. **Missing S3 bucket** — Ensure Lambda packages bucket exists and ZIPs are uploaded
2. **Insufficient permissions** — Need IAM permissions for all resources (CloudFormation, Lambda, S3, SQS, EventBridge,
   Glue, Athena)
3. **Organization ID invalid** — Verify Organization ID format (`o-xxxxxxxxxx`)

### No Data Appearing

1. **Check Resource Explorer** — Must have an org-wide aggregator view (ARN contains `/org/`)
2. **Check Coordinator logs** — `aws logs tail /aws/lambda/ti-orches-prod-ACCOUNT --since 1h --profile ${AWS_PROFILE}`
3. **Check scan checkpoint** — Is scan still in progress?
4. **Trigger manual scan** — Use the lambda invoke command above

### Worker Not Scanning All Accounts

The Worker uses the org-wide RE view. Verify it exists:

```bash
aws resource-explorer-2 list-views --profile ${AWS_PROFILE} | jq '.Views[] | select(contains("org"))'
```

### Cost Analysis Not Working

1. **Configure CUR** — See [cost-analysis/README.md](cost-analysis/README.md)
2. **Set CUR parameters** — `CurDatabaseName` and `CurTableName` must be set
3. **Cross-account CUR** — If CUR is in a different account, set `CurAccountRoleArn`

---

## Next Steps

After successful deployment:

1. ✅ **Set up monitoring** — Check CloudWatch dashboards and alarms (`ti-orches-errors-ENV`, etc.)

2. ✅ **Query your data** — Use Athena or visualization tools

3. ✅ **Read operations guide** — See [OPERATIONS.md](OPERATIONS.md)

4. ✅ **Deploy EventForwarder StackSet** — Enable real-time tag changes from member accounts (
   see [DEPLOYMENT.md](DEPLOYMENT.md))
