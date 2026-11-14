# TagInsights - Quick Start Deployment

Fast deployment with pre-configured CloudFormation links.

## Prerequisites

1. **AWS Resource Explorer** with an org-wide aggregator view (see
   [DEPLOYMENT.md](DEPLOYMENT.md#1-aws-resource-explorer-required))
2. **Lambda packages** built and uploaded to your code bucket (see
   [DEPLOYMENT.md](DEPLOYMENT.md#step-1-upload-to-s3-2-minutes))
3. **IAM permissions** for CloudFormation deployment
4. **AWS Organizations**: you deploy from the management account or a delegated
   administrator. The Coordinator fails if it cannot call
   `organizations:ListAccounts`.

---

## Manual Deployment (CLI)

### Steps 1-2: Build, upload, and deploy

Follow [DEPLOYMENT.md](DEPLOYMENT.md): set the shell variables, build and
upload all 7 Lambda packages (Step 1), and create the root `deploy.yaml` stack
named `TagInsights` (Step 2). Deploy the root
stack, not individual nested templates: `core-stack.yaml` on its own creates
storage only, not the scanner.

> **Prerequisite: access-log bucket.** The root stack requires
> `AccessLogsBucketName`: an existing S3 bucket for server access logs, usually
> your logging baseline's bucket (AWS Control Tower or Landing Zone Accelerator).
> S3 requires it in the same account and Region, encrypted with SSE-S3 (not
> SSE-KMS), with a bucket policy granting `s3:PutObject` to
> `logging.s3.amazonaws.com`
> ([S3 docs](https://docs.aws.amazon.com/AmazonS3/latest/userguide/enable-server-access-logging.html)).

### Step 3: Trigger initial scan

The commands below use `ENV`, `REGION`, and `AWS_PROFILE`. Set `ENV` to the
`Environment` you deployed (for example `ENV=test`).

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
  --stack-name TagInsights \
  --region ${REGION} \
  --query 'Stacks[0].StackStatus' \
  --profile ${AWS_PROFILE}
# Should return: CREATE_COMPLETE
```

### 2. View Outputs

```bash
aws cloudformation describe-stacks \
  --stack-name TagInsights \
  --region ${REGION} \
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
  s3://ti-central-analytics-${REGION}-${ENV}-${ACCOUNT}/aggregator-state/scan-checkpoint.json \
  - --profile ${AWS_PROFILE} | python3 -m json.tool
```

### 4. Check Data in S3

Wait ~5 minutes after first scan, then check for data:

```bash
# List delta files
aws s3 ls s3://ti-central-analytics-${REGION}-${ENV}-${ACCOUNT}/deltas/ --recursive --profile ${AWS_PROFILE}

# Should see files like:
# deltas/account_id=123456789012/region=us-east-1/2026-03-12-04-00-00-manual.json
```

### 5. Repair Athena Partitions (recovery only)

The Analytics Writer registers each partition as files land, so this is needed
only if a registration failed (look for records in the results dead-letter
queue).

```bash
ACCOUNT=$(aws sts get-caller-identity --query Account --output text --profile ${AWS_PROFILE})

aws athena start-query-execution \
  --query-string "MSCK REPAIR TABLE resource_tag_deltas_recent" \
  --query-execution-context "Database=ti_${REGION}_${ENV}_${ACCOUNT}" \
  --work-group "ti-${REGION}-${ENV}-${ACCOUNT}" \
  --region ${REGION} \
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
  --stack-name TagInsights \
  --region ${REGION} \
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
2. **Check Coordinator logs**: `aws logs tail /aws/lambda/ti-orches-${ENV}-${ACCOUNT} --since 1h --profile ${AWS_PROFILE}`
3. **Check scan checkpoint** — Is scan still in progress?
4. **Trigger manual scan** — Use the lambda invoke command above

### Worker Not Scanning All Accounts

The Worker uses the org-wide RE view. Verify it exists:

```bash
aws resource-explorer-2 list-views --region ${REGION} --profile ${AWS_PROFILE} \
  | jq '.Views[] | select(contains("/view/org/"))'
```

If your view has another name, set the `OrgViewArn` parameter to its ARN.
Without an org view, member-account scans fail with an error in the Worker log.

### Cost Analysis Not Working

1. **Configure CUR** — See [cost-analysis/README.md](cost-analysis/README.md)
2. **Set CUR parameters**: `CurDatabaseName`, `CurTableName`, and `CurBucketName` must be set
3. **Cross-account CUR** — If CUR is in a different account, set `CurCatalogAccountId` and apply the billing-account
   grants in [cost-analysis/README.md](cost-analysis/README.md#cross-account-cur-setup)

---

## Next Steps

After successful deployment:

1. **Set up monitoring**: check the CloudWatch alarms (`ti-orches-errors-ENV`
   and the others listed in [OPERATIONS.md](OPERATIONS.md#monitoring))
2. **Query your data**: use Athena or visualization tools
3. **Read the operations guide**: see [OPERATIONS.md](OPERATIONS.md)
4. **Deploy the EventForwarder StackSet**: enable real-time tag changes from
   member accounts (see
   [DEPLOYMENT.md](DEPLOYMENT.md#step-3-deploy-event-forwarder-to-member-accounts-real-time-events))
