# TagInsights - Deployment Guide

**No Python required.** Deploy TagInsights using pre-built Lambda packages and CloudFormation.

## What You Need

- AWS CLI installed and configured with your hub account credentials
- AWS account with CloudFormation, IAM, S3, Lambda, Glue, and Athena permissions
- An S3 bucket to host deployment artifacts (in the same region as your hub)
- AWS Resource Explorer enabled org-wide (see Prerequisites below)

---

## Prerequisites

### 1. AWS Resource Explorer (required)

TagInsights uses the org-wide RE aggregator to scan all accounts from the hub. Before deploying:

1. **Enable Resource Explorer org-wide** via AWS Systems Manager Quick Setup:
    - Go to **Systems Manager → Quick Setup → Resource Explorer**
    - Deploy to your entire organization
    - This creates local indexes in all member accounts and links them to the hub aggregator

2. **Create an org-wide view named `org`** in the hub account. Quick Setup does
   not create one. TagInsights finds the view by the `/org/` segment in its ARN,
   so the name must be exactly `org`; any other view only returns hub-account
   resources.
   ```bash
   ORG_ID=$(aws organizations describe-organization \
     --query Organization.Id --output text --profile ${AWS_PROFILE})
   HUB_ACCOUNT_ID=$(aws sts get-caller-identity \
     --query Account --output text --profile ${AWS_PROFILE})
   aws resource-explorer-2 create-view \
     --view-name org \
     --scope arn:aws:organizations::${HUB_ACCOUNT_ID}:organization/${ORG_ID} \
     --included-properties Name=tags \
     --region us-east-1 --profile ${AWS_PROFILE}
   ```

3. **Verify the view returns member-account resources** (a new org view can
   take an hour or more to populate):
   ```bash
   aws resource-explorer-2 search \
     --view-arn $(aws resource-explorer-2 list-views --region us-east-1 \
       --profile ${AWS_PROFILE} --query "Views[?contains(@, '/view/org/')] | [0]" --output text) \
     --query-string "" --max-results 1000 \
     --region us-east-1 --profile ${AWS_PROFILE} \
     --query 'Resources[].OwningAccountId' --output text | tr '\t' '\n' | sort | uniq -c
   ```
   You should see one line per account in your organization.

### 2. AWS CloudTrail (required for real-time events)

Real-time tag tracking (`EnableRealTimeEvents=true`, the default) reacts to
CloudTrail API-call events such as `TagResource` and `CreateTags`. EventBridge
only receives these events in accounts and regions where a CloudTrail trail is
logging management events. Without a trail, nothing is forwarded and tag
changes appear only after the next scheduled scan.

Use a multi-region **organization trail** from the management account so every
current and future member account is covered:

```bash
aws cloudtrail create-trail \
  --name org-management-events \
  --s3-bucket-name YOUR-CLOUDTRAIL-BUCKET \
  --is-organization-trail \
  --is-multi-region-trail \
  --region ${REGION} --profile ${AWS_PROFILE}
aws cloudtrail start-logging --name org-management-events \
  --region ${REGION} --profile ${AWS_PROFILE}
```

The bucket needs the standard CloudTrail bucket policy (see
[Amazon S3 bucket policy for CloudTrail](https://docs.aws.amazon.com/awscloudtrail/latest/userguide/create-s3-bucket-policy-for-cloudtrail.html)).
If your organization already has an org trail logging management events, you
are done. The first copy of management events is free; you pay only for S3
storage.

### 3. Build Lambda packages

```bash
git clone https://github.com/aws-samples/sample-multi-account-tag-governance-insights.git
cd sample-multi-account-tag-governance-insights

# Build all 7 Lambda ZIP files
python3 deployment/package_lambdas.py \
  --cleanup \
  --version "${DEPLOYMENT_VERSION}"

# Output: deployment/lambda_builds/*.zip
```

---

## Step 1: Upload to S3 (2 minutes)

```bash
set -euo pipefail

export BUCKET_NAME="your-taginsights-bucket"
export REGION="us-east-1"
export AWS_PROFILE="your-hub-account-profile"
export DEPLOYMENT_VERSION="2026-08-24.1"

put_immutable() {
  local source_path="$1"
  local object_key="$2"
  local checksum_sha256
  checksum_sha256=$(openssl dgst -sha256 -binary "${source_path}" | openssl base64 -A)

  aws s3api put-object \
    --bucket "${BUCKET_NAME}" \
    --key "${object_key}" \
    --body "${source_path}" \
    --if-none-match '*' \
    --checksum-sha256 "${checksum_sha256}" \
    --server-side-encryption AES256 \
    --region "${REGION}" \
    --profile "${AWS_PROFILE}"
}

# Reuse an accessible bucket; create it only when S3 reports that it is absent.
if bucket_head_error=$(aws s3api head-bucket \
  --bucket "${BUCKET_NAME}" \
  --region "${REGION}" \
  --profile "${AWS_PROFILE}" 2>&1); then
  :
else
  bucket_head_status=$?
  if [[ "${bucket_head_error}" != *"(404)"* ]]; then
    printf '%s\n' "${bucket_head_error}" >&2
    exit "${bucket_head_status}"
  fi

  if [[ "${REGION}" == "us-east-1" ]]; then
    aws s3api create-bucket \
      --bucket "${BUCKET_NAME}" \
      --region "${REGION}" \
      --profile "${AWS_PROFILE}"
  else
    aws s3api create-bucket \
      --bucket "${BUCKET_NAME}" \
      --create-bucket-configuration "LocationConstraint=${REGION}" \
      --region "${REGION}" \
      --profile "${AWS_PROFILE}"
  fi
fi

# Upload all Lambda packages to immutable versioned keys.
for package in aggregator_coordinator aggregator_worker analytics_writer \
  monthly_aggregator cost_analyzer_coordinator cost_analyzer_worker \
  cost_deduplicator; do
  put_immutable \
    deployment/lambda_builds/${package}.zip \
    lambda-packages/v${DEPLOYMENT_VERSION}/${package}.zip
done

# Upload the root and all three nested templates under one immutable version.
for template in deploy.yaml core-stack.yaml aggregator-stack.yaml \
  cost-analysis-stack.yaml; do
  put_immutable \
    deployment/${template} \
    templates/v${DEPLOYMENT_VERSION}/${template}
done

# Also upload the event forwarder template (from templates/ subfolder)
aws s3 cp deployment/templates/event-forwarder-stack.yaml \
  s3://${BUCKET_NAME}/templates/event-forwarder-stack.yaml --profile ${AWS_PROFILE}
```

`DeploymentVersion` is the immutable identity shared by every nested stack and
all seven Lambda packages. The versioned root template resolves all three nested
`TemplateURL` values from `templates/v${DEPLOYMENT_VERSION}/` and forwards the
same value to every nested stack that deploys Lambda code. Every immutable
upload includes a base64-encoded SHA-256 checksum, uses AES256 server-side
encryption, and rejects overwrites with
`--if-none-match '*'`. An HTTP 412 `PreconditionFailed` response means that
the version already exists; do not overwrite it. Choose a new
`DEPLOYMENT_VERSION` and publish the complete versioned set again.

### Production code signing (Environment=prod only)

With `Environment=prod`, every Lambda enforces AWS Signer code signing and loads
its code from `signed/v${DEPLOYMENT_VERSION}/` instead of `lambda-packages/`.
Unsigned packages are rejected, so a prod deploy fails until you complete these
steps. Other environments (`dev`, `test`, `staging`, ...) skip signing.

```bash
# One-time: create the signing profile the templates trust
aws signer put-signing-profile \
  --profile-name taginsights-lambda-signer \
  --platform-id AWSLambda-SHA384-ECDSA \
  --signature-validity-period value=365,type=DAYS \
  --region ${REGION} --profile ${AWS_PROFILE}

# AWS Signer reads from versioned S3 objects
aws s3api put-bucket-versioning --bucket ${BUCKET_NAME} \
  --versioning-configuration Status=Enabled --profile ${AWS_PROFILE}

# Build, upload, and sign in one step (replaces the package uploads above)
AWS_PROFILE=${AWS_PROFILE} AWS_DEFAULT_REGION=${REGION} \
python3 deployment/package_lambdas.py --cleanup \
  --environment prod \
  --upload-bucket ${BUCKET_NAME} \
  --version "${DEPLOYMENT_VERSION}" \
  --versioned-only
```

Then upload the templates as above and deploy with `ENVIRONMENT=prod`.

---

## Step 2: Deploy with CloudFormation (10-15 minutes)

### Option A: AWS Console (recommended)

1. Open [CloudFormation Console](https://console.aws.amazon.com/cloudformation)
2. **Create stack → With new resources → Amazon S3 URL**
3. Enter template URL:
   ```
   https://YOUR-BUCKET.s3.amazonaws.com/templates/vYOUR-DEPLOYMENT-VERSION/deploy.yaml
   ```
4. Fill in parameters (see Parameter Reference below)
5. Check both capability boxes:
    - ✅ `CAPABILITY_NAMED_IAM`
    - ✅ `CAPABILITY_AUTO_EXPAND`
6. Click **Create stack**

### Option B: AWS CLI

```bash
export BUCKET_NAME="your-taginsights-bucket"
export REGION="us-east-1"
export ORG_ID="o-yourorgid"   # optional, for multi-account
export AWS_PROFILE="your-hub-account-profile"
export DEPLOYMENT_VERSION="2026-08-24.1"
export ENVIRONMENT="test"   # use prod only after "Production code signing" below
export CHECKPOINT_SECRET="$(openssl rand -hex 32)"   # store it; updates must reuse it

# Basic deployment (single account or org, no optional features)
aws cloudformation create-stack \
  --stack-name TagInsights \
  --template-url https://${BUCKET_NAME}.s3.amazonaws.com/templates/v${DEPLOYMENT_VERSION}/deploy.yaml \
  --parameters \
      ParameterKey=Environment,ParameterValue=${ENVIRONMENT} \
      ParameterKey=LambdaCodeBucket,ParameterValue=${BUCKET_NAME} \
      ParameterKey=DeploymentVersion,ParameterValue=${DEPLOYMENT_VERSION} \
      ParameterKey=CheckpointSigningSecret,ParameterValue=${CHECKPOINT_SECRET} \
      ParameterKey=HubRegion,ParameterValue=${REGION} \
      ParameterKey=OrganizationId,ParameterValue=${ORG_ID} \
  --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND \
  --region ${REGION} --profile ${AWS_PROFILE}

# With optional features enabled
aws cloudformation create-stack \
  --stack-name TagInsights \
  --template-url https://${BUCKET_NAME}.s3.amazonaws.com/templates/v${DEPLOYMENT_VERSION}/deploy.yaml \
  --parameters \
      ParameterKey=Environment,ParameterValue=${ENVIRONMENT} \
      ParameterKey=LambdaCodeBucket,ParameterValue=${BUCKET_NAME} \
      ParameterKey=DeploymentVersion,ParameterValue=${DEPLOYMENT_VERSION} \
      ParameterKey=CheckpointSigningSecret,ParameterValue=${CHECKPOINT_SECRET} \
      ParameterKey=HubRegion,ParameterValue=${REGION} \
      ParameterKey=OrganizationId,ParameterValue=${ORG_ID} \
      ParameterKey=EnableCostAnalysis,ParameterValue=true \
      ParameterKey=CurDatabaseName,ParameterValue=athenacurcfn_cur \
      ParameterKey=CurTableName,ParameterValue=cur \
  --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND \
  --region ${REGION} --profile ${AWS_PROFILE}
```

### Monitor Deployment

```bash
# Watch status
aws cloudformation describe-stacks \
  --stack-name TagInsights \
  --query "Stacks[0].StackStatus" \
  --output text --profile ${AWS_PROFILE}

# View failed events (for troubleshooting)
aws cloudformation describe-stack-events \
  --stack-name TagInsights \
  --query "StackEvents[?ResourceStatus=='CREATE_FAILED'].[LogicalResourceId,ResourceStatusReason]" \
  --output table --profile ${AWS_PROFILE}
```

**Expected time:** 10-15 minutes

---

## Step 3: Deploy Event Forwarder to Member Accounts (real-time events)

The event forwarder deploys lightweight EventBridge rules to all member accounts so tag changes are forwarded to the hub
in real-time. Before you start:

- A CloudTrail trail must be logging in every member account and region you
  forward from (see [Prerequisites → AWS CloudTrail](#2-aws-cloudtrail-required-for-real-time-events)).
- StackSets trusted access must be enabled for your organization (one-time, from
  the management account):
  ```bash
  aws cloudformation activate-organizations-access \
    --region ${REGION} --profile ${AWS_PROFILE}
  ```
- Service-managed StackSets never deploy to the management account, so tag
  changes in the hub/management account itself are picked up by the scheduled
  scan, not in real time.

Deploy via CloudFormation StackSets:

```bash
# Get your org root ID
ROOT_ID=$(aws organizations list-roots --query 'Roots[0].Id' --output text \
  --profile ${AWS_PROFILE})

# Create the StackSet (SERVICE_MANAGED = auto-deploys to new accounts)
aws cloudformation create-stack-set \
  --stack-set-name TagInsights-EventForwarder \
  --template-url https://${BUCKET_NAME}.s3.amazonaws.com/templates/event-forwarder-stack.yaml \
  --parameters \
      ParameterKey=HubAccountId,ParameterValue=$(aws sts get-caller-identity \
        --query Account --output text --profile ${AWS_PROFILE}) \
      ParameterKey=HubRegion,ParameterValue=${REGION} \
      ParameterKey=Environment,ParameterValue=${ENVIRONMENT} \
  --capabilities CAPABILITY_NAMED_IAM \
  --permission-model SERVICE_MANAGED \
  --auto-deployment Enabled=true,RetainStacksOnAccountRemoval=false \
  --region ${REGION} --profile ${AWS_PROFILE}

# Deploy to all accounts in the org (adjust regions as needed)
aws cloudformation create-stack-instances \
  --stack-set-name TagInsights-EventForwarder \
  --deployment-targets OrganizationalUnitIds=${ROOT_ID} \
  --regions ${REGION} us-west-2 \
  --operation-preferences FailureToleranceCount=5,MaxConcurrentCount=10 \
  --region ${REGION} --profile ${AWS_PROFILE}
```

> **Note:** This step is optional but recommended. Without it, TagInsights will still scan all accounts on the daily
> schedule — real-time event detection just won't be active.

---

## Step 4: Trigger First Scan

After deployment, trigger the first org-wide scan manually:

```bash
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text \
  --profile ${AWS_PROFILE})
ENV="prod"

aws lambda invoke \
  --function-name ti-orches-${ENV}-${ACCOUNT_ID} \
  --cli-binary-format raw-in-base64-out \
  --payload '{"trigger_source": "manual_first_scan"}' \
  --region ${REGION} --profile ${AWS_PROFILE} \
  /tmp/scan-response.json && cat /tmp/scan-response.json | python3 -m json.tool
```

Expected response (small org):

```json
{"status": "complete", "total_account_regions": 85, "completed": 85, "errors": 0}
```

For large orgs the response will be `"status": "in_progress"` — re-invoke to resume.

---

## What Gets Deployed

✅ **Core Infrastructure** (`core-stack`)

- Central S3 analytics bucket: `ti-central-analytics-{region}-{env}-{account}`
- Athena results bucket: `ti-athena-results-{region}-{env}-{account}`
- Glue database: `ti_{region}_{env}_{account}` + 3 external tables
- Athena workgroup: `ti-{region}-{env}-{account}`
- Central SQS queue: `ti-central-results-{env}-{account}`
- Hub EventBridge bus: `ti-hub-{env}`

✅ **Aggregator Pipeline** (`aggregator-stack`)

- Coordinator Lambda: `ti-orches-{env}-{account}` — checkpoint-based org-wide scan
- Worker Lambda: `ti-worker-{env}-{account}` — queries org RE view from hub account
- Analytics Writer Lambda: `ti-analytics-{env}-{account}` — SQS → Parquet converter
- Monthly Aggregator Lambda: `ti-monthly-agg-{env}-{account}` — 90-day compression

✅ **Optional: Cost Analysis** (`cost-analysis-stack`, when `EnableCostAnalysis=true`)

- Cost Coordinator: `ti-cost-orches-{env}-{account}`
- Cost Worker: `ti-cost-worker-{env}-{account}`
- Cost Deduplicator: `ti-cost-dedup-{env}-{account}`

---

## Parameter Reference

### Core Parameters

| Parameter          | Required        | Description                                                                               |
|--------------------|-----------------|-------------------------------------------------------------------------------------------|
| `Environment`      | Yes             | `dev`, `test`, `cert`, `uat`, `staging`, or `prod`                                        |
| `LambdaCodeBucket` | Yes             | S3 bucket containing Lambda ZIP files                                                     |
| `DeploymentVersion` | Yes            | Immutable version shared by all nested templates and the cost worker artifact             |
| `HubRegion`        | Yes             | Region where RE aggregator index is deployed (default: `us-east-1`)                       |
| `OrganizationId`   | Org deployments | AWS Organization ID (e.g., `o-abc123xyz`) — grants member account access to hub resources |

### Optional Features

| Parameter              | Default | Description                                    |
|------------------------|---------|------------------------------------------------|
| `EnableCostAnalysis`   | `false` | CUR-based cost analysis via Athena             |
| `EnableRealTimeEvents` | `true`  | Real-time tag change detection via EventBridge |

### Cost Analysis Parameters (`EnableCostAnalysis=true`)

| Parameter                    | Default | Description                                                                                                           |
|------------------------------|---------|-----------------------------------------------------------------------------------------------------------------------|
| `CurDatabaseName`            | `""`    | Glue database name for the CUR Athena table                                                                           |
| `CurTableName`               | `""`    | CUR table name (e.g., `cur`)                                                                                          |
| `CurBucketName`              | —       | S3 bucket name where CUR data is stored. **Required** when `EnableCostAnalysis=true`. IAM scoped to this bucket only. |
| `CurAccountRoleArn`          | `""`    | IAM role ARN for cross-account CUR access (leave empty if CUR is in hub account)                                      |
| `EnableCostExplorerFallback` | `false` | Allow Cost Explorer API fallback — charges $0.01/request, opt-in only                                                 |

### Advanced

| Parameter                   | Default             | Description                                                                                               |
|-----------------------------|---------------------|-----------------------------------------------------------------------------------------------------------|
| `LogLevel`                  | `WARNING`           | Lambda log level (`DEBUG`, `INFO`, `WARNING`, `ERROR`)                                                    |
| `ScanSchedule`              | `cron(0 2 * * ? *)` | Daily scan schedule (2 AM UTC by default)                                                                 |
| `MaxScanDays`               | `3`                 | Days before Coordinator switches from sequential to fan-out scan mode                                     |
| `CheckpointSigningSecret`   | —                   | HMAC secret for checkpoint integrity. **Required.** Generate with `openssl rand -hex 32`.                 |
| `ForceDeleteOnStackRemoval` | `false`             | **PROD ONLY.** Set `true` to delete all S3 data on stack removal. Non-prod always deletes.                |

---

## Verify Deployment

```bash
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text \
  --profile ${AWS_PROFILE})
ENV="prod"

# 1. Check Lambda functions exist
aws lambda list-functions \
  --query 'Functions[?starts_with(FunctionName, `ti-`)].FunctionName' \
  --output table --region ${REGION} --profile ${AWS_PROFILE}

# 2. Check Glue tables exist
aws glue get-tables \
  --database-name ti_${REGION}_${ENV}_${ACCOUNT_ID} \
  --query 'TableList[].Name' --output table \
  --region ${REGION} --profile ${AWS_PROFILE}

# 3. Trigger first scan and check response
aws lambda invoke \
  --function-name ti-orches-${ENV}-${ACCOUNT_ID} \
  --cli-binary-format raw-in-base64-out \
  --payload '{"trigger_source": "manual"}' \
  --region ${REGION} --profile ${AWS_PROFILE} /tmp/response.json \
  && cat /tmp/response.json | python3 -m json.tool

# 4. Check scan progress (checkpoint file)
aws s3 cp \
  s3://ti-central-analytics-${REGION}-${ENV}-${ACCOUNT_ID}/aggregator-state/scan-checkpoint.json \
  - --profile ${AWS_PROFILE} | python3 -m json.tool

# 5. Verify delta files landing after scan
aws s3 ls \
  s3://ti-central-analytics-${REGION}-${ENV}-${ACCOUNT_ID}/deltas/ \
  --recursive --profile ${AWS_PROFILE} | head -20

# 6. Register Athena partitions (run once after first scan)
# In the Athena console (workgroup: ti-{region}-{env}-{account}, database: ti_{region}_{env}_{account}):
# MSCK REPAIR TABLE resource_tag_deltas_recent;
# MSCK REPAIR TABLE tag_costs;
```

---

## Update Stack

```bash
set -euo pipefail

put_immutable() {
  local source_path="$1"
  local object_key="$2"
  local checksum_sha256
  checksum_sha256=$(openssl dgst -sha256 -binary "${source_path}" | openssl base64 -A)

  aws s3api put-object \
    --bucket "${BUCKET_NAME}" \
    --key "${object_key}" \
    --body "${source_path}" \
    --if-none-match '*' \
    --checksum-sha256 "${checksum_sha256}" \
    --server-side-encryption AES256 \
    --region "${REGION}" \
    --profile "${AWS_PROFILE}"
}

export DEPLOYMENT_VERSION="2026-08-24.2"

# After updating Lambda code, build and publish a complete new immutable version.
python3 deployment/package_lambdas.py \
  --cleanup \
  --version "${DEPLOYMENT_VERSION}"
for package in aggregator_coordinator aggregator_worker analytics_writer \
  monthly_aggregator cost_analyzer_coordinator cost_analyzer_worker \
  cost_deduplicator; do
  put_immutable \
    deployment/lambda_builds/${package}.zip \
    lambda-packages/v${DEPLOYMENT_VERSION}/${package}.zip
done
for template in deploy.yaml core-stack.yaml aggregator-stack.yaml \
  cost-analysis-stack.yaml; do
  put_immutable \
    deployment/${template} \
    templates/v${DEPLOYMENT_VERSION}/${template}
done

# Update the CloudFormation stack
aws cloudformation update-stack \
  --stack-name TagInsights \
  --template-url https://${BUCKET_NAME}.s3.amazonaws.com/templates/v${DEPLOYMENT_VERSION}/deploy.yaml \
  --parameters \
      ParameterKey=Environment,UsePreviousValue=true \
      ParameterKey=LambdaCodeBucket,UsePreviousValue=true \
      ParameterKey=DeploymentVersion,ParameterValue=${DEPLOYMENT_VERSION} \
      ParameterKey=HubRegion,UsePreviousValue=true \
      ParameterKey=OrganizationId,UsePreviousValue=true \
  --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND \
  --profile ${AWS_PROFILE}
```

---

## Uninstall

```bash
# Delete main stack (removes Lambdas, Glue tables, SQS, EventBus)
aws cloudformation delete-stack --stack-name TagInsights \
  --profile ${AWS_PROFILE}

aws cloudformation wait stack-delete-complete --stack-name TagInsights \
  --profile ${AWS_PROFILE}

# Delete event forwarder StackSet (if deployed)
aws cloudformation delete-stack-instances \
  --stack-set-name TagInsights-EventForwarder \
  --deployment-targets OrganizationalUnitIds=${ROOT_ID} \
  --regions ${REGION} us-west-2 \
  --no-retain-stacks --profile ${AWS_PROFILE}

aws cloudformation delete-stack-set \
  --stack-set-name TagInsights-EventForwarder --profile ${AWS_PROFILE}

echo "TagInsights removed successfully"
```

> **Note:** S3 buckets with `DeletionPolicy: Retain` (prod analytics bucket, access logs) are kept after stack deletion
> to protect data. Delete them manually if needed.

---

## Troubleshooting

### Lambda "NoSuchKey" during stack creation

**Cause:** Lambda ZIP files not found at the expected S3 path  
**Fix:** Verify upload: `aws s3 ls s3://BUCKET/lambda-packages/ --profile PROFILE`

### No data in Athena after scan

**Cause 1:** Athena partitions not registered  
**Fix:** Run `MSCK REPAIR TABLE resource_tag_deltas_recent;` in the Athena console with database `ti_REGION_ENV_ACCOUNT` selected

**Cause 2:** Worker Lambda AccessDenied on SQS  
**Fix:** Verify `ti-worker-{region}-{env}` role has `sqs:SendMessage` on the central results queue

### Coordinator only scans hub account (not member accounts)

**Cause:** Resource Explorer org-wide view not set up  
**Fix:**

1. Verify the hub has an `/org/` view: `aws resource-explorer-2 list-views`
2. Enable RE in member accounts via Systems Manager Quick Setup → Resource Explorer

### ReservedConcurrentExecutions error during deployment

**Cause:** Account has insufficient Lambda concurrency headroom  
**Symptom:** `decreases account's UnreservedConcurrentExecution below its minimum value of [100]`  
**Fix:** Check account concurrency: `aws lambda get-account-settings` — reduce `ReservedConcurrentExecutions` in the
templates to fit within available headroom

### Cost analysis "Unable to verify/create output bucket"

**Cause:** Cost Lambda IAM role missing S3 permissions on Athena results bucket  
**Fix:** Add `s3:GetObject/PutObject/ListBucket/GetBucketLocation` on `ti-athena-results-{region}-{env}-{account}` to
the cost Lambda role

---

## Architecture Summary

```
Hub Account
├── ti-orches-{env}     Coordinator — discovers all org accounts, builds scan pairs
├── ti-worker-{env}     Worker — queries org-wide RE view, writes deltas + SQS notify
├── ti-analytics-{env}  Analytics Writer — SQS → Parquet converter
├── ti-monthly-agg      Monthly Aggregator — compresses old data
├── Central S3 bucket   Tag delta data (Parquet, queryable via Athena)
├── Glue + Athena       Analytics query layer
└── Central SQS queue   Connects Worker → Analytics Writer

EventBridge StackSet → all member accounts
└── Event Forwarder Rules
    └── Tag change events → Hub Event Bus → Worker (real-time path)
```

**Key:** Worker Lambda runs in the **hub account only** and uses the org-wide Resource Explorer view to discover
resources across all member accounts — no StackSet deployment needed for tag collection.
