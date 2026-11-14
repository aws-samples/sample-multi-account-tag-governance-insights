# TagInsights - Deployment Guide

You deploy TagInsights with CloudFormation. You first build the Lambda packages
with Python, then upload them and the templates to an S3 bucket in your hub
account.

## What You Need

- AWS CLI v2, configured with credentials for your hub account
- Python 3.9 or later with `pip` and access to PyPI (the packaging script
  installs the Lambda dependencies). `boto3` is also needed if you use
  `--upload-bucket` or `--sign`.
- `openssl` (checksums and the checkpoint secret)
- A hub account that is the AWS Organizations management account or a
  delegated administrator. The Coordinator calls `organizations:ListAccounts`
  and fails if it cannot.
- Permissions to create CloudFormation stacks, IAM roles, S3 buckets, Lambda
  functions, SQS queues, EventBridge resources, Glue, and Athena
- An S3 bucket for deployment artifacts, in the hub Region. The nested template
  URLs are virtual-hosted style, so the bucket name must not contain dots.
- AWS Resource Explorer enabled org-wide (see Prerequisites below)
- Enough Lambda concurrency. The stacks reserve concurrency (Worker 20,
  Analytics Writer 20, tag compliance rule 20, Coordinator 1, Cost Allocator 1),
  and Lambda keeps 100 unreserved. A new account with a concurrency quota of 10
  must request a quota increase before you deploy. Check yours with
  `aws lambda get-account-settings`.

---

## Set shell variables

Every command in this guide uses these variables. Set them first, in the shell
you deploy from.

```bash
export AWS_PROFILE="<hub-account-profile>"
export REGION="us-east-1"                  # Region you deploy the stacks in
export HUB_REGION="${REGION}"              # Region of the Resource Explorer aggregator index
export BUCKET_NAME="<code-bucket-name>"    # you create it in Step 1; no dots
export DEPLOYMENT_VERSION="2026-08-24.1"   # new value for every release
export ENVIRONMENT="test"                  # dev, test, cert, uat, staging, or prod
export ACCESS_LOGS_BUCKET="<existing-access-log-bucket>"
export ORG_ID=$(aws organizations describe-organization \
  --query Organization.Id --output text --profile ${AWS_PROFILE})   # for example o-a1b2c3d4e5
export CHECKPOINT_SECRET="$(openssl rand -hex 32)"
```

Store `CHECKPOINT_SECRET` somewhere safe, such as your password manager. Every
stack update must pass the same value (or `UsePreviousValue=true`); a new value
invalidates the scan checkpoint. The stack copies the value into an AWS Secrets
Manager secret, encrypted with the stack's messaging KMS key. The Coordinator
Lambda reads it from there, so it never appears in the function's environment
variables.

Use `ENVIRONMENT=test` while you evaluate. Non-prod environments delete their
data buckets when you delete the stack. `prod` keeps them (see
[Uninstall](#uninstall)).

---

## Prerequisites

### 1. AWS Resource Explorer (required)

TagInsights uses the org-wide Resource Explorer aggregator to scan all accounts
from the hub. Before deploying:

1. **Enable Resource Explorer org-wide** with AWS Systems Manager Quick Setup:
    - Go to **Systems Manager > Quick Setup > Resource Explorer**
    - Deploy to your entire organization
    - This creates local indexes in all member accounts and an aggregator
      index in the hub Region

2. **Create an org-wide view named `org`** in the hub account. Quick Setup does
   not create one. By default TagInsights finds the view by the `/org/` segment
   in its ARN, so name it exactly `org`, or pass the view ARN in the
   `OrgViewArn` parameter. Without an org-scoped view, the Worker fails every
   member-account scan with an error, because those scans would otherwise
   return nothing. The scope ARN uses the
   management account ID, not the hub account ID.
   ```bash
   MANAGEMENT_ACCOUNT_ID=$(aws organizations describe-organization \
     --query Organization.MasterAccountId --output text --profile ${AWS_PROFILE})
   aws resource-explorer-2 create-view \
     --view-name org \
     --scope arn:aws:organizations::${MANAGEMENT_ACCOUNT_ID}:organization/${ORG_ID} \
     --included-properties Name=tags \
     --region ${HUB_REGION} --profile ${AWS_PROFILE}
   ```

3. **Verify the view returns member-account resources.** A new org view can
   take an hour or more to populate.
   ```bash
   aws resource-explorer-2 search \
     --view-arn $(aws resource-explorer-2 list-views --region ${HUB_REGION} \
       --profile ${AWS_PROFILE} --query "Views[?contains(@, '/view/org/')] | [0]" --output text) \
     --query-string "" --max-results 1000 \
     --region ${HUB_REGION} --profile ${AWS_PROFILE} \
     --query 'Resources[].OwningAccountId' --output text | tr '\t' '\n' | sort | uniq -c
   ```
   You should see one line per account in your organization.

### 2. AWS CloudTrail (required for real-time events)

Real-time tag tracking (`EnableRealTimeEvents=true`, the default) reacts to the
native `aws.tag` "Tag Change on Resource" event and to CloudTrail API-call
events such as `TagResource` and `CreateTags`. The CloudTrail events carry the
caller identity, which TagInsights stores in the `changed_by` column.
EventBridge only receives the CloudTrail events in accounts and Regions where a
CloudTrail trail logs management events. Without a trail, only the best-effort
native events are forwarded, and the scheduled scan catches anything missed.

Use a multi-Region **organization trail** from the management account so every
current and future member account is covered:

```bash
aws cloudtrail create-trail \
  --name org-management-events \
  --s3-bucket-name <cloudtrail-bucket-name> \
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
# Skip this loop if you sign packages (see "Optional: Lambda code signing").
for package in aggregator_coordinator aggregator_worker analytics_writer \
  monthly_aggregator cost_allocator tag_compliance_rule tag_compliance_report; do
  put_immutable \
    deployment/lambda_builds/${package}.zip \
    lambda-packages/v${DEPLOYMENT_VERSION}/${package}.zip
done

# Upload every template under one immutable version.
for template in deployment/deploy.yaml deployment/core-stack.yaml \
  deployment/aggregator-stack.yaml deployment/cost-analysis-stack.yaml \
  deployment/tag-compliance-stack.yaml \
  deployment/templates/event-forwarder-stack.yaml \
  deployment/templates/tag-compliance-member-role.yaml; do
  put_immutable \
    "${template}" \
    templates/v${DEPLOYMENT_VERSION}/$(basename "${template}")
done
```

`DeploymentVersion` is the immutable identity shared by every nested stack and
all seven Lambda packages. The root template builds each nested `TemplateURL`
as `https://<bucket>.s3.<region>.amazonaws.com/templates/v<version>/<file>` and
passes the same version to every nested stack that deploys Lambda code. Every
immutable upload includes a base64-encoded SHA-256 checksum, uses AES256
server-side encryption, and rejects overwrites with `--if-none-match '*'`. An
HTTP 412 `PreconditionFailed` response means that the version already exists.
Do not overwrite it. Choose a new `DEPLOYMENT_VERSION` and publish the complete
versioned set again.

### Optional: Lambda code signing (EnableCodeSigning)

Code signing is off by default, in every environment. `Environment=prod` does
not turn it on. To turn it on, set the root parameter `EnableCodeSigning=true`
and sign the packages with `package_lambdas.py --sign`.

With `EnableCodeSigning=true`, each Lambda function loads its code from
`signed/v${DEPLOYMENT_VERSION}/` instead of `lambda-packages/`, and the
functions get a code signing configuration with
`UntrustedArtifactOnDeployment: Enforce`. Unsigned packages are rejected. The
Analytics Writer is the exception: it never gets a code signing configuration,
because it uses the AWS SDK for pandas layer, which your signing profile did
not sign. Its package is still loaded from `signed/`.

AWS Signer signs one exact S3 object version, so the code bucket must have
versioning turned on before you upload the packages.

```bash
# One-time: create the signing profile the templates trust
aws signer put-signing-profile \
  --profile-name taginsights-lambda-signer \
  --platform-id AWSLambda-SHA384-ECDSA \
  --signature-validity-period value=365,type=DAYS \
  --region ${REGION} --profile ${AWS_PROFILE}

# One-time: turn on versioning for the code bucket
aws s3api put-bucket-versioning --bucket ${BUCKET_NAME} \
  --versioning-configuration Status=Enabled \
  --region ${REGION} --profile ${AWS_PROFILE}

# Build, upload, and sign in one step (replaces the package loop in Step 1)
AWS_PROFILE=${AWS_PROFILE} AWS_DEFAULT_REGION=${REGION} \
python3 deployment/package_lambdas.py --cleanup \
  --upload-bucket ${BUCKET_NAME} \
  --version "${DEPLOYMENT_VERSION}" \
  --versioned-only \
  --sign
```

Then run the template loop from Step 1 and add
`ParameterKey=EnableCodeSigning,ParameterValue=true` when you create the stack.
`tag-compliance-stack.yaml` has its own `EnableCodeSigning` parameter.

---

## Step 2: Deploy with CloudFormation (10-15 minutes)

### Option A: AWS Console

1. Open the [CloudFormation console](https://console.aws.amazon.com/cloudformation)
2. Choose **Create stack > With new resources > Amazon S3 URL**
3. Enter the template URL:
   ```
   https://<code-bucket-name>.s3.<region>.amazonaws.com/templates/v<deployment-version>/deploy.yaml
   ```
4. Fill in the parameters (see [Parameter Reference](#parameter-reference))
5. Select both capability acknowledgements:
    - `CAPABILITY_NAMED_IAM`
    - `CAPABILITY_AUTO_EXPAND`
6. Choose **Create stack**

### Option B: AWS CLI

> **Prerequisite: access-log bucket.** Set `ACCESS_LOGS_BUCKET` to an existing S3
> bucket for server access logs, usually your logging baseline's bucket (AWS Control
> Tower or Landing Zone Accelerator). S3 requires it in the same account and Region,
> encrypted with SSE-S3 (not SSE-KMS), with a bucket policy granting `s3:PutObject` to
> `logging.s3.amazonaws.com` ([S3 docs](https://docs.aws.amazon.com/AmazonS3/latest/userguide/enable-server-access-logging.html)).

The commands use the variables from [Set shell variables](#set-shell-variables).
`OrganizationId` is needed for real-time events: it lets member accounts put
events on the hub event bus.

```bash
# Basic deployment (no optional features)
aws cloudformation create-stack \
  --stack-name TagInsights \
  --template-url https://${BUCKET_NAME}.s3.${REGION}.amazonaws.com/templates/v${DEPLOYMENT_VERSION}/deploy.yaml \
  --parameters \
      ParameterKey=Environment,ParameterValue=${ENVIRONMENT} \
      ParameterKey=LambdaCodeBucket,ParameterValue=${BUCKET_NAME} \
      ParameterKey=AccessLogsBucketName,ParameterValue=${ACCESS_LOGS_BUCKET} \
      ParameterKey=DeploymentVersion,ParameterValue=${DEPLOYMENT_VERSION} \
      ParameterKey=CheckpointSigningSecret,ParameterValue=${CHECKPOINT_SECRET} \
      ParameterKey=OrganizationId,ParameterValue=${ORG_ID} \
      ParameterKey=HubRegion,ParameterValue=${HUB_REGION} \
  --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND \
  --region ${REGION} --profile ${AWS_PROFILE}

# With cost analysis enabled (CurBucketName is required)
aws cloudformation create-stack \
  --stack-name TagInsights \
  --template-url https://${BUCKET_NAME}.s3.${REGION}.amazonaws.com/templates/v${DEPLOYMENT_VERSION}/deploy.yaml \
  --parameters \
      ParameterKey=Environment,ParameterValue=${ENVIRONMENT} \
      ParameterKey=LambdaCodeBucket,ParameterValue=${BUCKET_NAME} \
      ParameterKey=AccessLogsBucketName,ParameterValue=${ACCESS_LOGS_BUCKET} \
      ParameterKey=DeploymentVersion,ParameterValue=${DEPLOYMENT_VERSION} \
      ParameterKey=CheckpointSigningSecret,ParameterValue=${CHECKPOINT_SECRET} \
      ParameterKey=OrganizationId,ParameterValue=${ORG_ID} \
      ParameterKey=HubRegion,ParameterValue=${HUB_REGION} \
      ParameterKey=EnableCostAnalysis,ParameterValue=true \
      ParameterKey=CurDatabaseName,ParameterValue=<cur-glue-database> \
      ParameterKey=CurTableName,ParameterValue=<cur-glue-table> \
      ParameterKey=CurBucketName,ParameterValue=<cur-bucket-name> \
  --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND \
  --region ${REGION} --profile ${AWS_PROFILE}
```

For a CUR table in another account, also set `CurCatalogAccountId` (and
`CurKmsKeyArn` for an SSE-KMS bucket). See
[Cross-Account CUR Setup](cost-analysis/README.md#cross-account-cur-setup).

### Monitor Deployment

```bash
# Watch status
aws cloudformation describe-stacks \
  --stack-name TagInsights \
  --query "Stacks[0].StackStatus" \
  --output text --region ${REGION} --profile ${AWS_PROFILE}

# View failed events (for troubleshooting)
aws cloudformation describe-stack-events \
  --stack-name TagInsights \
  --query "StackEvents[?ResourceStatus=='CREATE_FAILED'].[LogicalResourceId,ResourceStatusReason]" \
  --output table --region ${REGION} --profile ${AWS_PROFILE}
```

**Expected time:** 10-15 minutes

---

## Step 3: Deploy Event Forwarder to Member Accounts (real-time events)

The event forwarder deploys EventBridge rules to member accounts, so tag
changes are forwarded to the hub in real time. Before you start:

- A CloudTrail trail must be logging in every member account and Region you
  forward from (see [Prerequisites > AWS CloudTrail](#2-aws-cloudtrail-required-for-real-time-events)).
- StackSets trusted access must be enabled for your organization (one time,
  from the management account):
  ```bash
  aws cloudformation activate-organizations-access \
    --region ${REGION} --profile ${AWS_PROFILE}
  ```
- Service-managed StackSets never deploy to the management account. Tag
  changes in the management account are picked up by the scheduled scan, not in
  real time.

Deploy with CloudFormation StackSets:

```bash
# Get your org root ID
ROOT_ID=$(aws organizations list-roots --query 'Roots[0].Id' --output text \
  --profile ${AWS_PROFILE})

# Create the StackSet (SERVICE_MANAGED deploys to new accounts automatically)
aws cloudformation create-stack-set \
  --stack-set-name TagInsights-EventForwarder \
  --template-url https://${BUCKET_NAME}.s3.${REGION}.amazonaws.com/templates/v${DEPLOYMENT_VERSION}/event-forwarder-stack.yaml \
  --parameters \
      ParameterKey=HubAccountId,ParameterValue=$(aws sts get-caller-identity \
        --query Account --output text --profile ${AWS_PROFILE}) \
      ParameterKey=HubRegion,ParameterValue=${REGION} \
      ParameterKey=Environment,ParameterValue=${ENVIRONMENT} \
  --capabilities CAPABILITY_NAMED_IAM \
  --permission-model SERVICE_MANAGED \
  --auto-deployment Enabled=true,RetainStacksOnAccountRemoval=false \
  --region ${REGION} --profile ${AWS_PROFILE}

# Deploy to all accounts in the org. EventBridge rules are regional, so
# create instances in every Region you cover, and in us-east-1, where global
# services (IAM, CloudFront, Route 53) send their CloudTrail tag events.
aws cloudformation create-stack-instances \
  --stack-set-name TagInsights-EventForwarder \
  --deployment-targets OrganizationalUnitIds=${ROOT_ID} \
  --regions ${REGION} us-west-2 \
  --operation-preferences FailureToleranceCount=5,MaxConcurrentCount=10 \
  --region ${REGION} --profile ${AWS_PROFILE}
```

> **Note:** Without this step, TagInsights still scans all accounts on the
> daily schedule, but there is no real-time detection and `tag_change_events`
> stays empty, so no caller is recorded for a tag change.

---

## Optional: Tag Compliance (AWS Config rule)

`deployment/tag-compliance-stack.yaml` is **not** nested in `deploy.yaml`. It
deploys an organization custom AWS Config rule (`tag_compliance_rule`) and an
optional daily SES report (`tag_compliance_report`). Deploy it separately in
the AWS Config delegated administrator (or management) account, once per Region
you evaluate.

Prerequisites:

- AWS Config recording in the member accounts.
- StackSets trusted access (`aws cloudformation activate-organizations-access`,
  see Step 3).
- Trusted access for organization Config rules, and a registered delegated
  administrator. Run these in the management account:
  ```bash
  aws organizations enable-aws-service-access \
    --service-principal config-multiaccountsetup.amazonaws.com --profile ${AWS_PROFILE}
  aws organizations register-delegated-administrator \
    --account-id <config-admin-account-id> \
    --service-principal config-multiaccountsetup.amazonaws.com --profile ${AWS_PROFILE}
  ```
- For the daily report, an organization Config aggregator in the delegated
  administrator account.

Each member account in `MemberAccountIds` adds one Lambda permission to the
rule function's resource-based policy. Lambda caps that policy at 20 KB, so one
stack fits roughly 60-80 accounts. For more accounts, split them across rule
stacks.

1. Deploy `tag-compliance-member-role.yaml` to every evaluated account with a
   service-managed StackSet. IAM is global, so deploy one StackSet instance per
   account (one Region only). The role serves every Region the rule evaluates.
   ```bash
   ROOT_ID=$(aws organizations list-roots --query 'Roots[0].Id' --output text \
     --profile ${AWS_PROFILE})

   aws cloudformation create-stack-set \
     --stack-set-name TagInsights-TagComplianceMemberRole \
     --template-url https://${BUCKET_NAME}.s3.${REGION}.amazonaws.com/templates/v${DEPLOYMENT_VERSION}/tag-compliance-member-role.yaml \
     --parameters \
         ParameterKey=HubAccountId,ParameterValue=<config-admin-account-id> \
         ParameterKey=Environment,ParameterValue=${ENVIRONMENT} \
     --capabilities CAPABILITY_NAMED_IAM \
     --permission-model SERVICE_MANAGED \
     --auto-deployment Enabled=true,RetainStacksOnAccountRemoval=false \
     --region ${REGION} --profile ${AWS_PROFILE}

   aws cloudformation create-stack-instances \
     --stack-set-name TagInsights-TagComplianceMemberRole \
     --deployment-targets OrganizationalUnitIds=${ROOT_ID} \
     --regions ${REGION} \
     --region ${REGION} --profile ${AWS_PROFILE}
   ```
   StackSets skip the management account. If the rule evaluates it, also
   deploy the template there as a plain stack:
   ```bash
   aws cloudformation create-stack \
     --stack-name TagInsights-TagComplianceMemberRole \
     --template-url https://${BUCKET_NAME}.s3.${REGION}.amazonaws.com/templates/v${DEPLOYMENT_VERSION}/tag-compliance-member-role.yaml \
     --parameters \
         ParameterKey=HubAccountId,ParameterValue=<config-admin-account-id> \
         ParameterKey=Environment,ParameterValue=${ENVIRONMENT} \
     --capabilities CAPABILITY_NAMED_IAM \
     --region ${REGION} --profile <management-account-profile>
   ```
2. Deploy the rule stack from the delegated administrator account. The
   template was uploaded in Step 1. If the delegated administrator is not the
   hub account, its deployer needs read access to the code bucket.

```bash
aws cloudformation create-stack \
  --stack-name TagInsights-TagCompliance \
  --template-url https://${BUCKET_NAME}.s3.${REGION}.amazonaws.com/templates/v${DEPLOYMENT_VERSION}/tag-compliance-stack.yaml \
  --parameters \
      ParameterKey=Environment,ParameterValue=${ENVIRONMENT} \
      ParameterKey=OrganizationId,ParameterValue=${ORG_ID} \
      ParameterKey=LambdaCodeBucket,ParameterValue=${BUCKET_NAME} \
      ParameterKey=DeploymentVersion,ParameterValue=${DEPLOYMENT_VERSION} \
      'ParameterKey=MemberAccountIds,ParameterValue="111122223333,444455556666"' \
  --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND \
  --region ${REGION} --profile <config-admin-account-profile>
```

`MemberAccountIds` must list every account the rule evaluates, including the
delegated administrator account. Set `AggregatorName`, `ReportSender`, and
`ReportRecipients` to enable the daily report. The tag schema is stored in SSM
Parameter Store (`TagSchema`), so you can change it without redeploying.

---

## Step 4: Trigger First Scan

After deployment, trigger the first org-wide scan manually:

```bash
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text \
  --profile ${AWS_PROFILE})

aws lambda invoke \
  --function-name ti-orches-${ENVIRONMENT}-${ACCOUNT_ID} \
  --cli-binary-format raw-in-base64-out \
  --payload '{"trigger_source": "manual_first_scan"}' \
  --region ${REGION} --profile ${AWS_PROFILE} \
  /tmp/scan-response.json && cat /tmp/scan-response.json | python3 -m json.tool
```

Expected response (small org):

```json
{"status": "complete", "total_account_regions": 85, "completed": 85, "errors": 0}
```

For large orgs the response is `"status": "in_progress"`. Invoke it again to
resume. The Coordinator returns an error if the hub cannot call
`organizations:ListAccounts`. Member-account scans fail, and the checkpoint
records errors, if no org-scoped Resource Explorer view exists (see
[Troubleshooting](#troubleshooting)).

---

## What Gets Deployed

**Core Infrastructure** (`core-stack`)

- Central S3 analytics bucket: `ti-central-analytics-{region}-{env}-{account}`
- Athena results bucket: `ti-athena-results-{region}-{env}-{account}`
- Glue database: `ti_{region}_{env}_{account}` + 2 external tables (`resource_tag_deltas_recent`, `resource_tag_deltas_historical`)
- Athena workgroup: `ti-{region}-{env}-{account}`
- Central SQS queue: `ti-central-results-{env}-{account}`
- Hub EventBridge bus: `ti-hub-{env}`

**Aggregator Pipeline** (`aggregator-stack`)

- Coordinator Lambda: `ti-orches-{env}-{account}`. Checkpoint-based org-wide scan.
- Worker Lambda: `ti-worker-{env}-{account}`. Queries the org Resource Explorer view from the hub account.
- Analytics Writer Lambda: `ti-analytics-{env}-{account}`. Converts SQS results to Parquet.
- Monthly Aggregator Lambda: `ti-monthly-agg-{env}-{account}`. Compacts one finished month of daily data into the historical table.
- Worker failure queue: `ti-worker-failures-{env}`. Receives failed asynchronous Worker invocations.

**Optional: Cost Analysis** (`cost-analysis-stack`, when `EnableCostAnalysis=true`)

- Cost Allocator: `ti-cost-alloc-{env}-{account}`. Runs daily at 06:00 UTC and writes `tag_cost_allocation`.

---

## Parameter Reference

### Core Parameters

| Parameter                 | Required        | Description |
|---------------------------|-----------------|-------------|
| `Environment`             | Yes (default `prod`) | `dev`, `test`, `cert`, `uat`, `staging`, or `prod`. Non-prod values delete data buckets on stack removal. |
| `LambdaCodeBucket`        | Yes             | S3 bucket with the Lambda ZIP files and templates. No dots in the name. |
| `DeploymentVersion`       | Yes             | Immutable version shared by all nested templates and Lambda artifacts |
| `CheckpointSigningSecret` | Yes             | HMAC secret for checkpoint integrity, at least 32 characters. Generate with `openssl rand -hex 32`. Keep it: updates must reuse it. Stored in Secrets Manager, not in a Lambda environment variable. |
| `AccessLogsBucketName`    | Yes             | Existing S3 server access-log bucket (same account and Region, SSE-S3, bucket policy grants `logging.s3.amazonaws.com`) |
| `OrganizationId`          | Org deployments | AWS Organizations ID (for example `o-a1b2c3d4e5`). Lets member accounts forward events to the hub event bus. |
| `HubRegion`               | No (default `us-east-1`) | Region of the Resource Explorer aggregator index. Must be a Region code such as `us-east-1`. |

### Optional Features

| Parameter                    | Default | Description |
|------------------------------|---------|-------------|
| `EnableCostAnalysis`         | `false` | CUR-based cost analysis with Athena. Requires `CurBucketName`. |
| `EnableRealTimeEvents`       | `true`  | Real-time tag change detection with EventBridge |
| `EnableEventBridgeExtension` | `true`  | Forward tag events from 40+ additional services |
| `EnableCodeSigning`          | `false` | Enforce AWS Signer code signing and load packages from `signed/v<version>/`. Requires `package_lambdas.py --sign`. Independent of `Environment`. |

### Cost Analysis Parameters (`EnableCostAnalysis=true`)

| Parameter             | Default          | Description |
|-----------------------|------------------|-------------|
| `CurDatabaseName`     | `""`             | Glue database name for the CUR table |
| `CurTableName`        | `""`             | CUR table name (for example `cur`) |
| `CurBucketName`       | `not-configured` | S3 bucket where CUR data is stored. **Required** when `EnableCostAnalysis=true`; a template rule rejects the stack without it. IAM access is scoped to this bucket. |
| `CurCatalogAccountId` | `""`             | Billing account ID when CUR is in another account. Registers its Glue Data Catalog in Athena. The CUR Glue table must be in the hub Region. See [Cross-Account CUR Setup](cost-analysis/README.md#cross-account-cur-setup). |
| `CurKmsKeyArn`        | `""`             | KMS key ARN for a CUR bucket that uses SSE-KMS. The allocator role gets `kms:Decrypt` through S3; the key policy must also allow the allocator role. Leave empty for SSE-S3. |

### Advanced

| Parameter                   | Default             | Description |
|-----------------------------|---------------------|-------------|
| `LogLevel`                  | `WARNING`           | Lambda log level (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |
| `ScanSchedule`              | `cron(0 2 * * ? *)` | Daily scan schedule (2 AM UTC by default) |
| `MaxScanDays`               | `3`                 | Days before the Coordinator switches from sequential to fan-out scan mode |
| `PandasLayerArn`            | `""`                | AWS SDK for pandas layer for the Analytics Writer. Empty uses `arn:<partition>:lambda:<stack-region>:336392948345:layer:AWSSDKPandas-Python314:12`. Layer versions differ by Region; set this if version 12 is not published in your Region. |
| `OrgViewArn`                | `""`                | ARN of the org-scoped Resource Explorer view. Empty auto-discovers a view whose ARN contains `/org/`. |
| `ForceDeleteOnStackRemoval` | `false`             | **Prod only.** Set `true` to delete data buckets and other retained resources on stack removal. Non-prod always deletes. |

---

## Verify Deployment

```bash
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text \
  --profile ${AWS_PROFILE})

# 1. Check Lambda functions exist
aws lambda list-functions \
  --query 'Functions[?starts_with(FunctionName, `ti-`)].FunctionName' \
  --output table --region ${REGION} --profile ${AWS_PROFILE}

# 2. Check Glue tables exist
aws glue get-tables \
  --database-name ti_${REGION}_${ENVIRONMENT}_${ACCOUNT_ID} \
  --query 'TableList[].Name' --output table \
  --region ${REGION} --profile ${AWS_PROFILE}

# 3. Trigger first scan and check response
aws lambda invoke \
  --function-name ti-orches-${ENVIRONMENT}-${ACCOUNT_ID} \
  --cli-binary-format raw-in-base64-out \
  --payload '{"trigger_source": "manual"}' \
  --region ${REGION} --profile ${AWS_PROFILE} /tmp/response.json \
  && cat /tmp/response.json | python3 -m json.tool

# 4. Check scan progress (checkpoint file)
aws s3 cp \
  s3://ti-central-analytics-${REGION}-${ENVIRONMENT}-${ACCOUNT_ID}/aggregator-state/scan-checkpoint.json \
  - --region ${REGION} --profile ${AWS_PROFILE} | python3 -m json.tool

# 5. Verify delta files landing after scan
aws s3 ls \
  s3://ti-central-analytics-${REGION}-${ENVIRONMENT}-${ACCOUNT_ID}/deltas/ \
  --recursive --region ${REGION} --profile ${AWS_PROFILE} | head -20

# 6. Partitions are registered by the Analytics Writer as files land; no
#    MSCK REPAIR is needed. Query resource_tag_deltas_recent in Athena.
```

---

## Update Stack

After you change Lambda code or templates, publish a complete new immutable
version and point the stack at it.

```bash
set -euo pipefail

export DEPLOYMENT_VERSION="2026-08-24.2"   # new, unused version

python3 deployment/package_lambdas.py \
  --cleanup \
  --version "${DEPLOYMENT_VERSION}"
```

Then run both upload loops from [Step 1](#step-1-upload-to-s3-2-minutes) (they
use `put_immutable` and the new `DEPLOYMENT_VERSION`), or the signing command
if you use `EnableCodeSigning=true`. Then update the stack:

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

Keep `UsePreviousValue=true` for every parameter you do not change. A
parameter you leave out reverts to its template default. For example, leaving
out `EnableCostAnalysis` sets it to `false` and deletes the cost analysis
stack, and leaving out `CheckpointSigningSecret` fails the update. If the
running stack was created from an older template that lacks a parameter (such
as `EnableCodeSigning`), `UsePreviousValue=true` fails for that parameter.
Remove that line so it takes its default, or pass a value.

---

## Uninstall

Remove the stacks in this order. Run each command in the account named in its
comment.

```bash
ROOT_ID=$(aws organizations list-roots --query 'Roots[0].Id' --output text \
  --profile ${AWS_PROFILE})

# Waits for a StackSet operation; delete-stack-set fails while instances remain.
wait_stack_set_operation() {
  local stack_set="$1" operation_id="$2" status
  while true; do
    status=$(aws cloudformation describe-stack-set-operation \
      --stack-set-name "${stack_set}" --operation-id "${operation_id}" \
      --query StackSetOperation.Status --output text \
      --region "${REGION}" --profile "${AWS_PROFILE}")
    case "${status}" in
      SUCCEEDED) return 0 ;;
      FAILED|STOPPED) echo "${stack_set}: operation ${status}" >&2; return 1 ;;
    esac
    sleep 30
  done
}

# 1. Tag compliance rule stack (Config delegated administrator account), if deployed
aws cloudformation delete-stack --stack-name TagInsights-TagCompliance \
  --region ${REGION} --profile <config-admin-account-profile>
aws cloudformation wait stack-delete-complete --stack-name TagInsights-TagCompliance \
  --region ${REGION} --profile <config-admin-account-profile>

# 2. Tag compliance member role StackSet (management account), if deployed
OP_ID=$(aws cloudformation delete-stack-instances \
  --stack-set-name TagInsights-TagComplianceMemberRole \
  --deployment-targets OrganizationalUnitIds=${ROOT_ID} \
  --regions ${REGION} --no-retain-stacks \
  --query OperationId --output text \
  --region ${REGION} --profile ${AWS_PROFILE})
wait_stack_set_operation TagInsights-TagComplianceMemberRole "${OP_ID}"
aws cloudformation delete-stack-set \
  --stack-set-name TagInsights-TagComplianceMemberRole \
  --region ${REGION} --profile ${AWS_PROFILE}
# Also delete the plain TagInsights-TagComplianceMemberRole stack in the
# management account if you created one.

# 3. Event forwarder StackSet (management account), if deployed.
#    Use the same Regions you deployed to.
OP_ID=$(aws cloudformation delete-stack-instances \
  --stack-set-name TagInsights-EventForwarder \
  --deployment-targets OrganizationalUnitIds=${ROOT_ID} \
  --regions ${REGION} us-west-2 --no-retain-stacks \
  --query OperationId --output text \
  --region ${REGION} --profile ${AWS_PROFILE})
wait_stack_set_operation TagInsights-EventForwarder "${OP_ID}"
aws cloudformation delete-stack-set \
  --stack-set-name TagInsights-EventForwarder \
  --region ${REGION} --profile ${AWS_PROFILE}

# 4. Root stack (hub account)
aws cloudformation delete-stack --stack-name TagInsights \
  --region ${REGION} --profile ${AWS_PROFILE}
aws cloudformation wait stack-delete-complete --stack-name TagInsights \
  --region ${REGION} --profile ${AWS_PROFILE}
```

**What is retained.** With `Environment=prod` and
`ForceDeleteOnStackRemoval=false`, stack deletion keeps these resources to
protect data:

- S3 buckets `ti-central-analytics-*` and `ti-athena-results-*`
- The hub event bus, the messaging KMS key, and the SQS queues (results,
  dead-letter, forwarded events, worker failures)
- The Coordinator, Worker, Analytics Writer, and Monthly Aggregator functions
  and their IAM roles
- The checkpoint signing secret in AWS Secrets Manager (about USD 0.40 per
  month)

Non-prod environments delete all of them. Retained resources keep their names,
so a new deployment with the same `Environment` in the same account fails until
you delete them. Your code bucket and the access-log bucket are not managed by
the stacks.

**Deleting a retained bucket.** The data buckets have versioning on.
`aws s3 rb --force` deletes only current objects, so it fails on a versioned
bucket. Delete every object version first, for example with the S3 console
**Empty** action, or:

```bash
python3 -c "import boto3, sys; boto3.resource('s3').Bucket(sys.argv[1]).object_versions.delete()" \
  ti-central-analytics-${REGION}-prod-<account-id>
aws s3api delete-bucket --bucket ti-central-analytics-${REGION}-prod-<account-id> \
  --region ${REGION} --profile ${AWS_PROFILE}
```

This permanently deletes the tag history. Run it only when you mean to.

---

## Troubleshooting

### Lambda "NoSuchKey" during stack creation

**Cause:** Lambda ZIP files not found at the expected S3 path.
**Fix:** Verify the upload: `aws s3 ls s3://<code-bucket-name>/lambda-packages/v${DEPLOYMENT_VERSION}/ --profile ${AWS_PROFILE}`.
With `EnableCodeSigning=true`, check `signed/v${DEPLOYMENT_VERSION}/` instead.

### Coordinator fails with an Organizations error

**Cause:** The hub account is not the management account or a delegated
administrator, so `organizations:ListAccounts` is denied. The Coordinator stops
instead of scanning only the hub account.
**Fix:** Deploy in the management account, or register the hub account as a
delegated administrator for AWS Organizations.

### Member-account scans fail because no org view exists

**Symptom:** Worker errors and the `ti-worker-errors-{env}` alarm. The Worker
log says `No org-scoped Resource Explorer view found`.
**Cause:** No Resource Explorer view with `/org/` in its ARN exists in
`HubRegion`, and `OrgViewArn` is empty.
**Fix:**

1. Create the view (see [Prerequisites](#1-aws-resource-explorer-required)), or
   set `OrgViewArn` to your org-scoped view.
2. Check the views: `aws resource-explorer-2 list-views --region ${HUB_REGION} --profile ${AWS_PROFILE}`
3. Enable Resource Explorer in member accounts with Systems Manager Quick Setup.

### No data in Athena after scan

**Cause 1:** The Analytics Writer could not register a partition (it retries through SQS, then sends the record to the dead-letter queue).
**Fix:** Check the Analytics Writer logs and the results DLQ. To recover, run `MSCK REPAIR TABLE resource_tag_deltas_recent;` in the Athena console with database `ti_REGION_ENV_ACCOUNT` selected.

**Cause 2:** Worker Lambda AccessDenied on SQS.
**Fix:** Verify the `ti-worker-{region}-{env}` role has `sqs:SendMessage` on the central results queue.

### Messages in the worker failures queue

**Cause:** Asynchronous Worker invocations from a fan-out failed after retries.
The `ti-worker-failures-{env}` alarm fires.
**Fix:** Read the failed events in the `ti-worker-failures-{env}` queue and the
Worker logs. The next scheduled scan starts a fresh cycle and rescans every
account.

### ReservedConcurrentExecutions error during deployment

**Cause:** The account has too little Lambda concurrency.
**Symptom:** `decreases account's UnreservedConcurrentExecution below its minimum value of [100]`
**Fix:** Check the quota with `aws lambda get-account-settings`, then request a
concurrency quota increase in Service Quotas.

### Cost analysis "Unable to verify/create output bucket"

**Cause:** Cost Lambda IAM role missing S3 permissions on the Athena results bucket.
**Fix:** Add `s3:GetObject/PutObject/ListBucket/GetBucketLocation` on `ti-athena-results-{region}-{env}-{account}` to
the cost Lambda role.

---

## Architecture Summary

```
Hub Account
├── ti-orches-{env}-{account}      Coordinator: discovers all org accounts, builds scan pairs
├── ti-worker-{env}-{account}      Worker: queries org-wide RE view, writes deltas + SQS notify
├── ti-analytics-{env}-{account}   Analytics Writer: SQS to Parquet converter
├── ti-monthly-agg-{env}-{account} Monthly Aggregator: compacts finished months
├── Central S3 bucket              Tag delta data (Parquet, queryable with Athena)
├── Glue + Athena                  Analytics query layer
└── Central SQS queue              Connects Worker to Analytics Writer

EventBridge StackSet → all member accounts
└── Event Forwarder Rules
    └── Tag change events → Hub Event Bus → Worker (real-time path)
```

**Key:** The Worker runs in the **hub account only** and uses the org-wide
Resource Explorer view to discover resources across all member accounts. Tag
collection needs no StackSet.
