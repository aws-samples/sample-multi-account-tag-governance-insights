# TagInsights Security

## Reporting Security Vulnerabilities

To report a security vulnerability, please use
the [Amazon vulnerability reporting process](https://aws.amazon.com/security/vulnerability-reporting/) and **do not**
create a public GitHub issue.

---

## Security Architecture

### IAM Design

TagInsights follows least-privilege IAM. Each Lambda has its own dedicated role with only the permissions it needs:

| Role                           | Permissions                                                                                                                                                                                                          |
|--------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `ti-orches-REGION-ENV`         | `organizations:ListAccounts`, `organizations:DescribeOrganization`, `resource-explorer-2:ListIndexes/Search`, S3 read/write to central analytics bucket (aggregator-state prefix), `lambda:InvokeFunction` on Worker |
| `ti-worker-REGION-ENV`         | `resource-explorer-2:Search/ListIndexes/ListViews`, S3 read/write to central analytics bucket, SQS send to central results queue                                                                                     |
| `ti-analytics-REGION-ENV`      | S3 read/write to central analytics bucket, SQS receive/delete from central results queue, Glue partition management                                                                                                  |
| `ti-monthly-agg-REGION-ENV`    | S3 read/write/delete central analytics bucket, Athena query execution, Glue table/partition management                                                                                                               |
| `ti-bucket-emptier-REGION-ENV` | S3 list/delete objects on `ti-*-REGION-ENV-ACCOUNT` buckets (non-prod only)                                                                                                                                          |

### Cross-Account Access

**Tag collection (all modes):** Cross-account access uses **organization-scoped S3 bucket policies** (not IAM roles),
based on `aws:PrincipalOrgID`:

```json
{
  "Condition": {
    "StringEquals": {
      "aws:PrincipalOrgID": "o-yourorgid"
    }
  }
}
```

- **Central Analytics S3 bucket** — Worker Lambda writes directly from the hub account
- **Central SQS queue** — allows org accounts to send messages (event forwarder path)
- **Lambda deployment buckets** — allows org accounts to read Lambda packages (StackSet deployment)

**Cross-account CUR access (optional, cost analysis only):** When `CurAccountRoleArn` is set, the Cost Worker Lambda
uses `sts:AssumeRole` to reach into a separate billing account for CUR Athena queries. The assumed role is scoped to the
specific CUR Glue database, table, and Athena workgroup — no wildcard permissions.
See [cost-analysis/README.md](cost-analysis/README.md#cross-account-cur-setup) for the exact IAM policy.

| Access type                         | Mechanism                            | Scope                                          |
|-------------------------------------|--------------------------------------|------------------------------------------------|
| Tag collection (hub → org view)     | RE org-wide aggregator view          | Worker queries from hub; writes to central S3  |
| Event forwarding (member → hub)     | EventBridge cross-account put        | StackSet-deployed rules forward to hub bus     |
| CUR queries (hub → billing account) | STS AssumeRole (`CurAccountRoleArn`) | Specific Glue DB/table + Athena workgroup      |

---

## Data Security

### Encryption

All data is encrypted at rest and in transit:

| Resource        | Encryption                                    |
|-----------------|-----------------------------------------------|
| All S3 buckets  | AES-256 (SSE-S3) — enforced via bucket policy |
| SQS queues      | Server-side encryption                        |
| Athena results  | SSE-S3 via workgroup configuration            |
| Data in transit | TLS 1.2+ enforced by AWS service endpoints    |

### S3 Public Access

All S3 buckets have `PublicAccessBlockConfiguration` set to fully block public access:

```yaml
BlockPublicAcls: true
BlockPublicPolicy: true
IgnorePublicAcls: true
RestrictPublicBuckets: true
```

### S3 Server Access Logging

All S3 buckets have server access logging enabled to a dedicated `taginsights-access-logs-*` bucket.
Logs are retained for 1 year.

### S3 Versioning

The Central Analytics S3 bucket has versioning enabled for data protection and recovery.

---

## Data Retention and Deletion

### Non-Production Environments

For `dev`/`test`/`cert`/`uat`/`staging` environments:

- All S3 buckets are automatically **emptied and deleted** on stack deletion
- The `BucketEmptier` Custom Resource handles versioned bucket cleanup
- Regional deployment buckets are also deleted

### Production Environments

For `prod` environments:

- S3 buckets have `DeletionPolicy: Retain` — data is **preserved** on stack deletion
- To delete production data, set `ForceDeleteOnStackRemoval=true` on stack update before deleting
    - **Warning:** This permanently destroys all tag analytics data

### Data in Member Accounts

Member accounts do **not** store any TagInsights data. All tag collection runs from the hub account via the
Resource Explorer org-wide view. The only StackSet deployment to member accounts is a lightweight EventBridge rule
(no S3, no Lambda) for real-time tag change forwarding.

---

## Network Security

TagInsights Lambdas do **not** require VPC deployment — they use AWS service endpoints exclusively:

- AWS Resource Explorer API
- S3 (via HTTPS)
- SQS (via HTTPS)
- Glue/Athena (hub Lambdas only)

No inbound network access is required or configured.

---

## Audit Trail

All TagInsights API calls are logged via:

1. **CloudTrail** — all `lambda:Invoke`, `s3:PutObject`, `sqs:SendMessage` etc. calls
2. **S3 Server Access Logs** — all S3 object access
3. **CloudWatch Logs** — Lambda execution logs retained 14 days (worker) / 30 days (aggregator)

---

## Security Controls

### Input Validation

The Analytics Writer Lambda validates all JSON delta files read from S3 before processing:

- **JSON schema validation** — `src/schemas/delta_schema.json` enforces ARN format, `change_type` enum, and tag
  structure limits
- **10MB size limit** — files exceeding the limit are rejected before parsing
- **Quarantine prefix** — rejected files are moved to `quarantine/` in the central analytics bucket for forensic review
- **Implementation:** `analytics_writer_handler.py` — `_validate_delta_file()`, `_quarantine_malicious_file()`

### SQS Rate Limiting

The Worker Lambda implements a per-account in-memory sliding window rate limiter:

- **Limit:** 20 SQS messages / account / minute (configurable via `MAX_SQS_MESSAGES_PER_ACCOUNT_PER_MINUTE`)
- Rate-limited notifications skip the SQS send — deltas remain safely in S3, no data loss
- Emits CloudWatch metric `SQSRateLimitExceeded` with `AccountId` dimension
- **Implementation:** `aggregator_worker.py` — `_is_sqs_rate_limited()`

### Lambda Code Signing

Production Lambda deployments enforce cryptographic code signing via AWS Signer (ECDSA-SHA384):

- `UntrustedArtifactOnDeployment: Enforce` — unsigned or tampered packages are rejected by CloudFormation on prod
- Applied to all 7 packaged Lambda functions across both CFN stacks that deploy them (`aggregator-stack`,
  `cost-analysis-stack`)
- Dev/test/staging environments skip signing for deployment velocity
- One-time setup:
  `aws signer put-signing-profile --profile-name taginsights-lambda-signer --platform-id AWSLambda-SHA384-ECDSA`

### Dependency Vulnerability Scanning

All Python dependencies are scanned for known CVEs before deployment:

- **Tool:** `pip-audit` — scans `requirements.txt` and `deployment/requirements-lambda.txt`
- **CI-friendly:** exits with code 1 on any CVE finding

### Checkpoint Integrity

The Coordinator Lambda protects `scan-checkpoint.json` against tampering:

- **HMAC-SHA256 signatures** applied to checkpoint on every write
- **Signing secret** sourced from `CHECKPOINT_SIGNING_SECRET` environment variable (set via `CheckpointSigningSecret`
  CloudFormation parameter, required). Generate with `openssl rand -hex 32`.
- **Validation on read** — invalid or missing signatures cause the checkpoint to be deleted and a fresh scan to start
- Emits CloudWatch metric `CheckpointTamperingDetected` if a tampered checkpoint is detected
- **Implementation:** `aggregator_coordinator.py` — `_generate_checkpoint_signature()`,
  `_validate_checkpoint_signature()`

### SQL Injection Prevention

All Athena query builders validate user-supplied input before SQL interpolation:

- **Account IDs** — must match `^\d{12}$`
- **Regions** — must match `^[a-z]{2}(-gov)?-[a-z]+-\d$`
- **Service types** — must match `^[a-z][a-z0-9_-]{0,63}$`
- **Tag keys** — must match `^[a-zA-Z0-9_.:/+=@\-]{1,128}$`
- **ARNs** — must match AWS ARN format regex
- **Tag values** — rejected if containing `;`, `'`, `"`, or `\`
- Invalid values are logged and silently dropped from queries (no error thrown to attacker)
- **Implementation:** `cost/query_builder.py`, `monthly_aggregator_handler.py`

---

## Compliance Notes

- S3 public access blocked on all buckets ✅
- S3 encryption at rest (AES256) on all buckets ✅
- Least privilege IAM: separate role per Lambda, resource-specific ARNs on all data-plane permissions ✅
- IAM roles with scoped-down policies per Lambda function ✅
- S3 server access logging enabled on all buckets ✅
- Prod/non-prod separation via `Environment` parameter; code signing enforced on prod only ✅
