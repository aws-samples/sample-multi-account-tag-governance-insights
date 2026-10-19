# Multi-Account Tag Governance Insights (TagInsights)

> **New here?** Start with [DEPLOYMENT.md](docs/DEPLOYMENT.md), then use
> [USAGE.md](docs/USAGE.md) to find tagging gaps in the core Athena data.
> [OPERATIONS.md](docs/OPERATIONS.md) covers monitoring and troubleshooting.

## Overview

TagInsights is a serverless platform for AWS resource tagging governance at scale. It discovers resources across all
accounts and regions in an AWS Organization using **AWS Resource Explorer**, tracks tag changes in real time, and
provides analytics for cost attribution and tagging gaps.

---

## Features

- **Multi-account tag collection** — Coordinator with resumable checkpoint-based scanning across all org accounts
- **Real-time tag change detection** — EventBridge forwards CloudTrail tag API calls from 45 AWS services (requires a CloudTrail trail)
- **Resource Explorer-based discovery** — Org-wide RE view via direct boto3 calls for 300+ resource types, no
  per-service permissions needed
- **Delta-based processing** — Only changed resources are processed each scan
- **Parquet analytics** — Tag data stored as partitioned Parquet for efficient Athena querying
- **Tag-based cost analysis** — Matches resource ARNs to CUR data directly via Athena; Cost Explorer fallback opt-in

---

## Architecture

TagInsights uses a hub-and-spoke model:

- **Hub account** — All Lambda functions run here: Coordinator, Worker, Analytics Writer, Monthly Aggregator, Cost
  Lambdas
- **Central analytics** — S3 bucket (`ti-central-analytics-REGION-ENV-ACCOUNT`), Glue/Athena, SQS queue (
  `ti-central-results-ENV-ACCOUNT`)
- **Worker** — Uses org-wide Resource Explorer view to scan all accounts from the hub; no per-account deployments needed
- **EventForwarder StackSet** — Optional: deploys EventBridge forwarding rules to member accounts for real-time
  tag-change events

See [ARCHITECTURE.md](docs/ARCHITECTURE.md) for full technical details and [ORCHESTRATION.md](docs/ORCHESTRATION.md) for data flow
diagrams.

---

## Prerequisites

### AWS Resource Explorer

Enable Resource Explorer org-wide and create an org-scoped view **named `org`**
in the hub account before deploying. See
[DEPLOYMENT.md → AWS Resource Explorer](docs/DEPLOYMENT.md#1-aws-resource-explorer-required).

Resource Explorer is free and provides a single `resource-explorer-2:Search` permission for 300+ resource types.

### AWS CloudTrail

Real-time tag tracking needs a CloudTrail trail logging management events in
every member account and region, ideally one organization trail. Without it,
tag changes appear only after the next scheduled scan. See
[DEPLOYMENT.md → AWS CloudTrail](docs/DEPLOYMENT.md#2-aws-cloudtrail-required-for-real-time-events).

### Required AWS Services

CloudFormation, Lambda, S3, SQS, EventBridge, Athena, Glue, IAM, AWS Organizations, Resource Explorer, CloudTrail

### Deployer Permissions

The deploying user/role needs permissions to create CloudFormation stacks, IAM roles, Lambda functions, S3 buckets, SQS
queues, EventBridge rules, Glue databases, and Athena workgroups.

---

## Quick Start

### Steps 1-3: Build, upload, and deploy

Follow [DEPLOYMENT.md](docs/DEPLOYMENT.md): build the Lambda packages, upload them
with the templates under one `DEPLOYMENT_VERSION`, then create the root
`deploy.yaml` stack.

### Step 4: Trigger initial scan

```bash
ACCOUNT=$(aws sts get-caller-identity --query Account --output text --profile ${AWS_PROFILE})

aws lambda invoke \
  --function-name ti-orches-${ENV}-${ACCOUNT} \
  --region ${REGION} \
  --cli-binary-format raw-in-base64-out \
  --payload '{"trigger_source": "manual"}' \
  /tmp/response.json --profile ${AWS_PROFILE} && cat /tmp/response.json
```

### Step 5: Query data in Athena

Select workgroup: `ti-REGION-ENV-ACCOUNT`

```sql
-- Verify data is flowing
SELECT COUNT(*), account_id
FROM "ti_REGION_ENV_ACCOUNT".resource_tag_deltas_recent
GROUP BY account_id;
```

---

## Resource Naming

All TagInsights resources use the `ti-` prefix:

| Resource                  | Name Pattern                                    |
|---------------------------|-------------------------------------------------|
| Coordinator Lambda        | `ti-orches-{ENV}-{ACCOUNT}`                     |
| Worker Lambda             | `ti-worker-{ENV}-{ACCOUNT}`                     |
| Analytics Writer Lambda   | `ti-analytics-{ENV}-{ACCOUNT}`                  |
| Monthly Aggregator Lambda | `ti-monthly-agg-{ENV}-{ACCOUNT}`                |
| Cost Coordinator Lambda   | `ti-cost-orches-{ENV}-{ACCOUNT}`                |
| Cost Worker Lambda        | `ti-cost-worker-{ENV}-{ACCOUNT}`                |
| Cost Deduplicator Lambda  | `ti-cost-dedup-{ENV}-{ACCOUNT}`                 |
| Central Analytics S3      | `ti-central-analytics-{REGION}-{ENV}-{ACCOUNT}` |
| Athena Results S3         | `ti-athena-results-{REGION}-{ENV}-{ACCOUNT}`    |
| Results SQS Queue         | `ti-central-results-{ENV}-{ACCOUNT}`            |
| Cost Work FIFO Queue      | `ti-cost-analysis-work-{ENV}.fifo`              |
| Athena Workgroup          | `ti-{REGION}-{ENV}-{ACCOUNT}`                   |
| Glue Database             | `ti_{REGION}_{ENV}_{ACCOUNT}`                   |
| EventForwarder StackSet   | `TagInsights-EventForwarder`                    |

---

## Documentation

| Document                                                           | Contents                                                      |
|--------------------------------------------------------------------|---------------------------------------------------------------|
| [QUICK-START.md](docs/QUICK-START.md)                                   | Fast deployment and verification steps                        |
| [DEPLOYMENT.md](docs/DEPLOYMENT.md)                                     | Step-by-step deployment guide with all parameters             |
| [USAGE.md](docs/USAGE.md)                                               | Core Athena queries for finding tagging gaps                  |
| [ARCHITECTURE.md](docs/ARCHITECTURE.md)                                 | System architecture, data model, technology choices           |
| [ORCHESTRATION.md](docs/ORCHESTRATION.md)                               | Lambda data flow diagrams, trigger commands                   |
| [OPERATIONS.md](docs/OPERATIONS.md)                                     | Monitoring, troubleshooting, stack management                 |
| [SECURITY.md](docs/SECURITY.md)                                         | IAM design, encryption, compliance notes                      |
| [SOFTWARE_DESIGN_DOCUMENT.md](docs/SOFTWARE_DESIGN_DOCUMENT.md)         | Component design, data schemas, deployment design             |
| [cost-analysis/README.md](docs/cost-analysis/README.md)                 | Cost analysis feature (CUR matching + Cost Explorer fallback) |
| [cost-analysis/QUERY_EXAMPLES.md](docs/cost-analysis/QUERY_EXAMPLES.md) | Athena queries for cost analysis                              |

---

## Cost Estimate

TagInsights uses serverless services — you pay only for what you use. These are rough estimates, not benchmarks;
costs scale mainly with tag change volume and how often you query Athena.

| Deployment size | Resources     | Estimated monthly cost |
|-----------------|---------------|------------------------|
| Small           | < 10,000      | $5–$10                 |
| Medium          | 10,000–50,000 | $10–$30                |
| Large           | 50,000+       | $30–$100               |

**Primary cost drivers:** Lambda invocations, S3 storage, Athena queries  
**Note:** AWS Resource Explorer is free.

---

## Source Code Layout

```
src/
├── __init__.py
├── tag_manager.py
├── lambda_handlers/          # Lambda entry points
│   ├── aggregator_coordinator.py
│   ├── aggregator_worker.py
│   ├── analytics_writer_handler.py
│   ├── monthly_aggregator_handler.py
│   └── cost/                 # Cost analysis handlers + library
│       ├── cost_analyzer_coordinator_handler.py
│       ├── cost_analyzer_worker_handler.py
│       ├── cost_deduplicator_handler.py
│       └── ...
├── tag_retrievers/           # Resource Explorer integration
└── utils/                    # Shared utilities

deployment/                   # CloudFormation templates and build scripts
├── core-stack.yaml
├── cost-analysis-stack.yaml
├── aggregator-stack.yaml
├── package_lambdas.py        # Build script
└── lambda_builds/            # Output ZIPs
```

---

## Known Limitations

- **Real-time events need CloudTrail.** Member accounts must have a CloudTrail trail logging management events
  (ideally an organization trail). Without one, tag changes appear only after the next daily scan.
- **The management account is not tracked in real time.** Service-managed StackSets never deploy to it; its tag
  changes are picked up by the daily scan.
- **`last_month_cost` is not populated yet (always 0).** Month-over-month cost queries return no rows until it is.
  `current_month_mtd`, `avg_daily_cost` and `current_month_projected` are populated.
- **`tag_costs` lags one day.** The deduplicator compacts the previous day's results. To compact today's results,
  invoke it with `{"process_date": "YYYY-MM-DD"}`.
- **Not tested in AWS GovCloud (US) or China Regions.**
- **Very large account-Regions are not sharded.** The worker logs a warning when one account-Region exceeds
  `STATE_SHARD_THRESHOLD` resources (default 50,000).

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.
