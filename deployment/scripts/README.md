# TagInsights Helper Scripts

Deployment itself is covered in [docs/DEPLOYMENT.md](../../docs/DEPLOYMENT.md),
including [production code signing](../../docs/DEPLOYMENT.md#production-code-signing-environmentprod-only).
These scripts are optional helpers for sizing and maintenance tasks
against an existing deployment. They use your current AWS CLI credentials,
except `cleanup-orphaned-resources.sh`, which takes a profile per account.

| Script | Purpose |
|---|---|
| `collect-resource-counts.py` | Count resources per account and region via Resource Explorer, as CSV, for sizing and cost estimates |
| `cleanup-orphaned-resources.sh` | Delete TagInsights resources left behind by a failed or partial deployment |

## collect-resource-counts.py

```bash
python3 deployment/scripts/collect-resource-counts.py --output resource-counts.csv
python3 deployment/scripts/collect-resource-counts.py --output counts.csv \
  --accounts 123456789012,987654321098 --regions us-east-1,us-west-2
```

Requires an org-wide Resource Explorer view (see
[DEPLOYMENT.md → AWS Resource Explorer](../../docs/DEPLOYMENT.md#1-aws-resource-explorer-required)).

## cleanup-orphaned-resources.sh

Removes orphaned S3 buckets, SQS queues, Lambda functions, IAM roles and
CloudWatch alarms/logs that block a CloudFormation redeploy. Hub resources
are protected when you pass `--hub-account`. **This deletes resources; always
start with `--dry-run`.**

```bash
./deployment/scripts/cleanup-orphaned-resources.sh \
  --accounts "111111111111:hub-profile 222222222222:member-profile" \
  --hub-account 111111111111 \
  --environment dev \
  --regions "us-east-1 us-west-2" \
  --dry-run
```

Run `--help` for all options. To remove a healthy deployment, delete the
CloudFormation stacks instead (see
[DEPLOYMENT.md → Uninstall](../../docs/DEPLOYMENT.md#uninstall)).
