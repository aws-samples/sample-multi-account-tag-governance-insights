# TagInsights Helper Scripts

Deployment itself, including optional Lambda code signing, is covered in
[docs/DEPLOYMENT.md](../../docs/DEPLOYMENT.md). This optional helper sizes a
deployment using your current AWS CLI credentials.

| Script | Purpose |
|---|---|
| `collect-resource-counts.py` | Count resources per account and Region via Resource Explorer, as CSV, for sizing |

## collect-resource-counts.py

```bash
python3 deployment/scripts/collect-resource-counts.py --output resource-counts.csv
python3 deployment/scripts/collect-resource-counts.py --output counts.csv \
  --accounts 111122223333,444455556666 --regions us-east-1,us-west-2
```

Requires an org-wide Resource Explorer view (see
[DEPLOYMENT.md → AWS Resource Explorer](../../docs/DEPLOYMENT.md#1-aws-resource-explorer-required)).
The script uses the view with `/org/` in its ARN, like the Worker, or the one you
pass with `--view-arn`; it stops if there is none. It needs
`resource-explorer-2:ListViews` and `resource-explorer-2:Search` (AWS authorizes
`ListResources` with `Search`).
Account and Region come from each result's `OwningAccountId` and `Region`
fields; global resources are counted under `global`.

To remove a deployment, delete the CloudFormation stacks (see
[DEPLOYMENT.md → Uninstall](../../docs/DEPLOYMENT.md#uninstall)).
