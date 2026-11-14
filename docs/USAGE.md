# Using TagInsights: Finding Tagging Gaps

This guide starts after deployment and the first successful scan. Every query
in the core sections uses `resource_tag_deltas_recent`, which is created by the
core stack and does not require Cost Analysis.

In Athena, select:

- Workgroup: `ti-REGION-ENV-ACCOUNT`
- Database: `ti_REGION_ENV_ACCOUNT`

Use the `AthenaWorkgroup` and `GlueDatabase` outputs from the root
CloudFormation stack for the exact deployed values.

The database name contains hyphens (the region, e.g. `ti_us-east-1_prod_123456789012`),
so Athena requires it to be double-quoted in SQL: `"ti_us-east-1_prod_123456789012".resource_tag_deltas_recent`.
Unquoted, the query fails with `mismatched input '-'`. Replace `"ti_REGION_ENV_ACCOUNT"`
below with your quoted database name, and `YOUR_ACCOUNT_ID` with a member account ID.

## Core Table

`resource_tag_deltas_recent` contains:

| Column | Type | Notes |
|---|---|---|
| `arn` | STRING | AWS resource ARN |
| `resource_type` | STRING | Resource type reported by Resource Explorer |
| `tags` | `MAP<STRING,STRING>` | Current tags; query with `element_at`, `cardinality`, or `map_keys` |
| `previous_tags` | `MAP<STRING,STRING>` | Tags before the recorded change |
| `change_type` | STRING | `new_resource`, `tags_changed`, or `resource_removed` from scheduled scans; `new_resource` or `tags_changed` from real-time events |
| `timestamp` | BIGINT | When TagInsights recorded the change, in epoch seconds |
| `source_system` | STRING | `aggregator_worker` (scan), `aggregator_worker_realtime` (CloudTrail event), or `aggregator_worker_tag_event` (native `aws.tag` event) |
| `region` | STRING | Resource region |
| `scan_id` | STRING | Scan cycle or event that produced the row |
| `changed_by` | STRING | Caller ARN when the change was first seen through a CloudTrail event; usually NULL because the native `aws.tag` event arrives first. Use [`tag_change_events`](#who-changed-this-tag) for who made a change |
| `event_name` | STRING | API or event name, for example `TagResource`, `CreateTags`, or `TagChangeOnResource`. NULL for scans |
| `account_id` | STRING | Partition key |
| `year`, `month`, `day` | INT | Partition keys; compare them as integers |

The recent table holds daily data for roughly the last one to two months. On
day 2 of each month, the Monthly Aggregator compacts the most recent month that
ended at least 28 days earlier into `resource_tag_deltas_historical` (one row
per account and resource, with `first_seen_timestamp`, `last_seen_timestamp`,
and `change_count`), then deletes those daily partitions. The historical table
also has `changed_by` and `event_name`, taken from the latest change in the
month that had them.

## Find Untagged Resources

```sql
SELECT arn, resource_type, region
FROM "ti_REGION_ENV_ACCOUNT".resource_tag_deltas_recent
WHERE account_id = 'YOUR_ACCOUNT_ID'
  AND year = YEAR(CURRENT_DATE)
  AND month = MONTH(CURRENT_DATE)
  AND change_type <> 'resource_removed'
  AND cardinality(tags) = 0
ORDER BY resource_type, arn;
```

## Find Resources Missing a Required Tag

```sql
SELECT arn, resource_type, region, tags
FROM "ti_REGION_ENV_ACCOUNT".resource_tag_deltas_recent
WHERE account_id = 'YOUR_ACCOUNT_ID'
  AND year = YEAR(CURRENT_DATE)
  AND month = MONTH(CURRENT_DATE)
  AND change_type <> 'resource_removed'
  AND element_at(tags, 'Owner') IS NULL
ORDER BY resource_type, arn;
```

## Measure Tag Coverage by Resource Type

```sql
SELECT
    resource_type,
    COUNT(*) AS total_resources,
    SUM(CASE WHEN cardinality(tags) > 0 THEN 1 ELSE 0 END) AS tagged_resources,
    ROUND(
        100.0 * SUM(CASE WHEN cardinality(tags) > 0 THEN 1 ELSE 0 END) / COUNT(*),
        1
    ) AS coverage_pct
FROM "ti_REGION_ENV_ACCOUNT".resource_tag_deltas_recent
WHERE account_id = 'YOUR_ACCOUNT_ID'
  AND year = YEAR(CURRENT_DATE)
  AND month = MONTH(CURRENT_DATE)
  AND change_type <> 'resource_removed'
GROUP BY resource_type
ORDER BY coverage_pct, resource_type;
```

## Compare Accounts

```sql
SELECT
    account_id,
    COUNT(*) AS total_resources,
    SUM(CASE WHEN cardinality(tags) = 0 THEN 1 ELSE 0 END) AS untagged_resources
FROM "ti_REGION_ENV_ACCOUNT".resource_tag_deltas_recent
WHERE year = YEAR(CURRENT_DATE)
  AND month = MONTH(CURRENT_DATE)
  AND change_type <> 'resource_removed'
GROUP BY account_id
ORDER BY untagged_resources DESC;
```

## Who Changed This Tag

The delta tables record what changed and when. The native `aws.tag` event
usually arrives first and has no caller, so `changed_by` on a delta is often
NULL. Events are delivered out of order sometimes, so for a resource changed
twice within seconds, order the rows by `timestamp` and expect a brief
reverted value until the next daily scan. The caller comes from CloudTrail and is stored separately, one row per
tag API call, in `tag_change_events` (needs `EnableRealTimeEvents=true` and a
CloudTrail trail). Join the two on the ARN and a short time window:

```sql
-- Who changed CostCenter on this resource, and to what
WITH changes AS (
    SELECT arn, timestamp,
           element_at(previous_tags, 'CostCenter') AS old_value,
           element_at(tags, 'CostCenter') AS new_value
    FROM "ti_REGION_ENV_ACCOUNT".resource_tag_deltas_recent
    WHERE arn = 'arn:aws:ec2:us-east-1:111122223333:instance/i-0123456789abcdef0'
      AND change_type = 'tags_changed'
)
SELECT from_unixtime(c.timestamp) AS changed_at, c.old_value, c.new_value,
       e.changed_by, e.event_name, from_unixtime(e.event_time) AS api_call_at
FROM changes c
LEFT JOIN "ti_REGION_ENV_ACCOUNT".tag_change_events e
  ON e.arn = c.arn
 AND e.event_time BETWEEN c.timestamp - 900 AND c.timestamp + 900
 AND (element_at(e.tags_set, 'CostCenter') IS NOT NULL OR contains(e.tags_removed, 'CostCenter'))
WHERE c.old_value IS DISTINCT FROM c.new_value
ORDER BY c.timestamp DESC;
```

To list recent tag API calls by one principal, query `tag_change_events`
directly, filtered on `year`, `month`, and `day` to limit the data scanned.

`tag_change_events` columns: `event_time` (epoch seconds), `event_id`,
`event_name`, `account_id`, `region`, `arn`, `changed_by` (CloudTrail
`userIdentity` ARN), `principal_type`, `tags_set` (map), `tags_removed`
(array), and partitions `year`, `month`, `day`. Records expire after about
13 months. Tags set at resource creation without a separate tag API call
(for example, `CreateFunction` with tags) have no row here.

## Find Empty or Placeholder Values

```sql
SELECT
    arn,
    resource_type,
    tag_entry.key AS tag_key,
    tag_entry.value AS tag_value
FROM "ti_REGION_ENV_ACCOUNT".resource_tag_deltas_recent
CROSS JOIN UNNEST(tags) AS tag_entry (key, value)
WHERE account_id = 'YOUR_ACCOUNT_ID'
  AND year = YEAR(CURRENT_DATE)
  AND month = MONTH(CURRENT_DATE)
  AND change_type <> 'resource_removed'
  AND (
      trim(tag_entry.value) = ''
      OR lower(tag_entry.value) IN ('tbd', 'todo', 'unknown', 'none')
  )
ORDER BY resource_type, arn, tag_key;
```

## Optional Features

| Feature | Deployment flag | Data used | Query guide |
|---|---|---|---|
| Core tagging data | Always enabled | `resource_tag_deltas_recent`, `resource_tag_deltas_historical` | This guide |
| Cost Analysis | `EnableCostAnalysis=true` | `tag_cost_allocation` | [Cost queries](cost-analysis/QUERY_EXAMPLES.md) |

### Cost allocation table

With Cost Analysis enabled, `tag_cost_allocation` holds CUR usage cost per
resource, day, and tag, using the tags that were on the resource when the cost
was incurred. A daily Lambda function rewrites the current and previous month.

| Column | Type | Notes |
|---|---|---|
| `account_id`, `resource_arn`, `resource_type`, `region` | STRING | Resource identifiers |
| `usage_date` | DATE | Day the cost was incurred |
| `tag_key`, `tag_value` | STRING | One tag on the resource. NULL for untagged resources |
| `tags_assumed` | BOOLEAN | `true` when the cost predates the first time TagInsights observed the resource |
| `cost` | DOUBLE | Unblended cost for `Usage` lines; effective cost for `SavingsPlanCoveredUsage` and `DiscountedUsage` lines |
| `year`, `month` | INT | Partition keys (partition projection) |

Each row repeats the resource's cost once per tag key, so filter to one
`tag_key` before you `SUM(cost)`. For queries, see
[Cost queries](cost-analysis/QUERY_EXAMPLES.md#cost-by-tag-from-tag_cost_allocation).

If a query reports a missing table or returns no cost data, verify that
the corresponding optional stack is enabled and has completed at least one run.
