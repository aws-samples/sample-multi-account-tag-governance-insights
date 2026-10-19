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
| `change_type` | STRING | `new_resource`, `tags_changed`, or `resource_removed` for scheduled scans |
| `region` | STRING | Resource region |
| `account_id` | STRING | Partition key |
| `year`, `month`, `day` | INT | Partition keys; compare them as integers |

The recent table retains daily data. Older data is aggregated into
`resource_tag_deltas_historical`.

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
| Cost Analysis | `EnableCostAnalysis=true` | `tag_costs` | [Cost queries](cost-analysis/QUERY_EXAMPLES.md) |

If a query reports a missing table or returns no cost data, verify that
the corresponding optional stack is enabled and has completed at least one run.
