# TagInsights Cost Analysis Query Examples

SQL query patterns for cost by tag, missing-tag cost, and cost per resource.

> **Requires `EnableCostAnalysis=true`.** These queries use the
> `tag_cost_allocation` table, which the cost allocator Lambda function fills
> daily. For core tagging-gap queries, use
> [USAGE.md](../USAGE.md).

> **How to use these queries:** In Athena, select the workgroup
> `ti-REGION-ENV-ACCOUNT` and replace `"ti_REGION_ENV_ACCOUNT"` in the queries
> below with your actual database name, keeping the double quotes (e.g.,
> `"ti_us-east-1_prod_111122223333"`). The name contains hyphens, so Athena
> rejects it unquoted with `mismatched input '-'`.
> Find it with: `aws glue list-databases --query "DatabaseList[?starts_with(Name, 'ti_')].Name"`

## Cost by tag from `tag_cost_allocation`

`tag_cost_allocation` is a history-aware cost view. A daily
allocator joins AWS Cost and Usage Report (CUR) usage line items to the tags
that were on each resource at the hour the cost was incurred, so it covers
tags that are not activated as cost allocation tags and tags that changed
during the month. For how the allocation works, see
[How cost allocation works](README.md#how-cost-allocation-works).

| Column | Type | Notes |
|---|---|---|
| `account_id`, `resource_arn`, `resource_type`, `region` | STRING | Resource identifiers |
| `usage_date` | DATE | Day the cost was incurred |
| `tag_key` | STRING | One tag key on the resource. NULL for untagged resources |
| `tag_value` | STRING | Value of `tag_key` at that time. NULL for untagged resources |
| `tags_assumed` | BOOLEAN | `true` when the cost was incurred before TagInsights first observed the resource, so the row uses the first-observed tags |
| `cost` | DOUBLE | Cost for the resource, day, and tag: unblended for `Usage` lines, effective cost for `SavingsPlanCoveredUsage` and `DiscountedUsage` lines |
| `year`, `month` | INT | Partition keys (partition projection, no `MSCK REPAIR` needed) |

> **Important: filter to one `tag_key` before you `SUM(cost)`.** Each row
> repeats the resource's cost once per tag key. A resource with five tags
> appears five times per day, so summing across all keys over-counts its cost
> five times. Either filter to a single `tag_key`, or deduplicate to one row
> per resource and day first, as the [total cost per resource](#total-cost-per-resource)
> query shows.

### Cost by tag value for a month

Works for any tag key, including keys that are not activated as cost
allocation tags.

```sql
-- Last month's cost by BusinessLine
SELECT
    tag_value AS business_line,
    ROUND(SUM(cost), 2) AS cost
FROM "ti_REGION_ENV_ACCOUNT".tag_cost_allocation
WHERE tag_key = 'BusinessLine'
  AND year = YEAR(date_add('month', -1, CURRENT_DATE))
  AND month = MONTH(date_add('month', -1, CURRENT_DATE))
GROUP BY 1
ORDER BY 2 DESC;
```

This query returns only resources that have the key. To find the cost of
resources without it, use [cost of resources missing a tag](#cost-of-resources-missing-a-tag).

### Cost by tag value for a quarter

```sql
-- Replace the year and months with the quarter you want (here, Q3 2026)
SELECT
    tag_value AS business_line,
    month,
    ROUND(SUM(cost), 2) AS cost
FROM "ti_REGION_ENV_ACCOUNT".tag_cost_allocation
WHERE tag_key = 'BusinessLine'
  AND year = 2026
  AND month IN (7, 8, 9)
GROUP BY 1, 2
ORDER BY 1, 2;
```

Remove `month` from the `SELECT` and `GROUP BY` for one total per value.

### Cost of resources missing a tag

Counts each resource and day once, whether the resource has other tags or no
tags at all. A resource and day count as tagged when the key was present for
any part of that day.

```sql
-- Last month's cost of resources with no BusinessLine tag, by resource type
WITH per_key AS (
    SELECT resource_arn, resource_type, usage_date, tag_key, SUM(cost) AS cost
    FROM "ti_REGION_ENV_ACCOUNT".tag_cost_allocation
    WHERE year = YEAR(date_add('month', -1, CURRENT_DATE))
      AND month = MONTH(date_add('month', -1, CURRENT_DATE))
    GROUP BY resource_arn, resource_type, usage_date, tag_key
),
per_resource_day AS (
    SELECT
        resource_arn,
        resource_type,
        usage_date,
        MAX(cost) AS cost,
        COUNT_IF(tag_key = 'BusinessLine') > 0 AS has_tag
    FROM per_key
    GROUP BY resource_arn, resource_type, usage_date
)
SELECT
    resource_type,
    COUNT(DISTINCT resource_arn) AS resources,
    ROUND(SUM(cost), 2) AS missing_tag_cost
FROM per_resource_day
WHERE NOT has_tag
GROUP BY resource_type
ORDER BY missing_tag_cost DESC;
```

### Total cost per resource

Deduplicates across tag keys: `per_key` sums each key's rows for the day
(a key whose value changed during the day has one row per value), and
`MAX` keeps one cost per resource and day.

```sql
-- Top 20 resources by last month's cost
WITH per_key AS (
    SELECT resource_arn, usage_date, tag_key, SUM(cost) AS cost
    FROM "ti_REGION_ENV_ACCOUNT".tag_cost_allocation
    WHERE year = YEAR(date_add('month', -1, CURRENT_DATE))
      AND month = MONTH(date_add('month', -1, CURRENT_DATE))
    GROUP BY resource_arn, usage_date, tag_key
),
per_resource_day AS (
    SELECT resource_arn, usage_date, MAX(cost) AS cost
    FROM per_key
    GROUP BY resource_arn, usage_date
)
SELECT
    resource_arn,
    ROUND(SUM(cost), 2) AS cost
FROM per_resource_day
GROUP BY resource_arn
ORDER BY cost DESC
LIMIT 20;
```

If a resource's set of tag keys changes during a day, `MAX` can understate
that day's cost. For exact per-resource totals, query CUR directly.

### Multi-value tag split

Splits each resource's cost evenly across the entries of a multi-value tag
such as `CustomerName = ACME+GLOBEX`.

```sql
-- Entries are trimmed, de-duplicated, and empty entries dropped BEFORE
-- dividing, so each resource's allocations sum to its cost.
WITH shared AS (
    SELECT
        resource_arn,
        cost,
        array_distinct(filter(transform(split(tag_value, '+'), x -> trim(x)), x -> x <> '')) AS customers
    FROM "ti_REGION_ENV_ACCOUNT".tag_cost_allocation
    WHERE tag_key = 'CustomerName'
      AND year = YEAR(date_add('month', -1, CURRENT_DATE))
      AND month = MONTH(date_add('month', -1, CURRENT_DATE))
)
SELECT
    customer,
    COUNT(DISTINCT resource_arn) AS resources,
    ROUND(SUM(cost / CARDINALITY(customers)), 2) AS allocated_cost
FROM shared
CROSS JOIN UNNEST(customers) AS t(customer)
WHERE CARDINALITY(customers) > 0
GROUP BY customer
ORDER BY allocated_cost DESC;
```

### Share of cost on assumed tags

Shows how much of each value's cost uses first-observed tags
(`tags_assumed = true`) rather than tags recorded at the time.

```sql
SELECT
    tag_value AS business_line,
    ROUND(SUM(cost), 2) AS cost,
    ROUND(SUM(IF(tags_assumed, cost, 0)), 2) AS assumed_cost,
    ROUND(100.0 * SUM(IF(tags_assumed, cost, 0)) / NULLIF(SUM(cost), 0), 1) AS assumed_pct
FROM "ti_REGION_ENV_ACCOUNT".tag_cost_allocation
WHERE tag_key = 'BusinessLine'
  AND year = YEAR(date_add('month', -1, CURRENT_DATE))
  AND month = MONTH(date_add('month', -1, CURRENT_DATE))
GROUP BY 1
ORDER BY 2 DESC;
```

A high `assumed_pct` means most of the period predates TagInsights. For
periods before deployment, see [How cost allocation works](README.md#how-cost-allocation-works).

## Current tag state (one row per resource)

The delta tables hold change events, not resources: a resource with three
tag changes has three rows, and a `resource_removed` row can follow earlier
rows. Count resources from the latest row per ARN, across both the daily
(roughly the last one to two months) and monthly-compacted tables. This query does not need Cost
Analysis.

```sql
-- Required-tag coverage by account from current state
WITH all_deltas AS (
    SELECT account_id, arn, tags, change_type, timestamp
    FROM "ti_REGION_ENV_ACCOUNT".resource_tag_deltas_recent
    UNION ALL
    SELECT account_id, arn, tags, change_type, last_seen_timestamp AS timestamp
    FROM "ti_REGION_ENV_ACCOUNT".resource_tag_deltas_historical
),
current_state AS (
    SELECT account_id, arn, tags, change_type,
           ROW_NUMBER() OVER (PARTITION BY arn ORDER BY timestamp DESC) AS rn
    FROM all_deltas
)
SELECT
    account_id,
    COUNT(*) AS total_resources,
    COUNT_IF(element_at(tags, 'Environment') IS NOT NULL) AS tagged_count,
    ROUND(100.0 * COUNT_IF(element_at(tags, 'Environment') IS NOT NULL) / COUNT(*), 1) AS coverage_pct
FROM current_state
WHERE rn = 1 AND change_type <> 'resource_removed'
GROUP BY account_id
ORDER BY coverage_pct ASC;
```

**Use case:** Org-wide tag coverage, and the impact of enforcing a key before
you enforce it (swap `'Environment'` for that key).

## Query Performance Best Practices

- **Always filter partitions.** `year` and `month` are INT partition keys:
  `WHERE year = YEAR(CURRENT_DATE) AND month = MONTH(CURRENT_DATE)`. Without
  them Athena scans every month.
- **Select only needed columns.** Parquet is columnar; `SELECT *` reads every column.
- **Use `LIMIT` for exploration.** It caps result size, not data scanned.
- **Prefer approximate functions on large data.** `APPROX_PERCENTILE` and
  `APPROX_DISTINCT` are much cheaper than exact `COUNT(DISTINCT ...)`.
  (Athena has no `PERCENTILE_CONT`.)

## Support

For questions or issues, open an issue in this repository.

## Additional Resources

- [README.md](README.md) - Cost analysis setup and overview
- [../OPERATIONS.md](../OPERATIONS.md) - Operations and maintenance
- [../DEPLOYMENT.md](../DEPLOYMENT.md) - Deployment guide
