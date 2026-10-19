# TagInsights Cost Analysis Query Examples

SQL query patterns for tag-based cost allocation, missing-tag cost impact, and
cost trends.

> **Requires `EnableCostAnalysis=true`.** These queries use the `tag_costs`
> table, which is populated only after the Cost Analysis pipeline runs. For
> core tagging-gap queries, use [USAGE.md](../USAGE.md).

> **How to use these queries:** In Athena, select the workgroup
> `ti-REGION-ENV-ACCOUNT` and replace `"ti_REGION_ENV_ACCOUNT"` in the queries
> below with your actual database name, keeping the double quotes (e.g.,
> `"ti_us-east-1_prod_123456789012"`). The name contains hyphens, so Athena
> rejects it unquoted with `mismatched input '-'`.
> Find it with: `aws glue list-databases --query 'DatabaseList[?starts_with(Name,\`ti_\`)].Name'`

## How `tag_costs` Is Shaped

| Column | Type | Notes |
|---|---|---|
| `tag_key`, `tag_value` | STRING | One row per tag on the resource |
| `is_cost_allocation_tag` | BOOLEAN | Whether the key is an activated cost allocation tag |
| `resource_arn`, `resource_type`, `region` | STRING | Resource identifiers |
| `current_month_mtd` | DOUBLE | Current month cost to date, from CUR. Repeated on each of the resource's tag rows |
| `avg_daily_cost`, `current_month_projected` | DOUBLE | `current_month_mtd` / day of month, and that × days in month |
| `last_month_cost` | DOUBLE | **Not populated yet (always 0).** Month-over-month queries below return no rows until it is |
| `analysis_date` | DATE | Date of the Cost Analysis run |
| `last_updated` | TIMESTAMP | Write time |
| `account_id`, `year`, `month` | Partition keys | `year`/`month` are INT |

Three things matter when aggregating:

1. **Each resource appears once per tag.** Never `SUM` cost across all rows;
   either filter to one `tag_key` or collapse to one row per `resource_arn`
   with `MAX(...)` first.
2. **Each run adds a new snapshot to the month partition.** Filter to the
   latest `analysis_date` so a month is not counted once per run.
3. **Untagged resources are not in `tag_costs`.** Only resources with at least
   one tag are written, so the "missing tag" queries below find resources
   missing a *specific* tag.

Every query below starts from the same `latest` CTE (current month, latest
run). Queries that need one row per resource add a `resources` CTE.

## Table of Contents

- [Tag-Based Cost Analysis](#tag-based-cost-analysis)
- [Missing-Tag Cost Analysis](#missing-tag-cost-analysis)
- [Cost Trend Analysis](#cost-trend-analysis)
- [Resource Optimization Queries](#resource-optimization-queries)
- [BI Tool Integration](#bi-tool-integration)
- [Query Performance Best Practices](#query-performance-best-practices)

## Tag-Based Cost Analysis

### Environment-Based Cost Allocation

```sql
-- Cost breakdown by environment and service
WITH latest AS (
    SELECT * FROM "ti_REGION_ENV_ACCOUNT".tag_costs
    WHERE year = YEAR(CURRENT_DATE) AND month = MONTH(CURRENT_DATE)
      AND analysis_date = (
          SELECT MAX(analysis_date) FROM "ti_REGION_ENV_ACCOUNT".tag_costs
          WHERE year = YEAR(CURRENT_DATE) AND month = MONTH(CURRENT_DATE))
)
SELECT
    tag_value AS environment,
    split_part(resource_arn, ':', 3) AS service,
    COUNT(DISTINCT resource_arn) AS resource_count,
    SUM(current_month_projected) AS total_cost,
    ROUND(AVG(current_month_projected), 2) AS avg_resource_cost,
    MAX(current_month_projected) AS max_resource_cost
FROM latest
WHERE tag_key = 'Environment'
GROUP BY tag_value, split_part(resource_arn, ':', 3)
ORDER BY total_cost DESC;
```

**Use case:** Allocate AWS spending to development, staging, and production environments.

### Project-Based Cost Tracking

```sql
-- Project costs with owner information
WITH latest AS (
    SELECT * FROM "ti_REGION_ENV_ACCOUNT".tag_costs
    WHERE year = YEAR(CURRENT_DATE) AND month = MONTH(CURRENT_DATE)
      AND analysis_date = (
          SELECT MAX(analysis_date) FROM "ti_REGION_ENV_ACCOUNT".tag_costs
          WHERE year = YEAR(CURRENT_DATE) AND month = MONTH(CURRENT_DATE))
)
SELECT
    p.tag_value AS project,
    o.tag_value AS owner,
    COUNT(DISTINCT p.resource_arn) AS resource_count,
    SUM(p.current_month_projected) AS project_cost,
    ROUND(AVG(p.current_month_projected), 2) AS avg_cost_per_resource
FROM latest p
LEFT JOIN latest o
  ON p.resource_arn = o.resource_arn
  AND o.tag_key = 'Owner'
WHERE p.tag_key = 'Project'
GROUP BY p.tag_value, o.tag_value
ORDER BY project_cost DESC;
```

**Use case:** Track spending per project and identify project owners for cost accountability.

### Cost Center Allocation

```sql
-- Cost center breakdown with month-over-month change
WITH latest AS (
    SELECT * FROM "ti_REGION_ENV_ACCOUNT".tag_costs
    WHERE year = YEAR(CURRENT_DATE) AND month = MONTH(CURRENT_DATE)
      AND analysis_date = (
          SELECT MAX(analysis_date) FROM "ti_REGION_ENV_ACCOUNT".tag_costs
          WHERE year = YEAR(CURRENT_DATE) AND month = MONTH(CURRENT_DATE))
)
SELECT
    tag_value AS cost_center,
    split_part(resource_arn, ':', 3) AS service,
    account_id,
    region,
    COUNT(*) AS resource_count,
    SUM(current_month_projected) AS monthly_cost,
    SUM(last_month_cost) AS previous_month_cost,
    ROUND(
        ((SUM(current_month_projected) - SUM(last_month_cost)) / NULLIF(SUM(last_month_cost), 0)) * 100,
        2
    ) AS cost_change_percent
FROM latest
WHERE tag_key = 'CostCenter'
GROUP BY tag_value, split_part(resource_arn, ':', 3), account_id, region
ORDER BY monthly_cost DESC;
```

**Use case:** Financial reporting for cost center chargebacks and budget tracking.

### Multi-Tag Cost Analysis

```sql
-- Analyze costs across multiple tag dimensions
WITH latest AS (
    SELECT * FROM "ti_REGION_ENV_ACCOUNT".tag_costs
    WHERE year = YEAR(CURRENT_DATE) AND month = MONTH(CURRENT_DATE)
      AND analysis_date = (
          SELECT MAX(analysis_date) FROM "ti_REGION_ENV_ACCOUNT".tag_costs
          WHERE year = YEAR(CURRENT_DATE) AND month = MONTH(CURRENT_DATE))
),
tagged_resources AS (
    SELECT
        resource_arn,
        MAX(CASE WHEN tag_key = 'Environment' THEN tag_value END) AS environment,
        MAX(CASE WHEN tag_key = 'Project' THEN tag_value END) AS project,
        MAX(CASE WHEN tag_key = 'Team' THEN tag_value END) AS team,
        MAX(CASE WHEN tag_key = 'CostCenter' THEN tag_value END) AS cost_center,
        MAX(current_month_projected) AS cost
    FROM latest
    WHERE tag_key IN ('Environment', 'Project', 'Team', 'CostCenter')
    GROUP BY resource_arn
)
SELECT
    environment,
    project,
    team,
    cost_center,
    COUNT(*) AS resource_count,
    SUM(cost) AS total_cost,
    ROUND(AVG(cost), 2) AS avg_cost
FROM tagged_resources
WHERE environment IS NOT NULL
GROUP BY environment, project, team, cost_center
ORDER BY total_cost DESC
LIMIT 50;
```

**Use case:** Multi-dimensional cost analysis for complex organizational structures.

## Missing-Tag Cost Analysis

### High-Cost Resources Missing Required Tags

```sql
-- Expensive resources missing Environment, Project, or Owner
WITH latest AS (
    SELECT * FROM "ti_REGION_ENV_ACCOUNT".tag_costs
    WHERE year = YEAR(CURRENT_DATE) AND month = MONTH(CURRENT_DATE)
      AND analysis_date = (
          SELECT MAX(analysis_date) FROM "ti_REGION_ENV_ACCOUNT".tag_costs
          WHERE year = YEAR(CURRENT_DATE) AND month = MONTH(CURRENT_DATE))
),
resources AS (
    SELECT
        resource_arn,
        split_part(resource_arn, ':', 3) AS service,
        resource_type,
        account_id,
        region,
        MAX(current_month_projected) AS current_month_projected,
        MAX(last_month_cost) AS last_month_cost,
        map_agg(tag_key, tag_value) AS tags
    FROM latest
    GROUP BY resource_arn, resource_type, account_id, region
)
SELECT
    resource_arn,
    service,
    resource_type,
    account_id,
    region,
    current_month_projected AS monthly_cost,
    COALESCE(element_at(tags, 'Name'), 'Unnamed') AS resource_name,
    tags
FROM resources
WHERE current_month_projected > 50
  AND (
    element_at(tags, 'Environment') IS NULL
    OR element_at(tags, 'Project') IS NULL
    OR element_at(tags, 'Owner') IS NULL
  )
ORDER BY current_month_projected DESC
LIMIT 100;
```

**Use case:** Identify costly resources that need tagging for proper cost allocation.

### Missing-Tag Cost by Service

```sql
-- Cost impact of incomplete tagging by service
WITH latest AS (
    SELECT * FROM "ti_REGION_ENV_ACCOUNT".tag_costs
    WHERE year = YEAR(CURRENT_DATE) AND month = MONTH(CURRENT_DATE)
      AND analysis_date = (
          SELECT MAX(analysis_date) FROM "ti_REGION_ENV_ACCOUNT".tag_costs
          WHERE year = YEAR(CURRENT_DATE) AND month = MONTH(CURRENT_DATE))
),
resources AS (
    SELECT
        resource_arn,
        split_part(resource_arn, ':', 3) AS service,
        MAX(current_month_projected) AS current_month_projected,
        map_agg(tag_key, tag_value) AS tags
    FROM latest
    GROUP BY resource_arn
),
tagging AS (
    SELECT
        service,
        current_month_projected,
        CASE
            WHEN element_at(tags, 'Environment') IS NULL THEN 'Missing Environment'
            WHEN element_at(tags, 'Project') IS NULL THEN 'Missing Project'
            WHEN element_at(tags, 'Owner') IS NULL THEN 'Missing Owner'
            ELSE 'Fully Tagged'
        END AS tagging_status
    FROM resources
)
SELECT
    service,
    tagging_status,
    COUNT(*) AS resource_count,
    SUM(current_month_projected) AS total_cost,
    ROUND(AVG(current_month_projected), 2) AS avg_cost_per_resource
FROM tagging
WHERE tagging_status <> 'Fully Tagged'
GROUP BY service, tagging_status
ORDER BY total_cost DESC;
```

**Use case:** Quantify the cost impact of incomplete tagging by service.

### Required-Tag Coverage Cost Impact

```sql
-- Share of cost on resources carrying all required tags
WITH latest AS (
    SELECT * FROM "ti_REGION_ENV_ACCOUNT".tag_costs
    WHERE year = YEAR(CURRENT_DATE) AND month = MONTH(CURRENT_DATE)
      AND analysis_date = (
          SELECT MAX(analysis_date) FROM "ti_REGION_ENV_ACCOUNT".tag_costs
          WHERE year = YEAR(CURRENT_DATE) AND month = MONTH(CURRENT_DATE))
),
resources AS (
    SELECT
        resource_arn,
        MAX(current_month_projected) AS current_month_projected,
        map_agg(tag_key, tag_value) AS tags
    FROM latest
    GROUP BY resource_arn
),
tag_coverage AS (
    SELECT
        CASE
            WHEN element_at(tags, 'Environment') IS NOT NULL
                AND element_at(tags, 'Project') IS NOT NULL
                AND element_at(tags, 'Owner') IS NOT NULL
            THEN 'Tagged'
            ELSE 'Missing required tags'
        END AS status,
        current_month_projected
    FROM resources
)
SELECT
    status,
    COUNT(*) AS resource_count,
    SUM(current_month_projected) AS total_cost,
    ROUND(
        (SUM(current_month_projected) / SUM(SUM(current_month_projected)) OVER ()) * 100,
        2
    ) AS cost_percentage
FROM tag_coverage
GROUP BY status;
```

**Use case:** Executive reporting on cost allocation tag coverage.

## Cost Trend Analysis

> These queries compare against `last_month_cost`, which is not populated yet
> (always 0), so they currently return no rows.

### Month-over-Month Cost by Service

```sql
-- Compare last month's cost with this month's projection, by service
WITH latest AS (
    SELECT * FROM "ti_REGION_ENV_ACCOUNT".tag_costs
    WHERE year = YEAR(CURRENT_DATE) AND month = MONTH(CURRENT_DATE)
      AND analysis_date = (
          SELECT MAX(analysis_date) FROM "ti_REGION_ENV_ACCOUNT".tag_costs
          WHERE year = YEAR(CURRENT_DATE) AND month = MONTH(CURRENT_DATE))
),
resources AS (
    SELECT
        resource_arn,
        split_part(resource_arn, ':', 3) AS service,
        MAX(current_month_projected) AS current_month_projected,
        MAX(last_month_cost) AS last_month_cost
    FROM latest
    GROUP BY resource_arn
)
SELECT
    service,
    SUM(last_month_cost) AS last_month_cost,
    SUM(current_month_projected) AS current_month_projected,
    SUM(current_month_projected) - SUM(last_month_cost) AS cost_change,
    ROUND(
        (SUM(current_month_projected) - SUM(last_month_cost)) / NULLIF(SUM(last_month_cost), 0) * 100,
        2
    ) AS percent_change
FROM resources
GROUP BY service
HAVING SUM(last_month_cost) > 0
ORDER BY ABS(cost_change) DESC;
```

**Use case:** Identify services with significant cost changes month-over-month.

### Environment Cost Trends

```sql
-- Track environment costs across months (latest run in each month)
WITH snapshots AS (
    SELECT
        *,
        MAX(analysis_date) OVER (PARTITION BY year, month) AS month_latest
    FROM "ti_REGION_ENV_ACCOUNT".tag_costs
    WHERE tag_key = 'Environment'
      AND year >= YEAR(CURRENT_DATE) - 1
),
monthly AS (
    SELECT
        tag_value AS environment,
        year,
        month,
        SUM(current_month_projected) AS monthly_cost
    FROM snapshots
    WHERE analysis_date = month_latest
    GROUP BY tag_value, year, month
)
SELECT
    environment,
    year,
    month,
    monthly_cost,
    LAG(monthly_cost) OVER (PARTITION BY environment ORDER BY year, month) AS previous_month_cost,
    ROUND(
        (monthly_cost - LAG(monthly_cost) OVER (PARTITION BY environment ORDER BY year, month))
        / NULLIF(LAG(monthly_cost) OVER (PARTITION BY environment ORDER BY year, month), 0) * 100,
        2
    ) AS growth_rate
FROM monthly
ORDER BY environment, year, month;
```

**Use case:** Track cost growth trends by environment for capacity planning.

## Resource Optimization Queries

### High-Cost Resource Analysis

```sql
-- Resources in the top 5% of cost
WITH latest AS (
    SELECT * FROM "ti_REGION_ENV_ACCOUNT".tag_costs
    WHERE year = YEAR(CURRENT_DATE) AND month = MONTH(CURRENT_DATE)
      AND analysis_date = (
          SELECT MAX(analysis_date) FROM "ti_REGION_ENV_ACCOUNT".tag_costs
          WHERE year = YEAR(CURRENT_DATE) AND month = MONTH(CURRENT_DATE))
),
resources AS (
    SELECT
        resource_arn,
        split_part(resource_arn, ':', 3) AS service,
        resource_type,
        account_id,
        region,
        MAX(current_month_projected) AS current_month_projected,
        map_agg(tag_key, tag_value) AS tags
    FROM latest
    GROUP BY resource_arn, resource_type, account_id, region
),
cost_percentiles AS (
    SELECT APPROX_PERCENTILE(current_month_projected, 0.95) AS p95_cost
    FROM resources
)
SELECT
    r.resource_arn,
    r.service,
    r.resource_type,
    r.account_id,
    r.region,
    r.current_month_projected AS monthly_cost,
    element_at(r.tags, 'Environment') AS environment,
    element_at(r.tags, 'Project') AS project,
    element_at(r.tags, 'Owner') AS owner
FROM resources r
CROSS JOIN cost_percentiles p
WHERE r.current_month_projected >= p.p95_cost
ORDER BY r.current_month_projected DESC;
```

**Use case:** Focus optimization efforts on highest-cost resources (Pareto principle).

### Cost Anomaly Detection

> Uses `last_month_cost` (not populated yet), so this returns no rows for now.

```sql
-- Resources projected at 50%+ above last month
WITH latest AS (
    SELECT * FROM "ti_REGION_ENV_ACCOUNT".tag_costs
    WHERE year = YEAR(CURRENT_DATE) AND month = MONTH(CURRENT_DATE)
      AND analysis_date = (
          SELECT MAX(analysis_date) FROM "ti_REGION_ENV_ACCOUNT".tag_costs
          WHERE year = YEAR(CURRENT_DATE) AND month = MONTH(CURRENT_DATE))
),
resources AS (
    SELECT
        resource_arn,
        split_part(resource_arn, ':', 3) AS service,
        resource_type,
        region,
        MAX(current_month_projected) AS current_month_projected,
        MAX(last_month_cost) AS last_month_cost,
        map_agg(tag_key, tag_value) AS tags
    FROM latest
    GROUP BY resource_arn, resource_type, region
)
SELECT
    resource_arn,
    service,
    resource_type,
    region,
    last_month_cost AS previous_cost,
    current_month_projected AS current_cost,
    current_month_projected - last_month_cost AS cost_increase,
    ROUND((current_month_projected - last_month_cost) / NULLIF(last_month_cost, 0) * 100, 2) AS percent_increase,
    element_at(tags, 'Environment') AS environment
FROM resources
WHERE last_month_cost > 0
  AND current_month_projected > last_month_cost * 1.5
ORDER BY percent_increase DESC
LIMIT 100;
```

**Use case:** Detect cost anomalies that may indicate misconfigurations or runaway resources.

### Right-Sizing Opportunities

> Uses `last_month_cost` (not populated yet), so this returns no rows for now.

```sql
-- Resources whose projected cost dropped 10%+ versus last month
WITH latest AS (
    SELECT * FROM "ti_REGION_ENV_ACCOUNT".tag_costs
    WHERE year = YEAR(CURRENT_DATE) AND month = MONTH(CURRENT_DATE)
      AND analysis_date = (
          SELECT MAX(analysis_date) FROM "ti_REGION_ENV_ACCOUNT".tag_costs
          WHERE year = YEAR(CURRENT_DATE) AND month = MONTH(CURRENT_DATE))
),
cost_trends AS (
    SELECT
        resource_arn,
        split_part(resource_arn, ':', 3) AS service,
        resource_type,
        MAX(current_month_projected) AS current_month_projected,
        MAX(last_month_cost) AS last_month_cost,
        ROUND(
            (MAX(current_month_projected) - MAX(last_month_cost)) / NULLIF(MAX(last_month_cost), 0) * 100,
            2
        ) AS cost_trend_percent
    FROM latest
    GROUP BY resource_arn, resource_type
)
SELECT
    resource_arn,
    service,
    resource_type,
    last_month_cost AS previous_cost,
    current_month_projected AS current_cost,
    cost_trend_percent,
    CASE
        WHEN cost_trend_percent < -30 THEN 'Significant Decrease'
        ELSE 'Moderate Decrease'
    END AS optimization_opportunity
FROM cost_trends
WHERE last_month_cost > 50
  AND current_month_projected > 0
  AND cost_trend_percent < -10
ORDER BY cost_trend_percent ASC;
```

**Use case:** Identify resources with declining usage for right-sizing or termination.

## BI Tool Integration

### QuickSight / PowerBI / Tableau Dataset

Point BI tools at one row per resource per month (latest run in each month).
Run the `SELECT` directly, or wrap it in `CREATE OR REPLACE VIEW
"ti_REGION_ENV_ACCOUNT".taginsights_resource_costs AS ...` to reuse it.

```sql
WITH snapshots AS (
    SELECT
        *,
        MAX(analysis_date) OVER (PARTITION BY year, month) AS month_latest
    FROM "ti_REGION_ENV_ACCOUNT".tag_costs
    WHERE year >= YEAR(CURRENT_DATE) - 1
)
SELECT
    resource_arn,
    split_part(resource_arn, ':', 3) AS service,
    resource_type,
    account_id,
    region,
    year,
    month,
    DATE(CONCAT(CAST(year AS VARCHAR), '-', LPAD(CAST(month AS VARCHAR), 2, '0'), '-01')) AS month_date,
    MAX(current_month_projected) AS current_cost,
    MAX(last_month_cost) AS previous_cost,
    MAX(CASE WHEN tag_key = 'Environment' THEN tag_value END) AS environment,
    MAX(CASE WHEN tag_key = 'Project' THEN tag_value END) AS project,
    MAX(CASE WHEN tag_key = 'Owner' THEN tag_value END) AS owner,
    MAX(CASE WHEN tag_key = 'CostCenter' THEN tag_value END) AS cost_center,
    MAX(CASE WHEN tag_key = 'Application' THEN tag_value END) AS application
FROM snapshots
WHERE analysis_date = month_latest
GROUP BY resource_arn, resource_type, account_id, region, year, month;
```

**Use case:** One reusable dataset for dashboards; aggregate it in the BI tool.

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
