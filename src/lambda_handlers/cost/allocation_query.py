"""
Set-based tag cost allocation query (one Athena SELECT per billing month).

Joins the TagInsights tag history (resource_tag_deltas_recent UNION ALL
resource_tag_deltas_historical) to CUR line items by full ARN or trailing
bare ID, attributing each usage hour to the tags valid at that time.

Cost basis: Usage at the direct (unblended) cost; SavingsPlanCoveredUsage and
DiscountedUsage at their effective cost when the CUR table has those columns
(else unblended). Bare-ID matches also require the CUR usage account to equal
the resource's account, so identical IDs in two accounts are not double counted.
"""

import re
from datetime import datetime

from .query_builder import (
    SERVICE_NAME_PATTERNS,
    CURQueryBuilder,
    _SQL_IDENTIFIER_RE,
    _escape_sql_string,
)

_ANALYTICS_DB_RE = re.compile(r"^[A-Za-z0-9_-]{1,255}$")
# Athena DataCatalog names: alphanumeric, underscore, hyphen (double-quoted in SQL).
_CATALOG_RE = re.compile(r"^[A-Za-z0-9_-]{1,127}$")

# Region column matches how the product columns are stored: flattened
# (CUR1 and transformed CUR2) vs native CUR2 product map. A profile's
# region_expression (product_region_code when present) takes precedence.
_REGION_EXPRESSIONS = {
    'product_servicename': 'product_region',
    "element_at(product, 'servicename')": "element_at(product, 'region')",
}
_ALLOWED_REGION_EXPRESSIONS = set(_REGION_EXPRESSIONS.values()) | {'product_region_code'}

# Usage line items allocated to resources. Commitment-covered usage is costed at
# its effective (amortized) cost; the matching negation/fee lines are not resource-level.
_LINE_ITEM_TYPES = "'Usage', 'SavingsPlanCoveredUsage', 'DiscountedUsage'"

_PATTERN_ROWS = ",\n        ".join(
    f"('{_escape_sql_string(k)}', '{_escape_sql_string(v)}')"
    for k, v in sorted(SERVICE_NAME_PATTERNS.items())
)


def build_allocation_select(
    analytics_database: str,
    cur_database: str,
    cur_table: str,
    schema_profile: dict,
    year: int,
    month: int,
    cur_catalog: str = None,
) -> str:
    """SELECT producing the allocation rows for one billing month (no trailing semicolon).

    cur_catalog: Athena DataCatalog registered for another account's Glue catalog
    (cross-account CUR). None reads the CUR table from this account's catalog.
    """
    if cur_catalog is not None and (not isinstance(cur_catalog, str) or not _CATALOG_RE.fullmatch(cur_catalog)):
        raise ValueError(f"Invalid CUR data catalog identifier: {cur_catalog!r}")
    if not isinstance(analytics_database, str) or not _ANALYTICS_DB_RE.fullmatch(analytics_database):
        raise ValueError(f"Invalid analytics database identifier: {analytics_database!r}")
    if isinstance(year, bool) or not isinstance(year, int) or not 2000 <= year <= 2100:
        raise ValueError(f"Invalid year: {year!r}")
    if isinstance(month, bool) or not isinstance(month, int) or not 1 <= month <= 12:
        raise ValueError(f"Invalid month: {month!r}")
    if not isinstance(schema_profile, dict):
        raise ValueError("schema_profile must be a dict")

    # Validates cur_database/cur_table (_SQL_IDENTIFIER_RE) and service_name_expression.
    builder = CURQueryBuilder(cur_database, cur_table, schema_profile)
    service_expr = builder.service_name_expression
    region_expr = schema_profile.get('region_expression') or _REGION_EXPRESSIONS[service_expr]
    if region_expr not in _ALLOWED_REGION_EXPRESSIONS:
        raise ValueError(f"Unsupported region expression: {region_expr!r}")
    cost_column = (
        schema_profile.get('direct_cost_column')
        or schema_profile.get('cost_column')
        or 'line_item_unblended_cost'
    )
    # Missing effective-cost column -> that line item type falls back to the direct cost column.
    sp_cost_column = schema_profile.get('savings_plan_effective_cost_column') or cost_column
    ri_cost_column = schema_profile.get('reservation_effective_cost_column') or cost_column
    for column in (cost_column, sp_cost_column, ri_cost_column):
        if not isinstance(column, str) or not _SQL_IDENTIFIER_RE.fullmatch(column):
            raise ValueError(f"Invalid cost column: {column!r}")
    partition_filter = builder._generate_partition_filters(
        1, current_date=datetime(year, month, 1)
    )[0][0]

    adb = f'"{analytics_database}"'
    cur_table_ref = f'"{cur_catalog}".{builder.full_table_name}' if cur_catalog else builder.full_table_name
    return f"""WITH deltas AS (
    SELECT account_id, arn, resource_type, region, tags, change_type, from_unixtime(timestamp) AS changed_at
    FROM {adb}.resource_tag_deltas_recent
    UNION ALL
    SELECT account_id, arn, resource_type, region, tags, change_type, from_unixtime(last_seen_timestamp)
    FROM {adb}.resource_tag_deltas_historical
),
intervals AS (
    SELECT account_id, arn, resource_type, region, tags, change_type,
           MIN(changed_at) OVER (PARTITION BY arn) AS first_seen,
           -- Before TagInsights first saw a resource, assume its first-observed tags applied.
           CASE WHEN LAG(changed_at) OVER (PARTITION BY arn ORDER BY changed_at) IS NULL
                THEN TIMESTAMP '1970-01-01 00:00:00' ELSE changed_at END AS valid_from,
           LAG(changed_at) OVER (PARTITION BY arn ORDER BY changed_at) IS NULL AS assumed_before_first_seen,
           COALESCE(LEAD(changed_at) OVER (PARTITION BY arn ORDER BY changed_at), TIMESTAMP '9999-12-31 00:00:00') AS valid_to
    FROM deltas
),
live AS (SELECT * FROM intervals WHERE change_type <> 'resource_removed'),
patterns(resource_type, service_pattern) AS (VALUES
        {_PATTERN_ROWS}
),
id_forms AS (
    SELECT l.*, f.cur_id, f.cur_id LIKE 'arn:%' AS is_arn, p.service_pattern
    FROM live l
    CROSS JOIN UNNEST(ARRAY_DISTINCT(ARRAY[l.arn, regexp_extract(l.arn, '[^/:]+$')])) AS f(cur_id)
    LEFT JOIN patterns p ON p.resource_type = l.resource_type
),
cur AS (
    SELECT line_item_resource_id AS rid, line_item_usage_start_date AS usage_start,
           {service_expr} AS service_name, {region_expr} AS product_region,
           line_item_usage_account_id AS usage_account_id,
           SUM(CASE line_item_line_item_type
                   WHEN 'SavingsPlanCoveredUsage' THEN {sp_cost_column}
                   WHEN 'DiscountedUsage' THEN {ri_cost_column}
                   ELSE {cost_column} END) AS cost
    FROM {cur_table_ref}
    WHERE {partition_filter}
      AND line_item_line_item_type IN ({_LINE_ITEM_TYPES}) AND line_item_resource_id <> ''
    GROUP BY 1, 2, 3, 4, 5
),
matched AS (
    SELECT i.account_id, i.arn, i.resource_type, i.region, i.tags,
           date(c.usage_start) AS usage_date, c.cost,
           (i.assumed_before_first_seen AND c.usage_start < i.first_seen) AS tags_assumed
    FROM cur c
    JOIN id_forms i
      ON c.rid = i.cur_id
     AND c.usage_start >= i.valid_from AND c.usage_start < i.valid_to
     AND (i.is_arn OR (
            c.usage_account_id = i.account_id
        AND (c.product_region IS NULL OR c.product_region = '' OR c.product_region = i.region)
        AND c.service_name LIKE '%' || COALESCE(i.service_pattern, i.resource_type) || '%'))
)
SELECT account_id, arn AS resource_arn, resource_type, region, usage_date,
       t.tag_key, t.tag_value, tags_assumed, SUM(cost) AS cost
FROM matched
CROSS JOIN UNNEST(
    IF(COALESCE(cardinality(tags), 0) = 0, ARRAY[CAST(NULL AS varchar)], map_keys(tags)),
    IF(COALESCE(cardinality(tags), 0) = 0, ARRAY[CAST(NULL AS varchar)], map_values(tags))
) AS t(tag_key, tag_value)
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8"""  # nosec B608 - identifiers regex-validated (analytics db and CUR catalog double-quoted); service/region exprs from allowlists; year/month ints; pattern rows escaped constants
