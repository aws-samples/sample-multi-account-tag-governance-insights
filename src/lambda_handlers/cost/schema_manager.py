#!/usr/bin/env python3
"""
CUR schema profile: classifies a Glue CUR table (CUR1, transformed CUR2, or
native CUR2) into the columns and expressions the cost allocator query uses.
"""

from typing import Dict


class CURSchemaManager:
    RESOURCE_LEVEL_REQUIRED_COLUMNS = {
        'line_item_resource_id',
        'line_item_usage_start_date',
        'line_item_usage_account_id',
        'line_item_line_item_type',
    }

    NATIVE_CUR2_SERVICE_NAME_EXPRESSION = (
        "element_at(product, 'servicename')"
    )

    COST_COLUMN_CANDIDATES_BY_FORMAT = {
        'cur1': ('line_item_blended_cost', 'line_item_unblended_cost'),
        'cur2': ('line_item_unblended_cost', 'line_item_blended_cost'),
    }

    DIRECT_COST_COLUMN_CANDIDATES_BY_FORMAT = {
        'cur1': ('line_item_unblended_cost', 'line_item_blended_cost'),
        'cur2': ('line_item_unblended_cost', 'line_item_blended_cost'),
    }

    INT_PARTITION_TYPES = {'tinyint', 'smallint', 'int', 'integer', 'bigint'}

    @staticmethod
    def build_schema_profile(table_info: Dict) -> Dict:
        """Classify a Glue table into the runtime CUR query contract."""
        storage_descriptor = table_info.get('StorageDescriptor', {})
        columns = storage_descriptor.get('Columns', [])
        column_names = {column['Name'] for column in columns}
        column_types = {
            column['Name']: column.get('Type', '')
            for column in columns
        }
        partition_names = {
            partition['Name']
            for partition in table_info.get('PartitionKeys', [])
        }
        # Integer partitions need bare-int filters (year = 2026), not string literals.
        int_partition_columns = sorted(
            partition['Name']
            for partition in table_info.get('PartitionKeys', [])
            if partition.get('Type', '').lower() in CURSchemaManager.INT_PARTITION_TYPES
        )

        if 'billing_period' in partition_names:
            partition_mode = 'billing_period'
        elif {'year', 'month'}.issubset(partition_names):
            partition_mode = 'year_month'
        else:
            partition_mode = None

        tag_mode = None
        tag_column = None
        if (
            'resource_tags' in column_names
            and column_types.get('resource_tags', '').startswith('map')
        ):
            tag_mode = 'map'
            tag_column = 'resource_tags'
        elif any(
            name.startswith('resource_tags_user_')
            for name in column_names
        ):
            tag_mode = 'flattened'

        cur_format = (
            'cur2'
            if partition_mode == 'billing_period' or tag_mode == 'map'
            else 'cur1'
        )

        cost_column = next(
            (
                candidate
                for candidate in CURSchemaManager.COST_COLUMN_CANDIDATES_BY_FORMAT[
                    cur_format
                ]
                if candidate in column_names
            ),
            None,
        )
        direct_cost_column = next(
            (
                candidate
                for candidate in CURSchemaManager.DIRECT_COST_COLUMN_CANDIDATES_BY_FORMAT[
                    cur_format
                ]
                if candidate in column_names
            ),
            None,
        )
        if 'product_servicename' in column_names:
            service_name_expression = 'product_servicename'
        elif (
            'product' in column_names
            and column_types.get('product', '').startswith('map')
        ):
            service_name_expression = (
                CURSchemaManager.NATIVE_CUR2_SERVICE_NAME_EXPRESSION
            )
        else:
            service_name_expression = None
        # Effective (amortized) cost for commitment-covered usage. CUR1 flattened
        # Athena names and CUR2 names are the same snake_case columns.
        savings_plan_effective_cost_column = (
            'savings_plan_savings_plan_effective_cost'
            if 'savings_plan_savings_plan_effective_cost' in column_names
            else None
        )
        reservation_effective_cost_column = (
            'reservation_effective_cost'
            if 'reservation_effective_cost' in column_names
            else None
        )
        # Region code: product_region_code (CUR2, newer CUR1) else the product region.
        if 'product_region_code' in column_names:
            region_expression = 'product_region_code'
        elif service_name_expression == 'product_servicename':
            region_expression = 'product_region'
        elif service_name_expression is not None:
            region_expression = "element_at(product, 'region')"
        else:
            region_expression = None
        has_resource_level_data = (
            CURSchemaManager.RESOURCE_LEVEL_REQUIRED_COLUMNS.issubset(
                column_names
            )
        )

        return {
            'supported': bool(
                has_resource_level_data
                and cost_column is not None
                and partition_mode is not None
                and service_name_expression is not None
            ),
            'format': cur_format,
            'tag_mode': tag_mode,
            'tag_column': tag_column,
            'partition_mode': partition_mode,
            'int_partition_columns': int_partition_columns,
            'cost_column': cost_column,
            'direct_cost_column': direct_cost_column,
            'service_name_expression': service_name_expression,
            'region_expression': region_expression,
            'savings_plan_effective_cost_column': savings_plan_effective_cost_column,
            'reservation_effective_cost_column': reservation_effective_cost_column,
        }
