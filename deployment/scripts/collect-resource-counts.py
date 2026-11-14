#!/usr/bin/env python3
"""
Resource Count Collection Script for TagInsights sizing

This script uses AWS Resource Explorer to aggregate resource counts by account and region
and writes them as CSV (account_id, region, resource_count) for sizing a deployment.

Usage:
    python3 collect-resource-counts.py --output resource-counts.csv
    
    # With filtering
    python3 collect-resource-counts.py --output counts.csv --accounts 123456789012,987654321098
    python3 collect-resource-counts.py --output counts.csv --regions us-east-1,us-west-2

Requirements:
    - boto3 and AWS credentials (run in the Resource Explorer aggregator Region)
    - Resource Explorer enabled with indexes in target regions, and an org-wide view
      (found by '/org/' in its ARN, as the Worker does, or passed with --view-arn)
    - IAM permissions: resource-explorer-2:ListViews and resource-explorer-2:Search
      (AWS authorizes ListResources with the Search action)

Cost: $0 (Resource Explorer ListResources is free)
"""

import boto3
import csv
import sys
import argparse
from collections import defaultdict
from datetime import datetime


def resource_location(resource):
    """
    (account_id, region) from Resource Explorer's own fields. ARNs are not parsed:
    S3 bucket and other global ARNs omit the account and Region.
    """
    return resource.get('OwningAccountId') or 'unknown', resource.get('Region') or 'global'


def build_filter_strings(accounts_filter=None, regions_filter=None):
    """
    ListResources ANDs every filter term and has no OR, so each
    account/region combination is listed with its own filter string.
    """
    accounts = accounts_filter or [None]
    regions = regions_filter or [None]
    return [
        " ".join(t for t in (f"accountid:{a}" if a else "", f"region:{r}" if r else "") if t)
        for a in accounts for r in regions
    ]


def find_org_view(client):
    """
    First view whose ARN contains '/org/' (same rule as the Worker). Without an
    explicit view, ListResources uses the Region's default view, which usually
    covers only this account, so the counts would silently miss member accounts.
    """
    for page in client.get_paginator("list_views").paginate():
        for view_arn in page.get("Views", []):
            if "/org/" in view_arn:
                return view_arn
    sys.exit("No Resource Explorer view with '/org/' in its ARN in this Region. "
             "Create the org view (docs/DEPLOYMENT.md) or pass --view-arn.")


def iter_resource_pages(accounts_filter=None, regions_filter=None, view_arn=None):
    """
    Page through every matching resource with ListResources. (Search is not
    used: it returns at most 1,000 results per query, which undercounts.)
    """
    client = boto3.client("resource-explorer-2")
    view_arn = view_arn or find_org_view(client)
    print(f"Using Resource Explorer view: {view_arn}")
    paginator = client.get_paginator("list_resources")
    for filter_string in build_filter_strings(accounts_filter, regions_filter):
        kwargs = {"MaxResults": 1000, "ViewArn": view_arn}
        if filter_string:
            kwargs["Filters"] = {"FilterString": filter_string}
        yield from paginator.paginate(**kwargs)


def collect_resource_counts(accounts_filter=None, regions_filter=None, view_arn=None):
    """
    Paginate through Resource Explorer results and aggregate counts by (account_id, region).
    Uses constant memory regardless of total resource count.
    
    Returns: dict mapping (account_id, region) -> resource_count
    """
    counts = defaultdict(int)
    total_resources = 0
    page_num = 0
    
    print("Collecting resource counts from AWS Resource Explorer...")
    print("This may take 5-10 minutes for large environments (100M+ resources)...")
    
    for response in iter_resource_pages(accounts_filter, regions_filter, view_arn):
        page_num += 1
        resources = response.get('Resources', [])
        page_count = len(resources)
        total_resources += page_count
        
        print(f"  Page {page_num}: {page_count} resources (total: {total_resources:,})")
        
        # Aggregate counts in memory
        for resource in resources:
            counts[resource_location(resource)] += 1

    print(f"\n✓ Processed {total_resources:,} resources")
    print(f"✓ Found {len(counts)} unique account-region combinations")
    
    return counts


def write_csv(counts, output_file):
    """
    Write aggregated counts to CSV file.
    Format: account_id,region,resource_count
    """
    print(f"\nWriting results to {output_file}...")
    
    # Sort by resource count descending
    sorted_counts = sorted(counts.items(), key=lambda x: x[1], reverse=True)
    
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['account_id', 'region', 'resource_count'])

        for (account_id, region), count in sorted_counts:
            writer.writerow([account_id, region, count])

    print(f"✓ Wrote {len(sorted_counts)} rows to {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description='Collect AWS resource counts per account and Region for TagInsights sizing',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Collect all resources
  python3 collect-resource-counts.py --output resource-counts.csv
  
  # Filter by specific accounts
  python3 collect-resource-counts.py --output counts.csv --accounts 123456789012,987654321098
  
  # Filter by specific regions
  python3 collect-resource-counts.py --output counts.csv --regions us-east-1,us-west-2,eu-west-1
  
  # Combine filters
  python3 collect-resource-counts.py --output counts.csv --accounts 123456789012 --regions us-east-1
        """
    )
    
    parser.add_argument(
        '--output', '-o',
        required=True,
        help='Output CSV file path'
    )
    
    parser.add_argument(
        '--accounts',
        help='Comma-separated list of AWS account IDs to filter (optional)'
    )
    
    parser.add_argument(
        '--regions',
        help='Comma-separated list of AWS regions to filter (optional)'
    )

    parser.add_argument(
        '--view-arn',
        help="Org-wide Resource Explorer view ARN (optional; default: the view with '/org/' in its ARN)"
    )
    
    args = parser.parse_args()
    
    # Parse filters
    accounts_filter = args.accounts.split(',') if args.accounts else None
    regions_filter = args.regions.split(',') if args.regions else None
    
    if accounts_filter:
        print(f"Filtering by accounts: {', '.join(accounts_filter)}")
    if regions_filter:
        print(f"Filtering by regions: {', '.join(regions_filter)}")
    
    # Collect resource counts
    start_time = datetime.now()
    counts = collect_resource_counts(accounts_filter, regions_filter, args.view_arn)
    duration = (datetime.now() - start_time).total_seconds()
    
    # Write to CSV
    write_csv(counts, args.output)
    
    print(f"\nCompleted in {duration:.1f} seconds")
    print(f"Cost: $0 (Resource Explorer ListResources is free)")


if __name__ == '__main__':
    main()
