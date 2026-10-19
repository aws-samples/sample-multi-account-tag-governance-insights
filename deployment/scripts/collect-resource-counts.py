#!/usr/bin/env python3
"""
Resource Count Collection Script for TagInsights Cost Calculator

This script uses AWS Resource Explorer to aggregate resource counts by account and region.
It outputs a CSV file compatible with the TagInsights cost calculator.

Usage:
    python3 collect-resource-counts.py --output resource-counts.csv
    
    # With filtering
    python3 collect-resource-counts.py --output counts.csv --accounts 123456789012,987654321098
    python3 collect-resource-counts.py --output counts.csv --regions us-east-1,us-west-2

Requirements:
    - AWS CLI installed and configured
    - Resource Explorer enabled with indexes in target regions
    - IAM permissions: resource-explorer-2:Search

Cost: $0 (Resource Explorer Search API is free)
"""

import subprocess
import json
import csv
import sys
import argparse
from collections import defaultdict
from datetime import datetime


def parse_arn(arn):
    """
    Extract account_id and region from an ARN.
    ARN format: arn:partition:service:region:account-id:resource-type/resource-id
    
    Returns: (account_id, region) tuple
    """
    try:
        parts = arn.split(':')
        if len(parts) < 6:
            return None, None
        
        partition = parts[1]  # aws, aws-cn, aws-us-gov
        service = parts[2]
        region = parts[3] if parts[3] else 'global'
        account_id = parts[4] if parts[4] else 'unknown'
        
        return account_id, region
    except Exception:
        return None, None


def run_resource_explorer_search(next_token=None, accounts_filter=None, regions_filter=None):
    """
    Execute AWS Resource Explorer search using AWS CLI.
    Returns JSON response.
    """
    cmd = ['aws', 'resource-explorer-2', 'search', '--query-string', '*']
    
    # Add filters if specified
    if accounts_filter or regions_filter:
        query_parts = []
        if accounts_filter:
            account_queries = [f'accountid:{acc}' for acc in accounts_filter]
            query_parts.append(f"({' OR '.join(account_queries)})")
        if regions_filter:
            region_queries = [f'region:{reg}' for reg in regions_filter]
            query_parts.append(f"({' OR '.join(region_queries)})")
        
        query_string = ' AND '.join(query_parts)
        cmd[cmd.index('*')] = query_string
    
    if next_token:
        cmd.extend(['--next-token', next_token])
    
    cmd.extend(['--output', 'json'])
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return json.loads(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"ERROR: AWS CLI command failed: {e.stderr}", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"ERROR: Failed to parse AWS CLI output: {e}", file=sys.stderr)
        sys.exit(1)


def collect_resource_counts(accounts_filter=None, regions_filter=None):
    """
    Paginate through Resource Explorer results and aggregate counts by (account_id, region).
    Uses constant memory regardless of total resource count.
    
    Returns: dict mapping (account_id, region) -> resource_count
    """
    counts = defaultdict(int)
    next_token = None
    total_resources = 0
    page_num = 0
    
    print("Collecting resource counts from AWS Resource Explorer...")
    print("This may take 5-10 minutes for large environments (100M+ resources)...")
    
    while True:
        page_num += 1
        print(f"  Fetching page {page_num}...", end='', flush=True)
        
        response = run_resource_explorer_search(next_token, accounts_filter, regions_filter)
        
        resources = response.get('Resources', [])
        page_count = len(resources)
        total_resources += page_count
        
        print(f" {page_count} resources (total: {total_resources:,})")
        
        # Aggregate counts in memory
        for resource in resources:
            arn = resource.get('Arn', '')
            account_id, region = parse_arn(arn)
            
            if account_id and region:
                key = (account_id, region)
                counts[key] += 1
        
        # Check for next page
        next_token = response.get('NextToken')
        if not next_token:
            break
    
    print(f"\n✓ Processed {total_resources:,} resources")
    print(f"✓ Found {len(counts)} unique account-region combinations")
    
    return counts


def write_csv(counts, output_file):
    """
    Write aggregated counts to CSV file.
    Format: account_id,region,resource_count,monthly_spend
    (monthly_spend is 0.00 since this is resource count data only)
    """
    print(f"\nWriting results to {output_file}...")
    
    # Sort by resource count descending
    sorted_counts = sorted(counts.items(), key=lambda x: x[1], reverse=True)
    
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['account_id', 'region', 'resource_count', 'monthly_spend'])
        
        for (account_id, region), count in sorted_counts:
            writer.writerow([account_id, region, count, '0.00'])
    
    print(f"✓ Wrote {len(sorted_counts)} rows to {output_file}")
    print(f"\nNext steps:")
    print(f"  1. Upload {output_file} to TagInsights cost calculator")
    print(f"  2. Review accurate cost estimates based on your actual resource counts")
    print(f"  3. Download deployment recommendation CSVs")


def main():
    parser = argparse.ArgumentParser(
        description='Collect AWS resource counts for TagInsights cost calculator',
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
    counts = collect_resource_counts(accounts_filter, regions_filter)
    duration = (datetime.now() - start_time).total_seconds()
    
    # Write to CSV
    write_csv(counts, args.output)
    
    print(f"\nCompleted in {duration:.1f} seconds")
    print(f"Cost: $0 (Resource Explorer Search API is free)")


if __name__ == '__main__':
    main()
