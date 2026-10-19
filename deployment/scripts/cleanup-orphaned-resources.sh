#!/bin/bash
# cleanup-orphaned-resources.sh
# Deletes all orphaned TagInsights resources across accounts and regions
# Supports different AWS profiles per account

set -e

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_status() { echo -e "${BLUE}[INFO]${NC} $1"; }
print_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
print_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
print_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Configuration
ENVIRONMENT="dev"
PROJECT_NAME="taginsights"
REGIONS="us-east-1 us-west-2"
DRY_RUN="false"
ACCOUNTS=""
HUB_ACCOUNT=""

# Helper function for AWS commands with profile support
aws_cmd() {
    local account_profile=$1
    shift
    if [[ -n "$account_profile" ]]; then
        aws --profile "$account_profile" "$@"
    else
        aws "$@"
    fi
}

show_usage() {
    cat << EOF
TagInsights Orphaned Resource Cleanup

Deletes orphaned resources (S3 buckets, SQS queues, Lambda functions, IAM roles, 
CloudWatch alarms/logs) that prevent CloudFormation deployment.

IMPORTANT: Hub resources (central analytics bucket, hub Lambda functions)
are automatically protected from deletion in the hub account.

Usage: $0 [OPTIONS]

Options:
  --accounts "ACC1:PROF1 ACC2:PROF2"  Account:Profile pairs (space-separated)
  --hub-account ACCOUNT_ID             Hub account ID (protects hub resources)
  --environment ENV                    Environment name (default: dev)
  --regions "REG1 REG2"               Regions (default: us-east-1 us-west-2)
  --dry-run                           Show what would be deleted without deleting
  --help                              Show this help

Examples:
  # Dry run with hub account protection
  $0 --accounts "111111111111:hub-profile 222222222222:member-profile" --hub-account 111111111111 --dry-run

  # Actual cleanup (hub resources automatically protected)
  $0 --accounts "222222222222:hub-profile 333333333333:hub-profile" --hub-account 111111111111

  # Single spoke account cleanup (no hub resources to worry about)
  $0 --accounts "222222222222:hub-profile" --regions "us-west-2" --dry-run
EOF
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --accounts) ACCOUNTS="$2"; shift 2 ;;
        --hub-account) HUB_ACCOUNT="$2"; shift 2 ;;
        --environment) ENVIRONMENT="$2"; shift 2 ;;
        --regions) REGIONS="$2"; shift 2 ;;
        --dry-run) DRY_RUN="true"; shift ;;
        --help|-h) show_usage; exit 0 ;;
        *) print_error "Unknown option: $1"; show_usage; exit 1 ;;
    esac
done

if [[ -z "$ACCOUNTS" ]]; then
    print_error "Missing required --accounts parameter"
    show_usage
    exit 1
fi

if [[ "$DRY_RUN" == "true" ]]; then
    print_warning "DRY RUN MODE - No resources will be deleted"
    echo ""
fi

# Helper function to check if a resource should be protected (hub resources)
is_hub_resource() {
    local resource_name=$1
    local account=$2
    
    # Only protect resources if this IS the hub account
    if [[ "$account" != "$HUB_ACCOUNT" ]]; then
        return 1  # Not hub account, don't protect
    fi
    
    # If no hub account specified, don't protect anything
    if [[ -z "$HUB_ACCOUNT" ]]; then
        return 1
    fi
    
    # Hub S3 buckets to protect
    if [[ "$resource_name" =~ central-analytics || \
          "$resource_name" =~ hub-lambda || \
          "$resource_name" =~ hub-athena || \
          "$resource_name" =~ deploy-optimized || \
          "$resource_name" =~ deploy-dev ]]; then
        return 0  # Protect
    fi
    
    # Hub SQS queues to protect
    if [[ "$resource_name" =~ central-results || \
          "$resource_name" =~ cost-analysis ]]; then
        return 0  # Protect
    fi
    
    # Hub Lambda functions to protect
    if [[ "$resource_name" =~ analytics-writer || \
          "$resource_name" =~ monthly-aggregator || \
          "$resource_name" =~ cost-coordinator || \
          "$resource_name" =~ cost-worker || \
          "$resource_name" =~ cost-dedup ]]; then
        return 0  # Protect
    fi
    
    # Hub CloudWatch alarms to protect (same patterns as Lambda)
    if [[ "$resource_name" =~ analytics-writer || \
          "$resource_name" =~ monthly-aggregator || \
          "$resource_name" =~ cost- ]]; then
        return 0  # Protect
    fi
    
    # Hub log groups to protect (same patterns as Lambda)
    if [[ "$resource_name" =~ analytics-writer || \
          "$resource_name" =~ monthly-aggregator || \
          "$resource_name" =~ cost-coordinator || \
          "$resource_name" =~ cost-worker || \
          "$resource_name" =~ cost-dedup ]]; then
        return 0  # Protect
    fi
    
    return 1  # Not a hub resource, don't protect
}

print_status "╔════════════════════════════════════════════════════════════╗"
print_status "║   TagInsights Orphaned Resource Cleanup                   ║"
print_status "╚════════════════════════════════════════════════════════════╝"
print_status "  Environment: $ENVIRONMENT"
print_status "  Regions: $REGIONS"
if [[ -n "$HUB_ACCOUNT" ]]; then
    print_status "  Hub Account: $HUB_ACCOUNT (hub resources protected)"
fi
echo ""

# Process each account:profile pair
for account_profile in $ACCOUNTS; do
    IFS=':' read -r account profile <<< "$account_profile"
    
    print_status "════════════════════════════════════════════════════════════"
    print_status "Account: $account (Profile: $profile)"
    print_status "════════════════════════════════════════════════════════════"
    
    # Verify account access
    CURRENT_ACCOUNT=$(aws_cmd "$profile" sts get-caller-identity --query Account --output text 2>/dev/null || echo "")
    if [[ "$CURRENT_ACCOUNT" != "$account" ]]; then
        print_error "Profile '$profile' cannot access account $account (got: $CURRENT_ACCOUNT)"
        print_error "Skipping this account"
        echo ""
        continue
    fi
    print_success "✓ Verified access to account $account"
    
    for region in $REGIONS; do
        print_status ""
        print_status "──────────────────────────────────────────────────────────"
        print_status "Region: $region"
        print_status "──────────────────────────────────────────────────────────"
        
        # 1. Delete S3 Buckets
        print_status "Checking S3 buckets..."
        BUCKETS=$(aws_cmd "$profile" s3api list-buckets \
            --query "Buckets[?contains(Name, '$PROJECT_NAME') && contains(Name, '$account')].[Name]" \
            --output text 2>/dev/null || echo "")
        
        if [[ -n "$BUCKETS" ]]; then
            for bucket in $BUCKETS; do
                # Check if this is a hub resource that should be protected
                if is_hub_resource "$bucket" "$account"; then
                    print_status "  Found bucket (HUB - PROTECTED): $bucket"
                    continue
                fi
                
                print_warning "  Found bucket: $bucket"
                if [[ "$DRY_RUN" == "false" ]]; then
                    print_status "    Emptying bucket..."
                    
                    # Delete all versions
                    aws_cmd "$profile" s3api list-object-versions \
                        --bucket "$bucket" \
                        --query 'Versions[].{Key:Key,VersionId:VersionId}' \
                        --output text 2>/dev/null | \
                    while read key versionId; do
                        if [[ -n "$key" && -n "$versionId" ]]; then
                            aws_cmd "$profile" s3api delete-object \
                                --bucket "$bucket" \
                                --key "$key" \
                                --version-id "$versionId" \
                                >/dev/null 2>&1 || true
                        fi
                    done
                    
                    # Delete all delete markers
                    aws_cmd "$profile" s3api list-object-versions \
                        --bucket "$bucket" \
                        --query 'DeleteMarkers[].{Key:Key,VersionId:VersionId}' \
                        --output text 2>/dev/null | \
                    while read key versionId; do
                        if [[ -n "$key" && -n "$versionId" ]]; then
                            aws_cmd "$profile" s3api delete-object \
                                --bucket "$bucket" \
                                --key "$key" \
                                --version-id "$versionId" \
                                >/dev/null 2>&1 || true
                        fi
                    done
                    
                    # Delete remaining objects
                    aws_cmd "$profile" s3 rm "s3://$bucket" --recursive 2>/dev/null || true
                    
                    # Delete bucket
                    print_status "    Deleting bucket..."
                    aws_cmd "$profile" s3api delete-bucket \
                        --bucket "$bucket" \
                        2>/dev/null && print_success "  ✓ Deleted bucket: $bucket" || print_error "    Failed to delete bucket (may have remaining objects)"
                fi
            done
        else
            print_status "  No S3 buckets found"
        fi
        
        # 2. Delete SQS Queues
        print_status "Checking SQS queues..."
        QUEUES=$(aws_cmd "$profile" sqs list-queues \
            --queue-name-prefix "$PROJECT_NAME" \
            --region "$region" \
            --query 'QueueUrls[]' \
            --output text 2>/dev/null || echo "")
        
        if [[ -n "$QUEUES" ]]; then
            for queue_url in $QUEUES; do
                queue_name=$(basename "$queue_url")
                
                # Check if this is a hub resource that should be protected
                if is_hub_resource "$queue_name" "$account"; then
                    print_status "  Found queue (HUB - PROTECTED): $queue_name"
                    continue
                fi
                
                print_warning "  Found queue: $queue_name"
                if [[ "$DRY_RUN" == "false" ]]; then
                    aws_cmd "$profile" sqs delete-queue \
                        --queue-url "$queue_url" \
                        --region "$region" \
                        2>/dev/null && print_success "  ✓ Deleted queue: $queue_name" || print_error "    Failed to delete queue"
                fi
            done
        else
            print_status "  No SQS queues found"
        fi
        
        # 3. Delete Lambda Functions
        print_status "Checking Lambda functions..."
        FUNCTIONS=$(aws_cmd "$profile" lambda list-functions \
            --region "$region" \
            --query "Functions[?contains(FunctionName, '$PROJECT_NAME')].[FunctionName]" \
            --output text 2>/dev/null || echo "")
        
        if [[ -n "$FUNCTIONS" ]]; then
            for function in $FUNCTIONS; do
                # Check if this is a hub resource that should be protected
                if is_hub_resource "$function" "$account"; then
                    print_status "  Found function (HUB - PROTECTED): $function"
                    continue
                fi
                
                print_warning "  Found function: $function"
                if [[ "$DRY_RUN" == "false" ]]; then
                    aws_cmd "$profile" lambda delete-function \
                        --function-name "$function" \
                        --region "$region" \
                        2>/dev/null && print_success "  ✓ Deleted function: $function" || print_error "    Failed to delete function"
                fi
            done
        else
            print_status "  No Lambda functions found"
        fi
        
        # 4. Delete CloudWatch Alarms
        print_status "Checking CloudWatch alarms..."
        ALARMS=$(aws_cmd "$profile" cloudwatch describe-alarms \
            --region "$region" \
            --query "MetricAlarms[?contains(AlarmName, '$PROJECT_NAME')].[AlarmName]" \
            --output text 2>/dev/null || echo "")
        
        if [[ -n "$ALARMS" ]]; then
            for alarm in $ALARMS; do
                # Check if this is a hub resource that should be protected
                if is_hub_resource "$alarm" "$account"; then
                    print_status "  Found alarm (HUB - PROTECTED): $alarm"
                    continue
                fi
                
                print_warning "  Found alarm: $alarm"
                if [[ "$DRY_RUN" == "false" ]]; then
                    aws_cmd "$profile" cloudwatch delete-alarms \
                        --alarm-names "$alarm" \
                        --region "$region" \
                        2>/dev/null && print_success "  ✓ Deleted alarm: $alarm" || print_error "    Failed to delete alarm"
                fi
            done
        else
            print_status "  No CloudWatch alarms found"
        fi
        
        # 5. Delete CloudWatch Log Groups
        print_status "Checking CloudWatch log groups..."
        LOG_GROUPS=$(aws_cmd "$profile" logs describe-log-groups \
            --region "$region" \
            --query "logGroups[?contains(logGroupName, '$PROJECT_NAME')].[logGroupName]" \
            --output text 2>/dev/null || echo "")
        
        if [[ -n "$LOG_GROUPS" ]]; then
            for log_group in $LOG_GROUPS; do
                # Check if this is a hub resource that should be protected
                if is_hub_resource "$log_group" "$account"; then
                    print_status "  Found log group (HUB - PROTECTED): $log_group"
                    continue
                fi
                
                print_warning "  Found log group: $log_group"
                if [[ "$DRY_RUN" == "false" ]]; then
                    aws_cmd "$profile" logs delete-log-group \
                        --log-group-name "$log_group" \
                        --region "$region" \
                        2>/dev/null && print_success "  ✓ Deleted log group: $log_group" || print_error "    Failed to delete log group"
                fi
            done
        else
            print_status "  No log groups found"
        fi
    done
    
    # 6. Delete IAM Roles (global, only check once per account)
    print_status ""
    print_status "──────────────────────────────────────────────────────────"
    print_status "IAM Roles (global)"
    print_status "──────────────────────────────────────────────────────────"
    
    ROLES=$(aws_cmd "$profile" iam list-roles \
        --query "Roles[?contains(RoleName, '$PROJECT_NAME') && contains(RoleName, '$account')].[RoleName]" \
        --output text 2>/dev/null || echo "")
    
    if [[ -n "$ROLES" ]]; then
        for role in $ROLES; do
            print_warning "  Found role: $role"
            if [[ "$DRY_RUN" == "false" ]]; then
                # Detach managed policies
                ATTACHED_POLICIES=$(aws_cmd "$profile" iam list-attached-role-policies \
                    --role-name "$role" \
                    --query 'AttachedPolicies[].PolicyArn' \
                    --output text 2>/dev/null || echo "")
                for policy_arn in $ATTACHED_POLICIES; do
                    if [[ -n "$policy_arn" ]]; then
                        aws_cmd "$profile" iam detach-role-policy \
                            --role-name "$role" \
                            --policy-arn "$policy_arn" \
                            2>/dev/null || true
                    fi
                done
                
                # Delete inline policies
                INLINE_POLICIES=$(aws_cmd "$profile" iam list-role-policies \
                    --role-name "$role" \
                    --query 'PolicyNames[]' \
                    --output text 2>/dev/null || echo "")
                for policy_name in $INLINE_POLICIES; do
                    if [[ -n "$policy_name" ]]; then
                        aws_cmd "$profile" iam delete-role-policy \
                            --role-name "$role" \
                            --policy-name "$policy_name" \
                            2>/dev/null || true
                    fi
                done
                
                # Delete role
                aws_cmd "$profile" iam delete-role \
                    --role-name "$role" \
                    2>/dev/null && print_success "  ✓ Deleted role: $role" || print_error "    Failed to delete role"
            fi
        done
    else
        print_status "  No IAM roles found"
    fi
    
    print_success ""
    print_success "════════════════════════════════════════════════════════════"
    print_success "✓ Cleanup complete for account $account"
    print_success "════════════════════════════════════════════════════════════"
    echo ""
done

if [[ "$DRY_RUN" == "true" ]]; then
    print_warning ""
    print_warning "╔════════════════════════════════════════════════════════════╗"
    print_warning "║         DRY RUN COMPLETE                                   ║"
    print_warning "║  No resources were actually deleted                        ║"
    print_warning "╚════════════════════════════════════════════════════════════╝"
    print_status ""
    print_status "Run without --dry-run to perform actual deletion"
else
    print_success ""
    print_success "╔════════════════════════════════════════════════════════════╗"
    print_success "║  🎉 All Orphaned Resources Cleaned Up!                    ║"
    print_success "╚════════════════════════════════════════════════════════════╝"
    print_status ""
    print_status "Next steps:"
    print_status "  1. Redeploy with: ./deploy.sh [your deployment options]"
    print_status "  2. The fixed templates should now deploy cleanly"
    print_status "  3. No more 'AlreadyExists' errors!"
fi