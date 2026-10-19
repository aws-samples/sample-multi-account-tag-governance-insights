"""
TagInsights - AWS Resource Tag Retrieval System

This package provides reliable tag retrieval using service-specific APIs
instead of Resource Explorer 2, which has limitations with tag data.

Key Features:
- Uses service-specific APIs (CodeCommit, RDS, SSM, IAM, KMS, Secrets Manager, ACM, etc.)
- Falls back to Resource Groups Tagging API
- Modular and extensible architecture
- Comprehensive error handling and logging

Usage:
    from src.main import get_tags_for_resource
    tags = get_tags_for_resource("arn:aws:codecommit:us-east-1:123456789012:TagInsights")
"""

__version__ = "1.0.0"
__author__ = "AWS Tag Insights Team"
