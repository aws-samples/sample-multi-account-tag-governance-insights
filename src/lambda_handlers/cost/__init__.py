"""
Cost Analysis Package for TagInsights

This package provides cost analysis functionality including:
- Cost allocation tag management
- CUR integration and processing
- Athena query execution
- Cost analytics and reporting
"""

# Core cost allocation functionality
from .allocation_manager import CostAllocationManager

__all__ = [
    'CostAllocationManager',
]