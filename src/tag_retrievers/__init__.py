# Tag Retrievers Package
# Modular system for retrieving AWS resource tags

from .base import BaseTagRetriever
from .registry import TagRetrieverRegistry

__all__ = ['BaseTagRetriever', 'TagRetrieverRegistry']
