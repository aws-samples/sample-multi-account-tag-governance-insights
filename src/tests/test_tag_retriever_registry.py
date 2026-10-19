"""
Tests for tag_retrievers/registry.py and tag_retrievers/multi_region_registry.py.
Covers: retriever registration, lookup, fallback behavior,
multi-region coordination, preloading, and comprehensive discovery checks.
"""

import os
import sys
from unittest.mock import patch, MagicMock

import pytest

# Insert project root so 'src' is importable as a package (needed for relative imports)
_project_root = os.path.join(os.path.dirname(__file__), "..", "..")
sys.path.insert(0, _project_root)
# Also keep src/ in path for flat imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Ensure loguru is available (mock if not installed in test env)
try:
    import loguru  # noqa: F401
except ModuleNotFoundError:
    sys.modules.setdefault("loguru", MagicMock())

from src.tag_retrievers.base import BaseTagRetriever

# --- Helpers ---


class FakeRetriever(BaseTagRetriever):
    """Test retriever that handles a specific service prefix."""

    def __init__(
        self,
        service_name="fake-service",
        region_name="us-east-1",
        arn_prefix="arn:aws:fake:",
    ):
        super().__init__(region_name)
        self._service_name = service_name
        self._arn_prefix = arn_prefix

    def _create_client(self):
        return MagicMock()

    def get_service_name(self) -> str:
        return self._service_name

    def can_handle_arn(self, resource_arn: str) -> bool:
        return resource_arn.startswith(self._arn_prefix)

    def get_tags(self, resource_arn: str):
        return {"Service": self._service_name}


# We need to mock the AWS clients used during registry initialization
@pytest.fixture
def mock_aws_clients():
    """Mock boto3 clients to prevent real AWS calls during registry init."""
    with patch("boto3.client") as mock_client:
        mock_instance = MagicMock()
        # ResourceExplorer validate_setup will fail gracefully
        mock_instance.list_indexes.return_value = {"Indexes": []}
        mock_instance.get_resources.return_value = {"ResourceTagMappingList": []}
        mock_client.return_value = mock_instance
        yield mock_client


class TestTagRetrieverRegistry:
    """Tests for TagRetrieverRegistry."""

    def test_init_creates_registry(self, mock_aws_clients):
        from src.tag_retrievers.registry import TagRetrieverRegistry

        registry = TagRetrieverRegistry(region_name="us-west-2")
        assert registry.region_name == "us-west-2"

    def test_register_retriever(self, mock_aws_clients):
        from src.tag_retrievers.registry import TagRetrieverRegistry

        registry = TagRetrieverRegistry()
        registry.clear_retrievers()

        fake = FakeRetriever()
        registry.register_retriever(fake)
        assert fake in registry._retrievers

    def test_get_retriever_for_arn_specialized(self, mock_aws_clients):
        from src.tag_retrievers.registry import TagRetrieverRegistry

        registry = TagRetrieverRegistry()
        registry.clear_retrievers()

        fake = FakeRetriever(service_name="my-svc", arn_prefix="arn:aws:mysvc:")
        registry.register_retriever(fake)

        result = registry.get_retriever_for_arn("arn:aws:mysvc:us-east-1:123:thing/x")
        assert result is fake

    def test_get_retriever_falls_back(self, mock_aws_clients):
        from src.tag_retrievers.registry import TagRetrieverRegistry

        registry = TagRetrieverRegistry()
        registry.clear_retrievers()

        fallback = FakeRetriever(service_name="fallback", arn_prefix="arn:aws:")
        registry.set_fallback_retriever(fallback)

        result = registry.get_retriever_for_arn("arn:aws:unknown:us-east-1:123:thing/x")
        assert result is fallback

    def test_get_retriever_returns_none_when_no_match(self, mock_aws_clients):
        from src.tag_retrievers.registry import TagRetrieverRegistry

        registry = TagRetrieverRegistry()
        registry.clear_retrievers()
        # No fallback set after clear
        result = registry.get_retriever_for_arn("arn:aws:s3:::bucket")
        assert result is None

    def test_get_all_retrievers(self, mock_aws_clients):
        from src.tag_retrievers.registry import TagRetrieverRegistry

        registry = TagRetrieverRegistry()
        registry.clear_retrievers()

        r1 = FakeRetriever(service_name="svc1")
        r2 = FakeRetriever(service_name="svc2")
        fallback = FakeRetriever(service_name="fb")

        registry.register_retriever(r1)
        registry.register_retriever(r2)
        registry.set_fallback_retriever(fallback)

        all_r = registry.get_all_retrievers()
        assert r1 in all_r
        assert r2 in all_r
        assert fallback in all_r

    def test_get_supported_services(self, mock_aws_clients):
        from src.tag_retrievers.registry import TagRetrieverRegistry

        registry = TagRetrieverRegistry()
        registry.clear_retrievers()

        registry.register_retriever(FakeRetriever(service_name="alpha"))
        registry.register_retriever(FakeRetriever(service_name="beta"))
        registry.set_fallback_retriever(FakeRetriever(service_name="gamma"))

        services = registry.get_supported_services()
        assert "alpha" in services
        assert "beta" in services
        assert "gamma" in services

    def test_clear_retrievers(self, mock_aws_clients):
        from src.tag_retrievers.registry import TagRetrieverRegistry

        registry = TagRetrieverRegistry()
        registry.register_retriever(FakeRetriever())
        registry.clear_retrievers()
        assert registry._retrievers == []
        assert registry._fallback_retriever is None

    def test_get_comprehensive_retriever_none(self, mock_aws_clients):
        from src.tag_retrievers.registry import TagRetrieverRegistry

        registry = TagRetrieverRegistry()
        result = registry.get_comprehensive_retriever()
        assert result is None

    def test_is_comprehensive_discovery_available_false(self, mock_aws_clients):
        from src.tag_retrievers.registry import TagRetrieverRegistry

        registry = TagRetrieverRegistry()
        assert registry.is_comprehensive_discovery_available() is False

    def test_str_representation(self, mock_aws_clients):
        from src.tag_retrievers.registry import TagRetrieverRegistry

        registry = TagRetrieverRegistry(region_name="ap-south-1")
        s = str(registry)
        assert "TagRetrieverRegistry" in s
        assert "ap-south-1" in s


class TestMultiRegionTagRetrieverRegistry:
    """Tests for MultiRegionTagRetrieverRegistry."""

    def test_init_default_region(self, mock_aws_clients):
        from src.tag_retrievers.multi_region_registry import (
            MultiRegionTagRetrieverRegistry,
        )

        mr = MultiRegionTagRetrieverRegistry()
        assert mr.default_region == "us-east-1"

    def test_init_custom_region(self, mock_aws_clients):
        from src.tag_retrievers.multi_region_registry import (
            MultiRegionTagRetrieverRegistry,
        )

        mr = MultiRegionTagRetrieverRegistry(default_region="eu-west-1")
        assert mr.default_region == "eu-west-1"

    def test_get_registry_for_region_creates_new(self, mock_aws_clients):
        from src.tag_retrievers.multi_region_registry import (
            MultiRegionTagRetrieverRegistry,
        )

        mr = MultiRegionTagRetrieverRegistry()
        registry = mr.get_registry_for_region("ap-northeast-1")
        assert registry.region_name == "ap-northeast-1"
        # Second call returns same instance
        assert mr.get_registry_for_region("ap-northeast-1") is registry

    def test_get_retriever_for_arn_regional(self, mock_aws_clients):
        from src.tag_retrievers.multi_region_registry import (
            MultiRegionTagRetrieverRegistry,
        )

        mr = MultiRegionTagRetrieverRegistry()
        arn = "arn:aws:ec2:us-west-2:123456789012:instance/i-1234567890abcdef0"
        retriever = mr.get_retriever_for_arn(arn)
        assert retriever is not None

    def test_get_retriever_for_arn_global_service(self, mock_aws_clients):
        from src.tag_retrievers.multi_region_registry import (
            MultiRegionTagRetrieverRegistry,
        )

        mr = MultiRegionTagRetrieverRegistry()
        arn = "arn:aws:iam::123456789012:role/MyRole"
        retriever = mr.get_retriever_for_arn(arn)
        assert retriever is not None

    def test_get_all_regions_initially_empty(self, mock_aws_clients):
        from src.tag_retrievers.multi_region_registry import (
            MultiRegionTagRetrieverRegistry,
        )

        mr = MultiRegionTagRetrieverRegistry()
        assert mr.get_all_regions() == []

    def test_get_all_regions_after_access(self, mock_aws_clients):
        from src.tag_retrievers.multi_region_registry import (
            MultiRegionTagRetrieverRegistry,
        )

        mr = MultiRegionTagRetrieverRegistry()
        mr.get_registry_for_region("us-east-1")
        mr.get_registry_for_region("eu-west-1")
        regions = mr.get_all_regions()
        assert "us-east-1" in regions
        assert "eu-west-1" in regions

    def test_clear_registries(self, mock_aws_clients):
        from src.tag_retrievers.multi_region_registry import (
            MultiRegionTagRetrieverRegistry,
        )

        mr = MultiRegionTagRetrieverRegistry()
        mr.get_registry_for_region("us-east-1")
        mr.clear_registries()
        assert mr.get_all_regions() == []

    def test_get_registry_stats(self, mock_aws_clients):
        from src.tag_retrievers.multi_region_registry import (
            MultiRegionTagRetrieverRegistry,
        )

        mr = MultiRegionTagRetrieverRegistry()
        mr.get_registry_for_region("us-east-1")
        stats = mr.get_registry_stats()
        assert "us-east-1" in stats
        assert isinstance(stats["us-east-1"], int)

    def test_preload_regions(self, mock_aws_clients):
        from src.tag_retrievers.multi_region_registry import (
            MultiRegionTagRetrieverRegistry,
        )

        mr = MultiRegionTagRetrieverRegistry()
        mr.preload_regions(["us-east-1", "us-west-2", "eu-central-1"])
        regions = mr.get_all_regions()
        assert "us-east-1" in regions
        assert "us-west-2" in regions
        assert "eu-central-1" in regions

    def test_register_retriever_in_region(self, mock_aws_clients):
        from src.tag_retrievers.multi_region_registry import (
            MultiRegionTagRetrieverRegistry,
        )

        mr = MultiRegionTagRetrieverRegistry()
        fake = FakeRetriever(service_name="custom", region_name="eu-west-1")
        mr.register_retriever(fake, region="eu-west-1")
        registry = mr.get_registry_for_region("eu-west-1")
        assert fake in registry._retrievers

    def test_register_retriever_default_region(self, mock_aws_clients):
        from src.tag_retrievers.multi_region_registry import (
            MultiRegionTagRetrieverRegistry,
        )

        mr = MultiRegionTagRetrieverRegistry()
        fake = FakeRetriever(service_name="custom", region_name="ap-south-1")
        mr.register_retriever(fake)
        # Should use retriever's region_name
        registry = mr.get_registry_for_region("ap-south-1")
        assert fake in registry._retrievers

    def test_get_supported_services(self, mock_aws_clients):
        from src.tag_retrievers.multi_region_registry import (
            MultiRegionTagRetrieverRegistry,
        )

        mr = MultiRegionTagRetrieverRegistry()
        services = mr.get_supported_services()
        assert isinstance(services, list)

    def test_is_comprehensive_discovery_available(self, mock_aws_clients):
        from src.tag_retrievers.multi_region_registry import (
            MultiRegionTagRetrieverRegistry,
        )

        mr = MultiRegionTagRetrieverRegistry()
        assert mr.is_comprehensive_discovery_available() is False

    def test_get_comprehensive_retriever(self, mock_aws_clients):
        from src.tag_retrievers.multi_region_registry import (
            MultiRegionTagRetrieverRegistry,
        )

        mr = MultiRegionTagRetrieverRegistry()
        result = mr.get_comprehensive_retriever()
        assert result is None

    def test_str_representation(self, mock_aws_clients):
        from src.tag_retrievers.multi_region_registry import (
            MultiRegionTagRetrieverRegistry,
        )

        mr = MultiRegionTagRetrieverRegistry(default_region="ca-central-1")
        s = str(mr)
        assert "MultiRegionTagRetrieverRegistry" in s
        assert "ca-central-1" in s


class TestGetAvailableRegions:
    """Tests for the module-level get_available_regions function."""

    def test_returns_list(self):
        from src.tag_retrievers.multi_region_registry import get_available_regions

        regions = get_available_regions()
        assert isinstance(regions, list)
        assert len(regions) > 0

    def test_contains_common_regions(self):
        from src.tag_retrievers.multi_region_registry import get_available_regions

        regions = get_available_regions()
        assert "us-east-1" in regions
        assert "eu-west-1" in regions
        assert "ap-northeast-1" in regions

    def test_returns_copy(self):
        from src.tag_retrievers.multi_region_registry import (
            get_available_regions,
            COMMON_AWS_REGIONS,
        )

        regions = get_available_regions()
        regions.append("fake-region")
        assert "fake-region" not in COMMON_AWS_REGIONS
