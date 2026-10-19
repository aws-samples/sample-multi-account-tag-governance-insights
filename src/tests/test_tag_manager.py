"""
Tests for tag_manager.py - TagManager orchestration layer.
Covers: get_tags_for_resource, get_tags_for_resources, find_tagged_resources,
get_resource_summary, hybrid methods, comprehensive discovery, and discovery capabilities.
"""

import os
import sys
from unittest.mock import patch, MagicMock

import pytest

_project_root = os.path.join(os.path.dirname(__file__), "..", "..")
sys.path.insert(0, _project_root)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Ensure loguru is available (mock if not installed in test env)
try:
    import loguru  # noqa: F401
except ModuleNotFoundError:
    sys.modules.setdefault("loguru", MagicMock())

# Pre-mock the cost.allocation_manager module that tag_manager imports via relative import
_mock_cost_pkg = MagicMock()
_mock_alloc_mgr_module = MagicMock()
_MockCostAllocationManager = MagicMock()
_mock_alloc_mgr_module.CostAllocationManager = _MockCostAllocationManager
sys.modules.setdefault("src.cost", _mock_cost_pkg)
sys.modules.setdefault("src.cost.allocation_manager", _mock_alloc_mgr_module)

# Now we can safely import TagManager
from src.tag_manager import TagManager
from src.tag_retrievers.resource_groups import ResourceGroupsTagRetriever


@pytest.fixture(autouse=True)
def mock_aws_dependencies():
    """Mock all AWS-dependent components to prevent real API calls."""
    with patch("boto3.client") as mock_client:
        mock_instance = MagicMock()
        mock_instance.list_indexes.return_value = {"Indexes": []}
        mock_instance.get_resources.return_value = {"ResourceTagMappingList": []}
        mock_client.return_value = mock_instance

        # Reset the cost allocation manager mock for each test
        mock_cost_mgr_instance = MagicMock()
        _MockCostAllocationManager.reset_mock()
        _MockCostAllocationManager.return_value = mock_cost_mgr_instance

        yield {
            "boto3_client": mock_client,
            "cost_manager": mock_cost_mgr_instance,
        }


@pytest.fixture
def tag_manager(mock_aws_dependencies):
    """Create a TagManager instance with mocked dependencies."""
    with patch.dict(os.environ, {"TAGINSIGHTS_PREFER_CONFIG": "false"}):
        mgr = TagManager(
            region_name="us-east-1",
            prefer_config=False,
            enable_comprehensive_discovery=False,
        )
        return mgr


class TestTagManagerInit:
    """Test TagManager initialization."""

    def test_default_init(self, mock_aws_dependencies):
        with patch.dict(os.environ, {"TAGINSIGHTS_PREFER_CONFIG": "false"}):
            mgr = TagManager(
                region_name="us-west-2",
                prefer_config=False,
                enable_comprehensive_discovery=False,
            )
            assert mgr.region_name == "us-west-2"
            assert mgr.prefer_config is False

    def test_prefer_config_auto_detect_true(self, mock_aws_dependencies):
        with patch.dict(os.environ, {"TAGINSIGHTS_PREFER_CONFIG": "true"}):
            mgr = TagManager(
                region_name="us-east-1",
                prefer_config=None,
                enable_comprehensive_discovery=False,
            )
            assert mgr.prefer_config is True

    def test_prefer_config_auto_detect_false(self, mock_aws_dependencies):
        with patch.dict(os.environ, {"TAGINSIGHTS_PREFER_CONFIG": "false"}):
            mgr = TagManager(
                region_name="us-east-1",
                prefer_config=None,
                enable_comprehensive_discovery=False,
            )
            assert mgr.prefer_config is False

    def test_comprehensive_discovery_disabled(self, mock_aws_dependencies):
        with patch.dict(os.environ, {"TAGINSIGHTS_PREFER_CONFIG": "false"}):
            mgr = TagManager(enable_comprehensive_discovery=False, prefer_config=False)
            assert mgr._resource_explorer_retriever is None


class TestGetTagsForResource:
    """Test single resource tag retrieval."""

    def test_returns_tags_from_retriever(self, tag_manager):
        mock_retriever = MagicMock()
        mock_retriever.get_tags_safe.return_value = {
            "Environment": "prod",
            "Team": "platform",
        }
        mock_retriever.get_service_name.return_value = "test"
        tag_manager.registry.get_retriever_for_arn = MagicMock(
            return_value=mock_retriever
        )

        result = tag_manager.get_tags_for_resource(
            "arn:aws:ec2:us-east-1:123:instance/i-abc"
        )
        assert result == {"Environment": "prod", "Team": "platform"}

    def test_returns_empty_when_no_retriever(self, tag_manager):
        tag_manager.registry.get_retriever_for_arn = MagicMock(return_value=None)
        result = tag_manager.get_tags_for_resource(
            "arn:aws:unknown:us-east-1:123:thing/x"
        )
        assert result == {}

    def test_use_fallback_false_skips_resource_groups(self, tag_manager):
        mock_retriever = MagicMock(spec=ResourceGroupsTagRetriever)
        tag_manager.registry.get_retriever_for_arn = MagicMock(
            return_value=mock_retriever
        )

        result = tag_manager.get_tags_for_resource(
            "arn:aws:ec2:us-east-1:123:instance/i-abc", use_fallback=False
        )
        assert result == {}
        mock_retriever.get_tags_safe.assert_not_called()


class TestGetTagsForResources:
    """Test batch resource tag retrieval."""

    def test_empty_list_returns_empty(self, tag_manager):
        result = tag_manager.get_tags_for_resources([])
        assert result == {}

    def test_groups_by_retriever(self, tag_manager):
        mock_retriever = MagicMock()
        mock_retriever.get_batch_tags.return_value = {
            "arn:aws:ec2:us-east-1:123:instance/i-1": {"Name": "web1"},
            "arn:aws:ec2:us-east-1:123:instance/i-2": {"Name": "web2"},
        }
        mock_retriever.get_service_name.return_value = "ec2"
        tag_manager.registry.get_retriever_for_arn = MagicMock(
            return_value=mock_retriever
        )

        arns = [
            "arn:aws:ec2:us-east-1:123:instance/i-1",
            "arn:aws:ec2:us-east-1:123:instance/i-2",
        ]
        result = tag_manager.get_tags_for_resources(arns)
        assert len(result) == 2
        mock_retriever.get_batch_tags.assert_called_once()

    def test_no_retriever_returns_empty_tags(self, tag_manager):
        tag_manager.registry.get_retriever_for_arn = MagicMock(return_value=None)
        arns = ["arn:aws:fake:us-east-1:123:thing/x"]
        result = tag_manager.get_tags_for_resources(arns)
        assert result == {"arn:aws:fake:us-east-1:123:thing/x": {}}

    def test_batch_error_falls_back_to_individual(self, tag_manager):
        mock_retriever = MagicMock()
        mock_retriever.get_batch_tags.side_effect = Exception("batch failed")
        mock_retriever.get_tags_safe.return_value = {"Name": "recovered"}
        mock_retriever.get_service_name.return_value = "ec2"
        tag_manager.registry.get_retriever_for_arn = MagicMock(
            return_value=mock_retriever
        )

        arns = ["arn:aws:ec2:us-east-1:123:instance/i-1"]
        result = tag_manager.get_tags_for_resources(arns)
        assert result["arn:aws:ec2:us-east-1:123:instance/i-1"] == {"Name": "recovered"}


class TestFindTaggedResources:
    """Test resource search functionality."""

    def test_find_without_filters(self, tag_manager):
        with patch("src.tag_manager.ResourceGroupsTagRetriever") as mock_rg_cls:
            mock_rg = MagicMock()
            mock_rg.find_all_tagged_resources.return_value = [
                {
                    "ResourceARN": "arn:aws:ec2:us-east-1:123:instance/i-1",
                    "Tags": {"Name": "web"},
                },
            ]
            mock_rg_cls.return_value = mock_rg

            result = tag_manager.find_tagged_resources()
            assert len(result) == 1
            mock_rg.find_all_tagged_resources.assert_called_once_with(None)

    def test_find_with_tag_filters(self, tag_manager):
        with patch("src.tag_manager.ResourceGroupsTagRetriever") as mock_rg_cls:
            mock_rg = MagicMock()
            mock_rg.find_all_tagged_resources.return_value = []
            mock_rg_cls.return_value = mock_rg

            filters = [{"Key": "Environment", "Values": ["prod"]}]
            result = tag_manager.find_tagged_resources(tag_filters=filters)
            mock_rg.find_all_tagged_resources.assert_called_once_with(filters)

    def test_find_with_region_override(self, tag_manager):
        with patch("src.tag_manager.ResourceGroupsTagRetriever") as mock_rg_cls:
            mock_rg = MagicMock()
            mock_rg.find_all_tagged_resources.return_value = []
            mock_rg_cls.return_value = mock_rg

            tag_manager.find_tagged_resources(region_name="eu-west-1")
            mock_rg_cls.assert_called_once_with("eu-west-1")

    def test_find_handles_exception(self, tag_manager):
        with patch("src.tag_manager.ResourceGroupsTagRetriever") as mock_rg_cls:
            mock_rg = MagicMock()
            mock_rg.find_all_tagged_resources.side_effect = Exception("API error")
            mock_rg_cls.return_value = mock_rg

            result = tag_manager.find_tagged_resources()
            assert result == []


class TestGetResourceSummary:
    """Test resource summary statistics."""

    def test_summary_with_specific_arns(self, tag_manager):
        tag_manager.get_tags_for_resources = MagicMock(
            return_value={
                "arn:aws:ec2:us-east-1:123:instance/i-1": {
                    "Name": "web1",
                    "Env": "prod",
                },
                "arn:aws:ec2:us-east-1:123:instance/i-2": {},
                "arn:aws:s3:::my-bucket": {"Name": "data"},
            }
        )

        arns = [
            "arn:aws:ec2:us-east-1:123:instance/i-1",
            "arn:aws:ec2:us-east-1:123:instance/i-2",
            "arn:aws:s3:::my-bucket",
        ]
        summary = tag_manager.get_resource_summary(resource_arns=arns)

        assert summary["total_resources"] == 3
        assert summary["tagged_resources"] == 2
        assert summary["untagged_resources"] == 1
        assert summary["tag_coverage_percent"] == pytest.approx(66.67, rel=0.01)
        assert "Name" in summary["tag_keys"]
        assert "Env" in summary["tag_keys"]

    def test_summary_service_distribution(self, tag_manager):
        tag_manager.get_tags_for_resources = MagicMock(
            return_value={
                "arn:aws:ec2:us-east-1:123:instance/i-1": {"Name": "a"},
                "arn:aws:ec2:us-east-1:123:instance/i-2": {"Name": "b"},
                "arn:aws:s3:::bucket": {"Name": "c"},
            }
        )
        arns = list(tag_manager.get_tags_for_resources.return_value.keys())
        summary = tag_manager.get_resource_summary(resource_arns=arns)
        assert summary["service_distribution"]["ec2"] == 2
        assert summary["service_distribution"]["s3"] == 1

    def test_summary_empty_resources(self, tag_manager):
        tag_manager.find_tagged_resources = MagicMock(return_value=[])
        summary = tag_manager.get_resource_summary()
        assert summary["total_resources"] == 0
        assert summary["tag_coverage_percent"] == 0


class TestHybridMethods:
    """Test Config-first hybrid approach."""

    def test_hybrid_empty_arns(self, tag_manager):
        result = tag_manager.get_tags_for_resources_hybrid([])
        assert result == {}

    def test_hybrid_config_not_available(self, tag_manager):
        tag_manager.config_available = False
        tag_manager.get_tags_for_resources = MagicMock(
            return_value={
                "arn:aws:ec2:us-east-1:123:instance/i-1": {"Name": "web"},
            }
        )
        arns = ["arn:aws:ec2:us-east-1:123:instance/i-1"]
        result = tag_manager.get_tags_for_resources_hybrid(arns)
        assert "arn:aws:ec2:us-east-1:123:instance/i-1" in result

    def test_hybrid_config_available(self, tag_manager):
        tag_manager.config_available = True
        tag_manager.config_retriever = MagicMock()
        tag_manager.config_retriever.get_batch_tags.return_value = {
            "arn:aws:ec2:us-east-1:123:instance/i-1": {"Name": "from-config"},
        }
        tag_manager.get_tags_for_resources = MagicMock(return_value={})

        arns = ["arn:aws:ec2:us-east-1:123:instance/i-1"]
        result = tag_manager.get_tags_for_resources_hybrid(arns)
        assert result["arn:aws:ec2:us-east-1:123:instance/i-1"] == {
            "Name": "from-config"
        }

    def test_hybrid_api_fallback_for_missing(self, tag_manager):
        tag_manager.config_available = True
        tag_manager.config_retriever = MagicMock()
        tag_manager.config_retriever.get_batch_tags.return_value = {
            "arn:aws:ec2:us-east-1:123:instance/i-1": {"Name": "from-config"},
        }
        tag_manager.get_tags_for_resources = MagicMock(
            return_value={
                "arn:aws:ec2:us-east-1:123:instance/i-2": {"Name": "from-api"},
            }
        )

        arns = [
            "arn:aws:ec2:us-east-1:123:instance/i-1",
            "arn:aws:ec2:us-east-1:123:instance/i-2",
        ]
        result = tag_manager.get_tags_for_resources_hybrid(arns)
        assert result["arn:aws:ec2:us-east-1:123:instance/i-1"] == {
            "Name": "from-config"
        }
        assert result["arn:aws:ec2:us-east-1:123:instance/i-2"] == {"Name": "from-api"}

    def test_validate_hybrid_setup_not_enabled(self, tag_manager):
        tag_manager.prefer_config = False
        result = tag_manager.validate_hybrid_setup()
        assert result["hybrid_enabled"] is False
        assert result["hybrid_ready"] is False


class TestComprehensiveDiscovery:
    """Test comprehensive discovery methods."""

    def test_find_all_resources_no_explorer(self, tag_manager):
        tag_manager._resource_explorer_retriever = None
        tag_manager.find_tagged_resources = MagicMock(
            return_value=[
                {
                    "ResourceARN": "arn:aws:ec2:us-east-1:123:instance/i-1",
                    "Tags": {"Name": "x"},
                },
            ]
        )
        result = tag_manager.find_all_resources_comprehensive()
        assert len(result) == 1

    def test_find_all_resources_include_untagged_false(self, tag_manager):
        tag_manager.find_tagged_resources = MagicMock(return_value=[])
        result = tag_manager.find_all_resources_comprehensive(include_untagged=False)
        tag_manager.find_tagged_resources.assert_called_once()

    def test_get_untagged_resources_no_explorer(self, tag_manager):
        tag_manager._resource_explorer_retriever = None
        result = tag_manager.get_untagged_resources()
        assert result == []

    def test_is_comprehensive_discovery_available_false(self, tag_manager):
        tag_manager._resource_explorer_retriever = None
        assert tag_manager.is_comprehensive_discovery_available() is False

    def test_get_discovery_capabilities(self, tag_manager):
        tag_manager._resource_explorer_retriever = None
        caps = tag_manager.get_discovery_capabilities()
        assert caps["standard_discovery"] is True
        assert caps["comprehensive_discovery"] is False
        assert caps["region"] == "us-east-1"
        assert isinstance(caps["supported_services"], list)


class TestCostAllocationAndMisc:
    """Test cost allocation, custom retrievers, and string representation."""

    def test_get_active_cost_allocation_tags(self, tag_manager, mock_aws_dependencies):
        mock_aws_dependencies[
            "cost_manager"
        ].get_active_cost_allocation_tags.return_value = {"CostCenter", "Project"}
        result = tag_manager.get_active_cost_allocation_tags()
        assert result == ["CostCenter", "Project"]

    def test_clear_cost_allocation_cache(self, tag_manager, mock_aws_dependencies):
        tag_manager.clear_cost_allocation_cache()
        mock_aws_dependencies["cost_manager"].clear_cache.assert_called_once()

    def test_add_custom_retriever(self, tag_manager):
        mock_retriever = MagicMock()
        tag_manager.registry.register_retriever = MagicMock()
        tag_manager.add_custom_retriever(mock_retriever, region="eu-west-1")
        tag_manager.registry.register_retriever.assert_called_once_with(
            mock_retriever, "eu-west-1"
        )

    def test_str_format(self, tag_manager):
        s = str(tag_manager)
        assert "TagManager" in s
        assert "us-east-1" in s
