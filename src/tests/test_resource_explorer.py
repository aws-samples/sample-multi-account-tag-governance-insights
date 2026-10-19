"""Tests for ResourceExplorerTagRetriever."""

import os
import sys
from unittest.mock import patch, MagicMock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Mock loguru before any imports that use it
mock_loguru = MagicMock()
mock_loguru.logger = MagicMock()
sys.modules.setdefault("loguru", mock_loguru)

mock_re_client = MagicMock()

with patch("boto3.client", return_value=mock_re_client):
    from tag_retrievers.resource_explorer import (
        ResourceExplorerTagRetriever,
        validate_resource_explorer_setup,
        get_resource_explorer_regions,
    )


@pytest.fixture
def mock_client():
    return MagicMock()


@pytest.fixture
def retriever(mock_client):
    with patch("boto3.client", return_value=mock_client):
        r = ResourceExplorerTagRetriever(region_name="us-east-1")
        r._client = mock_client
        return r


@pytest.fixture
def validated_retriever(retriever, mock_client):
    mock_client.list_indexes.return_value = {
        "Indexes": [
            {"Region": "us-east-1", "Type": "LOCAL"},
            {"Region": "us-west-2", "Type": "AGGREGATOR"},
        ]
    }
    retriever.validate_setup()
    return retriever


class TestValidateSetup:
    def test_success_with_local_index(self, retriever, mock_client):
        mock_client.list_indexes.return_value = {
            "Indexes": [{"Region": "us-east-1", "Type": "LOCAL"}]
        }
        assert retriever.validate_setup() is True
        assert retriever._index_available is True
        assert retriever._aggregator_region is None

    def test_success_with_aggregator(self, retriever, mock_client):
        mock_client.list_indexes.return_value = {
            "Indexes": [
                {"Region": "us-east-1", "Type": "LOCAL"},
                {"Region": "us-west-2", "Type": "AGGREGATOR"},
            ]
        }
        assert retriever.validate_setup() is True
        assert retriever._aggregator_region == "us-west-2"

    def test_no_indexes_found(self, retriever, mock_client):
        mock_client.list_indexes.return_value = {"Indexes": []}
        assert retriever.validate_setup() is False
        assert retriever._index_available is False

    def test_no_index_in_target_region(self, retriever, mock_client):
        mock_client.list_indexes.return_value = {
            "Indexes": [{"Region": "eu-west-1", "Type": "LOCAL"}]
        }
        assert retriever.validate_setup() is False

    def test_access_denied_error(self, retriever, mock_client):
        import botocore.exceptions

        mock_client.list_indexes.side_effect = botocore.exceptions.ClientError(
            {"Error": {"Code": "AccessDeniedException", "Message": "Denied"}},
            "ListIndexes",
        )
        assert retriever.validate_setup() is False

    def test_generic_exception(self, retriever, mock_client):
        mock_client.list_indexes.side_effect = RuntimeError("Network error")
        assert retriever.validate_setup() is False
        assert retriever._setup_validated is True

    def test_caches_result(self, retriever, mock_client):
        mock_client.list_indexes.return_value = {
            "Indexes": [{"Region": "us-east-1", "Type": "LOCAL"}]
        }
        retriever.validate_setup()
        retriever.validate_setup()
        assert mock_client.list_indexes.call_count == 1


class TestGetTags:
    def test_returns_tags_for_resource(self, validated_retriever, mock_client):
        mock_client.search.return_value = {
            "Resources": [
                {
                    "Arn": "arn:aws:ec2:us-east-1:123:instance/i-1",
                    "Properties": [
                        {
                            "Name": "tags",
                            "Data": [
                                {"Key": "env", "Value": "prod"},
                                {"Key": "team", "Value": "platform"},
                            ],
                        }
                    ],
                }
            ]
        }
        assert validated_retriever.get_tags(
            "arn:aws:ec2:us-east-1:123:instance/i-1"
        ) == {"env": "prod", "team": "platform"}

    def test_returns_empty_when_not_found(self, validated_retriever, mock_client):
        mock_client.search.return_value = {"Resources": []}
        assert (
            validated_retriever.get_tags("arn:aws:ec2:us-east-1:123:instance/i-999")
            == {}
        )

    def test_returns_empty_when_setup_invalid(self, retriever, mock_client):
        mock_client.list_indexes.return_value = {"Indexes": []}
        retriever.validate_setup()
        assert retriever.get_tags("arn:aws:ec2:us-east-1:123:instance/i-1") == {}

    def test_handles_search_exception(self, validated_retriever, mock_client):
        mock_client.search.side_effect = Exception("Throttled")
        assert (
            validated_retriever.get_tags("arn:aws:ec2:us-east-1:123:instance/i-1") == {}
        )


class TestExtractTagsFromProperties:
    def test_extracts_tags_correctly(self, retriever):
        resource = {
            "Properties": [
                {
                    "Name": "tags",
                    "Data": [
                        {"Key": "Name", "Value": "web-server"},
                        {"Key": "Env", "Value": "staging"},
                    ],
                }
            ]
        }
        assert retriever._extract_tags_from_properties(resource) == {
            "Name": "web-server",
            "Env": "staging",
        }

    def test_no_tags_property(self, retriever):
        assert (
            retriever._extract_tags_from_properties(
                {"Properties": [{"Name": "other", "Data": []}]}
            )
            == {}
        )

    def test_empty_properties(self, retriever):
        assert retriever._extract_tags_from_properties({"Properties": []}) == {}
        assert retriever._extract_tags_from_properties({}) == {}

    def test_skips_none_keys(self, retriever):
        resource = {
            "Properties": [
                {
                    "Name": "tags",
                    "Data": [
                        {"Key": None, "Value": "val"},
                        {"Key": "good", "Value": "yes"},
                    ],
                }
            ]
        }
        assert retriever._extract_tags_from_properties(resource) == {"good": "yes"}

    def test_includes_empty_string_value(self, retriever):
        resource = {
            "Properties": [{"Name": "tags", "Data": [{"Key": "empty", "Value": ""}]}]
        }
        assert retriever._extract_tags_from_properties(resource) == {"empty": ""}


class TestNormalizeResourceArn:
    def test_fixes_cluster_snapshot_arn(self, retriever):
        arn = "arn:aws:rds:us-east-1:123:cluster-snapshot:rds:my-snapshot"
        assert (
            retriever._normalize_resource_arn(arn)
            == "arn:aws:rds:us-east-1:123:cluster-snapshot:my-snapshot"
        )

    def test_fixes_snapshot_arn(self, retriever):
        arn = "arn:aws:rds:us-east-1:123:snapshot:rds:my-snapshot"
        assert (
            retriever._normalize_resource_arn(arn)
            == "arn:aws:rds:us-east-1:123:snapshot:my-snapshot"
        )

    def test_no_change_for_normal_arn(self, retriever):
        arn = "arn:aws:ec2:us-east-1:123:instance/i-abc"
        assert retriever._normalize_resource_arn(arn) == arn

    def test_no_change_for_non_rds_arn(self, retriever):
        assert (
            retriever._normalize_resource_arn("arn:aws:s3:::my-bucket")
            == "arn:aws:s3:::my-bucket"
        )

    def test_handles_none(self, retriever):
        assert retriever._normalize_resource_arn(None) is None

    def test_handles_empty_string(self, retriever):
        assert retriever._normalize_resource_arn("") == ""


class TestFindAllResourcesComprehensive:
    def test_returns_empty_when_not_validated(self, retriever, mock_client):
        mock_client.list_indexes.return_value = {"Indexes": []}
        retriever.validate_setup()
        assert retriever.find_all_resources_comprehensive() == []

    def test_routes_to_batch_when_feature_flag_set(
        self, validated_retriever, monkeypatch
    ):
        monkeypatch.setenv("USE_BATCH_DISCOVERY", "true")
        with patch.object(
            validated_retriever, "_discover_resources_batch", return_value=[{"r": 1}]
        ) as m:
            result = validated_retriever.find_all_resources_comprehensive(
                region_specific=True
            )
            m.assert_called_once()
            assert result == [{"r": 1}]

    def test_routes_to_legacy_when_flag_unset(self, validated_retriever, monkeypatch):
        monkeypatch.setenv("USE_BATCH_DISCOVERY", "false")
        with patch.object(
            validated_retriever,
            "_discover_resources_from_local_index_legacy",
            return_value=[],
        ) as m:
            validated_retriever.find_all_resources_comprehensive(region_specific=True)
            m.assert_called_once()

    def test_routes_to_aggregator_batch(self, validated_retriever, monkeypatch):
        monkeypatch.setenv("USE_BATCH_DISCOVERY", "true")
        with patch.object(
            validated_retriever, "_discover_resources_batch_aggregator", return_value=[]
        ) as m:
            validated_retriever.find_all_resources_comprehensive(region_specific=False)
            m.assert_called_once()

    def test_routes_to_aggregator_legacy(self, validated_retriever, monkeypatch):
        monkeypatch.setenv("USE_BATCH_DISCOVERY", "false")
        with patch.object(
            validated_retriever,
            "_discover_resources_from_aggregator_legacy",
            return_value=[],
        ) as m:
            validated_retriever.find_all_resources_comprehensive(region_specific=False)
            m.assert_called_once()


def _make_paginator(resources_list):
    """Helper: create a mock paginator returning given resource pages."""
    paginator = MagicMock()
    paginator.paginate.return_value = [{"Resources": resources_list}]
    return paginator


class TestDiscoverLocalIndexLegacy:
    def test_filters_by_region(self, validated_retriever, mock_client):
        mock_client.get_paginator.return_value = _make_paginator(
            [
                {
                    "Arn": "arn:aws:ec2:us-east-1:123:i/1",
                    "Region": "us-east-1",
                    "ResourceType": "EC2",
                    "Service": "ec2",
                    "Properties": [],
                },
                {
                    "Arn": "arn:aws:ec2:eu-west-1:123:i/2",
                    "Region": "eu-west-1",
                    "ResourceType": "EC2",
                    "Service": "ec2",
                    "Properties": [],
                },
            ]
        )
        with patch("time.sleep"):
            result = validated_retriever._discover_resources_from_local_index_legacy(
                None, False
            )
        assert len(result) == 1
        assert result[0]["Region"] == "us-east-1"

    def test_respects_max_resources(self, validated_retriever, mock_client):
        resources = [
            {
                "Arn": f"arn:aws:ec2:us-east-1:123:i/{i}",
                "Region": "us-east-1",
                "ResourceType": "EC2",
                "Service": "ec2",
                "Properties": [],
            }
            for i in range(10)
        ]
        mock_client.get_paginator.return_value = _make_paginator(resources)
        with patch("time.sleep"):
            result = validated_retriever._discover_resources_from_local_index_legacy(
                3, False
            )
        assert len(result) == 3

    def test_includes_properties_when_requested(self, validated_retriever, mock_client):
        mock_client.get_paginator.return_value = _make_paginator(
            [
                {
                    "Arn": "arn:aws:ec2:us-east-1:123:i/1",
                    "Region": "us-east-1",
                    "ResourceType": "EC2",
                    "Service": "ec2",
                    "Properties": [{"Name": "instanceType", "Data": "t3.micro"}],
                }
            ]
        )
        with patch("time.sleep"):
            result = validated_retriever._discover_resources_from_local_index_legacy(
                None, True
            )
        assert "Properties" in result[0]

    def test_skips_resources_without_arn(self, validated_retriever, mock_client):
        mock_client.get_paginator.return_value = _make_paginator(
            [
                {"Arn": None, "Region": "us-east-1"},
                {
                    "Arn": "arn:aws:ec2:us-east-1:123:i/1",
                    "Region": "us-east-1",
                    "ResourceType": "EC2",
                    "Service": "ec2",
                    "Properties": [],
                },
            ]
        )
        with patch("time.sleep"):
            result = validated_retriever._discover_resources_from_local_index_legacy(
                None, False
            )
        assert len(result) == 1


class TestDiscoverBatch:
    def test_batch_discovery(self, validated_retriever, mock_client):
        mock_client.get_paginator.return_value = _make_paginator(
            [
                {
                    "Arn": "arn:aws:ec2:us-east-1:123:i/1",
                    "Region": "us-east-1",
                    "ResourceType": "EC2",
                    "Service": "ec2",
                    "Properties": [],
                }
            ]
        )
        with patch("time.sleep"):
            result = validated_retriever._discover_resources_batch(None, False)
        assert len(result) == 1

    def test_raises_on_error(self, validated_retriever, mock_client):
        paginator = MagicMock()
        paginator.paginate.side_effect = Exception("API error")
        mock_client.get_paginator.return_value = paginator
        with pytest.raises(Exception, match="API error"):
            with patch("time.sleep"):
                validated_retriever._discover_resources_batch(None, False)


class TestGetOrgViewArn:
    def test_finds_org_view(self, retriever, mock_client):
        mock_client.list_views.return_value = {
            "Views": [
                "arn:aws:resource-explorer-2:us-east-1:123:view/default/abc",
                "arn:aws:resource-explorer-2:us-east-1:123:view/org/def",
            ]
        }
        assert "/org/" in retriever._get_org_view_arn()

    def test_returns_none_when_no_org_view(self, retriever, mock_client):
        mock_client.list_views.return_value = {
            "Views": ["arn:aws:resource-explorer-2:us-east-1:123:view/default/abc"]
        }
        assert retriever._get_org_view_arn() is None

    def test_handles_exception(self, retriever, mock_client):
        mock_client.list_views.side_effect = Exception("Access denied")
        assert retriever._get_org_view_arn() is None


class TestDiscoverBatchAggregator:
    def test_uses_org_view_when_available(self, validated_retriever, mock_client):
        mock_client.list_views.return_value = {
            "Views": ["arn:aws:resource-explorer-2:us-east-1:123:view/org/abc"]
        }
        paginator = MagicMock()
        paginator.paginate.return_value = [{"Resources": []}]
        mock_client.get_paginator.return_value = paginator
        with patch("time.sleep"):
            validated_retriever._discover_resources_batch_aggregator(None, False)
        assert "ViewArn" in paginator.paginate.call_args[1]

    def test_works_without_org_view(self, validated_retriever, mock_client):
        mock_client.list_views.return_value = {"Views": []}
        paginator = MagicMock()
        paginator.paginate.return_value = [{"Resources": []}]
        mock_client.get_paginator.return_value = paginator
        with patch("time.sleep"):
            assert (
                validated_retriever._discover_resources_batch_aggregator(None, False)
                == []
            )


class TestSearchResourcesByQuery:
    def test_returns_formatted_resources(self, validated_retriever, mock_client):
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {
                "Resources": [
                    {
                        "Arn": "arn:aws:s3:::bucket1",
                        "Tags": {"env": "prod"},
                        "ResourceType": "AWS::S3::Bucket",
                        "Service": "s3",
                        "Region": "us-east-1",
                    }
                ]
            }
        ]
        mock_client.get_paginator.return_value = paginator
        result = validated_retriever.search_resources_by_query("resourcetype:s3")
        assert len(result) == 1
        assert result[0]["ResourceARN"] == "arn:aws:s3:::bucket1"

    def test_respects_max_results(self, validated_retriever, mock_client):
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {
                "Resources": [
                    {
                        "Arn": f"arn:{i}",
                        "ResourceType": "T",
                        "Service": "s",
                        "Region": "r",
                    }
                    for i in range(50)
                ]
            }
        ]
        mock_client.get_paginator.return_value = paginator
        assert (
            len(validated_retriever.search_resources_by_query("*", max_results=5)) == 5
        )

    def test_returns_empty_when_invalid_setup(self, retriever, mock_client):
        mock_client.list_indexes.return_value = {"Indexes": []}
        retriever.validate_setup()
        assert retriever.search_resources_by_query("test") == []

    def test_handles_search_error(self, validated_retriever, mock_client):
        paginator = MagicMock()
        paginator.paginate.side_effect = Exception("Search failed")
        mock_client.get_paginator.return_value = paginator
        assert validated_retriever.search_resources_by_query("bad-query") == []


class TestGetBatchTags:
    def test_returns_tags_for_all_arns(self, validated_retriever, mock_client):
        mock_client.search.side_effect = [
            {
                "Resources": [
                    {
                        "Properties": [
                            {"Name": "tags", "Data": [{"Key": "k", "Value": "v"}]}
                        ]
                    }
                ]
            },
            {"Resources": []},
        ]
        with patch("time.sleep"):
            result = validated_retriever.get_batch_tags(["arn:1", "arn:2"])
        assert result == {"arn:1": {"k": "v"}, "arn:2": {}}

    def test_empty_input(self, validated_retriever):
        assert validated_retriever.get_batch_tags([]) == {}

    def test_returns_empty_when_invalid_setup(self, retriever, mock_client):
        mock_client.list_indexes.return_value = {"Indexes": []}
        retriever.validate_setup()
        assert retriever.get_batch_tags(["arn:1"]) == {}


class TestGetDiscoveryStatistics:
    def test_returns_stats_when_available(self, validated_retriever, mock_client):
        mock_client.list_supported_resource_types.return_value = {
            "ResourceTypes": [
                {"ResourceType": "AWS::EC2::Instance"},
                {"ResourceType": "AWS::S3::Bucket"},
            ]
        }
        stats = validated_retriever.get_discovery_statistics()
        assert stats["service_available"] is True
        assert stats["index_configured"] is True
        assert stats["aggregator_available"] is True
        assert stats["supported_resource_count"] == 2

    def test_returns_minimal_stats_when_unavailable(self, retriever, mock_client):
        mock_client.list_indexes.return_value = {"Indexes": []}
        retriever.validate_setup()
        stats = retriever.get_discovery_statistics()
        assert stats["service_available"] is False

    def test_handles_resource_types_error(self, validated_retriever, mock_client):
        mock_client.list_supported_resource_types.side_effect = Exception("API error")
        stats = validated_retriever.get_discovery_statistics()
        assert stats["service_available"] is True
        assert stats["supported_resource_types"] == []


class TestMiscMethods:
    def test_str_available(self, validated_retriever):
        assert "available" in str(validated_retriever)

    def test_str_unavailable(self, retriever, mock_client):
        mock_client.list_indexes.return_value = {"Indexes": []}
        retriever.validate_setup()
        assert "unavailable" in str(retriever)

    def test_can_handle_arn_valid(self, validated_retriever):
        assert (
            validated_retriever.can_handle_arn("arn:aws:ec2:us-east-1:123:instance/i-1")
            is True
        )

    def test_can_handle_arn_non_aws(self, validated_retriever):
        assert validated_retriever.can_handle_arn("not-an-arn") is False

    def test_can_handle_arn_when_unavailable(self, retriever, mock_client):
        mock_client.list_indexes.return_value = {"Indexes": []}
        retriever.validate_setup()
        assert (
            retriever.can_handle_arn("arn:aws:ec2:us-east-1:123:instance/i-1") is False
        )


class TestUtilityFunctions:
    def test_validate_resource_explorer_setup(self):
        with patch("boto3.client") as mock_boto:
            client = MagicMock()
            client.list_indexes.return_value = {
                "Indexes": [{"Region": "us-east-1", "Type": "LOCAL"}]
            }
            client.list_supported_resource_types.return_value = {"ResourceTypes": []}
            mock_boto.return_value = client
            result = validate_resource_explorer_setup("us-east-1")
            assert result["resource_explorer_available"] is True
            assert result["ready_for_comprehensive_discovery"] is True

    def test_validate_resource_explorer_setup_not_available(self):
        with patch("boto3.client") as mock_boto:
            client = MagicMock()
            client.list_indexes.return_value = {"Indexes": []}
            mock_boto.return_value = client
            result = validate_resource_explorer_setup("us-east-1")
            assert result["resource_explorer_available"] is False

    def test_get_resource_explorer_regions(self):
        with patch("boto3.client") as mock_boto:
            client = MagicMock()
            client.list_indexes.side_effect = [
                {"Indexes": [{"Region": "us-east-1", "Type": "LOCAL"}]},
                {"Indexes": [{"Region": "us-west-2", "Type": "LOCAL"}]},
                {"Indexes": []},
                {"Indexes": []},
                {"Indexes": []},
                {"Indexes": []},
                {"Indexes": []},
            ]
            mock_boto.return_value = client
            regions = get_resource_explorer_regions()
            assert "us-east-1" in regions
            assert "us-west-2" in regions
            assert len(regions) == 2
