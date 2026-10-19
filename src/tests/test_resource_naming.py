"""
Comprehensive tests for src/utils/resource_naming.py

Covers: ResourceNamingManager methods and module-level convenience functions.
All AWS calls (STS) are mocked to keep tests pure-logic.
"""

import importlib.util
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from unittest.mock import patch, MagicMock

# Load resource_naming directly to avoid utils/__init__.py pulling in loguru
_spec = importlib.util.spec_from_file_location(
    "resource_naming",
    os.path.join(os.path.dirname(__file__), "..", "utils", "resource_naming.py"),
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

ResourceNamingManager = _mod.ResourceNamingManager


@pytest.fixture
def manager():
    """ResourceNamingManager with controlled env."""
    with patch.dict(
        os.environ,
        {
            "AWS_REGION": "us-east-1",
            "ENVIRONMENT": "prod",
            "AWS_ACCOUNT_ID": "123456789012",
        },
        clear=False,
    ):
        yield ResourceNamingManager(project_name="ti")


@pytest.fixture
def manager_dev():
    """Manager configured for dev environment."""
    with patch.dict(
        os.environ,
        {
            "AWS_REGION": "us-west-2",
            "ENVIRONMENT": "dev",
            "AWS_ACCOUNT_ID": "999888777666",
        },
        clear=False,
    ):
        yield ResourceNamingManager(project_name="ti")


class TestGetResourceName:
    """Tests for the core get_resource_name method."""

    def test_basic_naming(self, manager):
        result = manager.get_resource_name("central-analytics")
        assert result == "ti-central-analytics-us-east-1-prod"

    def test_explicit_region_and_env(self, manager):
        result = manager.get_resource_name("work-queue", "eu-west-1", "staging")
        assert result == "ti-work-queue-eu-west-1-staging"

    def test_custom_project_name(self):
        with patch.dict(os.environ, {"AWS_REGION": "us-east-1", "ENVIRONMENT": "prod"}):
            mgr = ResourceNamingManager(project_name="myapp")
            assert mgr.get_resource_name("api") == "myapp-api-us-east-1-prod"

    def test_project_name_lowercased(self):
        with patch.dict(os.environ, {"AWS_REGION": "us-east-1", "ENVIRONMENT": "prod"}):
            mgr = ResourceNamingManager(project_name="MyApp")
            assert mgr.get_resource_name("api") == "myapp-api-us-east-1-prod"

    def test_env_defaults_from_environment_vars(self, manager_dev):
        result = manager_dev.get_resource_name("worker")
        assert result == "ti-worker-us-west-2-dev"


class TestConvenienceGenerators:
    """Tests for get_table_name, get_queue_name, get_lambda_name, etc."""

    def test_get_table_name(self, manager):
        assert (
            manager.get_table_name("regional-state")
            == "ti-regional-state-us-east-1-prod"
        )

    def test_get_queue_name_no_suffix(self, manager):
        # Queue should NOT append -queue suffix
        result = manager.get_queue_name("work")
        assert result == "ti-work-us-east-1-prod"
        assert "-queue" not in result

    def test_get_lambda_name(self, manager):
        assert (
            manager.get_lambda_name("orchestrator") == "ti-orchestrator-us-east-1-prod"
        )

    def test_get_role_name_appends_role(self, manager):
        result = manager.get_role_name("regional-worker")
        assert result == "ti-regional-worker-role-us-east-1-prod"

    def test_get_stack_name_appends_stack(self, manager):
        result = manager.get_stack_name("infrastructure")
        assert result == "ti-infrastructure-stack-us-east-1-prod"

    def test_get_dashboard_name_appends_dashboard(self, manager):
        result = manager.get_dashboard_name("ops")
        assert result == "ti-ops-dashboard-us-east-1-prod"

    def test_get_alarm_name_appends_alarm(self, manager):
        result = manager.get_alarm_name("high-cpu")
        assert result == "ti-high-cpu-alarm-us-east-1-prod"

    def test_get_topic_name_appends_topic(self, manager):
        result = manager.get_topic_name("alerts")
        assert result == "ti-alerts-topic-us-east-1-prod"

    def test_get_rule_name_appends_rule(self, manager):
        result = manager.get_rule_name("resource-changes")
        assert result == "ti-resource-changes-rule-us-east-1-prod"


class TestAccountIdNaming:
    """Tests for methods that include AWS account ID."""

    def test_get_bucket_name_with_account_id(self, manager):
        with patch.object(
            manager, "_get_current_account_id", return_value="123456789012"
        ):
            result = manager.get_bucket_name("cost-data")
            assert result == "ti-cost-data-us-east-1-prod-123456789012"

    def test_get_bucket_name_without_account_id(self, manager):
        result = manager.get_bucket_name("cost-data", include_account_id=False)
        assert result == "ti-cost-data-us-east-1-prod"

    def test_get_lambda_name_with_account(self, manager):
        with patch.object(
            manager, "_get_current_account_id", return_value="123456789012"
        ):
            result = manager.get_lambda_name_with_account("orchestrator")
            assert result == "ti-orchestrator-us-east-1-prod-123456789012"


class TestDatabaseNaming:
    """Tests for get_database_name (Athena/Glue compatible)."""

    def test_hyphens_converted_to_underscores(self, manager):
        with patch.object(
            manager, "_get_current_account_id", return_value="123456789012"
        ):
            result = manager.get_database_name("cost_analytics")
            assert result == "ti_cost_analytics_us_east_1_prod_123456789012"
            # No hyphens in the output
            assert "-" not in result

    def test_without_account_id(self, manager):
        result = manager.get_database_name("cost_analytics", include_account_id=False)
        assert result == "ti_cost_analytics_us_east_1_prod"

    def test_explicit_region(self, manager):
        result = manager.get_database_name(
            "tag_data",
            region="ap-southeast-1",
            environment="staging",
            include_account_id=False,
        )
        assert result == "ti_tag_data_ap_southeast_1_staging"


class TestParseResourceName:
    """Tests for parse_resource_name."""

    def test_standard_name(self, manager):
        parsed = manager.parse_resource_name("ti-work-queue-us-east-1-prod")
        assert parsed["project"] == "ti"
        assert parsed["environment"] == "prod"
        assert parsed["region"] == "us-east-1"
        assert parsed["component"] == "work-queue"

    def test_multi_part_region(self, manager):
        parsed = manager.parse_resource_name("ti-api-ap-southeast-1-dev")
        assert parsed["region"] == "ap-southeast-1"
        assert parsed["environment"] == "dev"

    def test_single_component(self, manager):
        parsed = manager.parse_resource_name("ti-worker-us-west-2-staging")
        assert parsed["component"] == "worker"
        assert parsed["region"] == "us-west-2"
        assert parsed["environment"] == "staging"

    def test_raises_on_too_few_parts(self, manager):
        with pytest.raises(ValueError, match="Invalid resource name format"):
            manager.parse_resource_name("ti-x-y")

    def test_raises_on_minimal_input(self, manager):
        with pytest.raises(ValueError):
            manager.parse_resource_name("abc")


class TestValidateResourceName:
    """Tests for validate_resource_name."""

    @pytest.mark.parametrize(
        "name",
        [
            "ti-work-queue-us-east-1-prod",
            "ti-central-analytics-us-west-2-dev",
            "ti-api-eu-west-1-staging",
        ],
    )
    def test_valid_names(self, manager, name):
        assert manager.validate_resource_name(name) is True

    @pytest.mark.parametrize(
        "name,reason",
        [
            ("other-work-us-east-1-prod", "wrong project prefix"),
            ("ti-Work-us-east-1-prod", "uppercase component"),
            ("ti-x-y", "too few parts"),
            ("", "empty string"),
        ],
    )
    def test_invalid_names(self, manager, name, reason):
        assert manager.validate_resource_name(name) is False, f"Failed: {reason}"


class TestPrivateHelpers:
    """Tests for _get_current_region, _get_current_environment, _get_current_account_id."""

    def test_get_current_region_from_aws_region(self):
        with patch.dict(os.environ, {"AWS_REGION": "eu-central-1"}, clear=False):
            mgr = ResourceNamingManager()
            assert mgr._get_current_region() == "eu-central-1"

    def test_get_current_region_from_default_region(self):
        env = {"AWS_DEFAULT_REGION": "ap-northeast-1"}
        with patch.dict(os.environ, env, clear=True):
            mgr = ResourceNamingManager()
            assert mgr._get_current_region() == "ap-northeast-1"

    def test_get_current_region_fallback(self):
        with patch.dict(os.environ, {}, clear=True):
            mgr = ResourceNamingManager()
            assert mgr._get_current_region() == "us-east-1"

    def test_get_current_environment_from_env(self):
        with patch.dict(os.environ, {"ENVIRONMENT": "staging"}, clear=False):
            mgr = ResourceNamingManager()
            assert mgr._get_current_environment() == "staging"

    def test_get_current_environment_fallback(self):
        with patch.dict(os.environ, {}, clear=True):
            mgr = ResourceNamingManager()
            assert mgr._get_current_environment() == "prod"

    def test_get_account_id_from_sts(self):
        mgr = ResourceNamingManager()
        mock_sts = MagicMock()
        mock_sts.get_caller_identity.return_value = {"Account": "111222333444"}
        with patch("boto3.client", return_value=mock_sts):
            assert mgr._get_current_account_id() == "111222333444"

    def test_get_account_id_fallback_to_env(self):
        mgr = ResourceNamingManager()
        with patch("boto3.client", side_effect=Exception("no creds")):
            with patch.dict(os.environ, {"AWS_ACCOUNT_ID": "555666777888"}):
                assert mgr._get_current_account_id() == "555666777888"

    def test_get_account_id_raises_when_unavailable(self):
        mgr = ResourceNamingManager()
        with patch("boto3.client", side_effect=Exception("no creds")):
            with patch.dict(os.environ, {}, clear=True):
                with pytest.raises(
                    ValueError, match="Unable to determine AWS account ID"
                ):
                    mgr._get_current_account_id()


class TestModuleLevelFunctions:
    """Verify module-level functions delegate to the global naming_manager."""

    @pytest.mark.parametrize(
        "fn_name,args,expected",
        [
            ("get_resource_name", ("api",), "ti-api-us-east-1-prod"),
            ("get_queue_name", ("work",), "ti-work-us-east-1-prod"),
        ],
    )
    def test_convenience_functions(self, fn_name, args, expected):
        fn = getattr(_mod, fn_name)
        with patch.dict(os.environ, {"AWS_REGION": "us-east-1", "ENVIRONMENT": "prod"}):
            assert fn(*args) == expected

    def test_validate_resource_name_function(self):
        fn = _mod.validate_resource_name
        assert fn("ti-api-us-east-1-prod") is True
        assert fn("bad") is False
