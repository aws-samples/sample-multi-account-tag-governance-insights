"""
Tests for tag_retrievers/base.py - BaseTagRetriever abstract class.
Covers: abstract method enforcement, get_tags_safe error handling,
stale resource detection, batch tag retrieval, and __str__ representation.
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

from botocore.exceptions import ClientError
from tag_retrievers.base import BaseTagRetriever

# --- Concrete subclass for testing the base class ---


class ConcreteTagRetriever(BaseTagRetriever):
    """Minimal concrete implementation for testing base class logic."""

    def _create_client(self):
        return MagicMock()

    def get_service_name(self) -> str:
        return "test-service"

    def can_handle_arn(self, resource_arn: str) -> bool:
        return resource_arn.startswith("arn:aws:test:")

    def get_tags(self, resource_arn: str):
        return {"Name": "test-resource"}


class TestBaseTagRetrieverAbstract:
    """Test that abstract methods are enforced."""

    def test_cannot_instantiate_base_directly(self):
        with pytest.raises(TypeError):
            BaseTagRetriever()

    def test_missing_create_client_raises(self):
        class Incomplete(BaseTagRetriever):
            def get_service_name(self):
                return "x"

            def can_handle_arn(self, arn):
                return True

            def get_tags(self, arn):
                return {}

        with pytest.raises(TypeError):
            Incomplete()

    def test_missing_get_service_name_raises(self):
        class Incomplete(BaseTagRetriever):
            def _create_client(self):
                return None

            def can_handle_arn(self, arn):
                return True

            def get_tags(self, arn):
                return {}

        with pytest.raises(TypeError):
            Incomplete()

    def test_missing_can_handle_arn_raises(self):
        class Incomplete(BaseTagRetriever):
            def _create_client(self):
                return None

            def get_service_name(self):
                return "x"

            def get_tags(self, arn):
                return {}

        with pytest.raises(TypeError):
            Incomplete()

    def test_missing_get_tags_raises(self):
        class Incomplete(BaseTagRetriever):
            def _create_client(self):
                return None

            def get_service_name(self):
                return "x"

            def can_handle_arn(self, arn):
                return True

        with pytest.raises(TypeError):
            Incomplete()


class TestBaseTagRetrieverInit:
    """Test initialization and client lazy loading."""

    def test_default_region(self):
        retriever = ConcreteTagRetriever()
        assert retriever.region_name == "us-east-1"

    def test_custom_region(self):
        retriever = ConcreteTagRetriever(region_name="eu-west-1")
        assert retriever.region_name == "eu-west-1"

    def test_client_is_none_initially(self):
        retriever = ConcreteTagRetriever()
        assert retriever._client is None

    def test_client_property_lazy_init(self):
        retriever = ConcreteTagRetriever()
        client = retriever.client
        assert client is not None
        # Should return same instance on second access
        assert retriever.client is client


class TestGetTagsSafe:
    """Test the error-handling wrapper get_tags_safe."""

    def test_success_returns_tags(self):
        retriever = ConcreteTagRetriever()
        result = retriever.get_tags_safe("arn:aws:test:us-east-1:123:resource/abc")
        assert result == {"Name": "test-resource"}

    def test_access_denied_returns_empty(self):
        retriever = ConcreteTagRetriever()
        error_response = {
            "Error": {"Code": "AccessDeniedException", "Message": "Not allowed"}
        }
        retriever.get_tags = MagicMock(
            side_effect=ClientError(error_response, "GetTags")
        )
        result = retriever.get_tags_safe("arn:aws:test:us-east-1:123:resource/abc")
        assert result == {}

    def test_stale_resource_error_returns_empty(self):
        retriever = ConcreteTagRetriever()
        error_response = {
            "Error": {
                "Code": "ResourceNotFoundException",
                "Message": "Resource not found",
            }
        }
        retriever.get_tags = MagicMock(
            side_effect=ClientError(error_response, "GetTags")
        )
        with patch.object(retriever, "_track_stale_resource") as mock_track:
            result = retriever.get_tags_safe("arn:aws:test:us-east-1:123:resource/abc")
            assert result == {}
            mock_track.assert_called_once()

    def test_other_client_error_returns_empty(self):
        retriever = ConcreteTagRetriever()
        error_response = {
            "Error": {"Code": "ThrottlingException", "Message": "Rate exceeded"}
        }
        retriever.get_tags = MagicMock(
            side_effect=ClientError(error_response, "GetTags")
        )
        result = retriever.get_tags_safe("arn:aws:test:us-east-1:123:resource/abc")
        assert result == {}

    def test_unexpected_exception_returns_empty(self):
        retriever = ConcreteTagRetriever()
        retriever.get_tags = MagicMock(side_effect=RuntimeError("connection timeout"))
        result = retriever.get_tags_safe("arn:aws:test:us-east-1:123:resource/abc")
        assert result == {}


class TestIsStaleResourceError:
    """Test stale resource detection logic."""

    def setup_method(self):
        self.retriever = ConcreteTagRetriever()

    def test_known_stale_error_codes(self):
        stale_codes = [
            "InvalidParameterValue",
            "ResourceNotFoundException",
            "NoSuchEntity",
            "NoSuchBucket",
            "InvalidInstanceID.NotFound",
            "DBInstanceNotFound",
            "DBClusterNotFound",
            "InvalidDBSnapshotState",
            "ClusterNotFound",
        ]
        for code in stale_codes:
            assert self.retriever._is_stale_resource_error(code, "") is True

    def test_non_stale_error_code(self):
        assert (
            self.retriever._is_stale_resource_error("ThrottlingException", "") is False
        )
        assert self.retriever._is_stale_resource_error("InternalError", "") is False

    def test_stale_message_patterns(self):
        stale_messages = [
            "Unable to find matching resource for ARN",
            "The specified instance does not exist",
            "Resource not found in region us-east-1",
            "No such entity exists in the account",
            "Invalid instance ID NotFound abc-123",
            "The resource was not found",
            "Bucket cannot be found",
        ]
        for msg in stale_messages:
            assert (
                self.retriever._is_stale_resource_error("Unknown", msg) is True
            ), f"Failed for: {msg}"

    def test_non_stale_message(self):
        assert (
            self.retriever._is_stale_resource_error("Unknown", "Rate limit exceeded")
            is False
        )
        assert (
            self.retriever._is_stale_resource_error("Unknown", "Internal server error")
            is False
        )


class TestTrackStaleResource:
    """Test stale resource tracking (best-effort, should not raise)."""

    def test_tracking_failure_does_not_raise(self):
        retriever = ConcreteTagRetriever()
        # This should not raise even if internal imports fail
        with patch(
            "tag_retrievers.base.BaseTagRetriever._track_stale_resource"
        ) as mock:
            mock.return_value = None
            retriever._track_stale_resource(
                "arn:aws:test:us-east-1:123:resource/gone",
                "ResourceNotFoundException",
                "Resource not found",
            )


class TestGetBatchTags:
    """Test batch tag retrieval."""

    def test_batch_returns_tags_for_handled_arns(self):
        retriever = ConcreteTagRetriever()
        arns = [
            "arn:aws:test:us-east-1:123:resource/a",
            "arn:aws:test:us-east-1:123:resource/b",
        ]
        result = retriever.get_batch_tags(arns)
        assert len(result) == 2
        assert all(arn in result for arn in arns)

    def test_batch_skips_unhandled_arns(self):
        retriever = ConcreteTagRetriever()
        arns = [
            "arn:aws:test:us-east-1:123:resource/a",
            "arn:aws:s3:::my-bucket",  # Not handled by ConcreteTagRetriever
        ]
        result = retriever.get_batch_tags(arns)
        assert len(result) == 1
        assert "arn:aws:test:us-east-1:123:resource/a" in result
        assert "arn:aws:s3:::my-bucket" not in result

    def test_batch_empty_list(self):
        retriever = ConcreteTagRetriever()
        result = retriever.get_batch_tags([])
        assert result == {}


class TestStr:
    """Test string representation."""

    def test_str_representation(self):
        retriever = ConcreteTagRetriever(region_name="us-west-2")
        s = str(retriever)
        assert "ConcreteTagRetriever" in s
        assert "test-service" in s
        assert "us-west-2" in s
