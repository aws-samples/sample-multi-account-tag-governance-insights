"""
Tests for aggregator_coordinator Lambda handler.

Covers: checkpoint logic, account discovery, worker invocation,
sequential scan, fan-out scan, and main handler flow.
"""

import json
import os
import sys
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# ── Fixtures & helpers ────────────────────────────────────────────────────────

ENV_VARS = {
    "ENVIRONMENT": "test",
    "HUB_REGION": "us-east-1",
    "CENTRAL_ANALYTICS_BUCKET": "test-analytics-bucket",
    "WORKER_FUNCTION_NAME": "test-worker-function",
    "MAX_SCAN_DAYS": "3",
    "CHECKPOINT_SIGNING_SECRET": "deadbeef" * 8,
    "LOG_LEVEL": "WARNING",
}


class NoSuchKey(Exception):
    pass


def _make_s3_mock():
    mock = MagicMock()
    mock.exceptions.NoSuchKey = NoSuchKey
    return mock


@pytest.fixture(autouse=True)
def reset_module_cache():
    """Remove cached module so each test gets a fresh import."""
    mods_to_remove = [k for k in sys.modules if "aggregator_coordinator" in k]
    for mod in mods_to_remove:
        del sys.modules[mod]
    yield


@pytest.fixture(autouse=True)
def patch_env():
    """Ensure env vars are set for all tests (signing secret etc)."""
    with patch.dict(os.environ, ENV_VARS):
        yield


def _import_module(s3_mock=None, lambda_mock=None):
    """Import the module with mocked boto3 clients."""
    s3 = s3_mock or _make_s3_mock()
    lam = lambda_mock or MagicMock()

    def client_factory(service, **kwargs):
        if service == "s3":
            return s3
        if service == "lambda":
            return lam
        if service == "cloudwatch":
            return MagicMock()
        if service == "organizations":
            return MagicMock()
        if service == "resource-explorer-2":
            return MagicMock()
        if service == "sts":
            m = MagicMock()
            m.get_caller_identity.return_value = {"Account": "111111111111"}
            return m
        return MagicMock()

    with patch.dict(os.environ, ENV_VARS):
        with patch("boto3.client", side_effect=client_factory):
            import lambda_handlers.aggregator_coordinator as mod

            # Patch module-level clients
            mod.s3 = s3
            mod.lambda_client = lam
            # Reset the cached signing secret so it re-reads from env
            mod._cached_signing_secret = None
            return mod


def _make_context(remaining_ms=900000):
    ctx = MagicMock()
    ctx.get_remaining_time_in_millis.return_value = remaining_ms
    return ctx


# ── Checkpoint tests ──────────────────────────────────────────────────────────


class TestCheckpointSignature:
    def test_generate_and_validate_signature(self):
        mod = _import_module()
        checkpoint = {"scan_id": "2026-01-01", "completed": [], "completed_count": 0}
        sig = mod._generate_checkpoint_signature(checkpoint)
        assert isinstance(sig, str)
        assert len(sig) == 64  # SHA-256 hex digest

        checkpoint["signature"] = sig
        assert mod._validate_checkpoint_signature(checkpoint) is True

    def test_tampered_checkpoint_rejected(self):
        mod = _import_module()
        checkpoint = {"scan_id": "2026-01-01", "completed": [], "completed_count": 0}
        sig = mod._generate_checkpoint_signature(checkpoint)
        checkpoint["signature"] = sig

        # Tamper with data
        checkpoint["completed_count"] = 999
        assert mod._validate_checkpoint_signature(checkpoint) is False

    def test_missing_signature_rejected(self):
        mod = _import_module()
        checkpoint = {"scan_id": "2026-01-01", "completed": []}
        assert mod._validate_checkpoint_signature(checkpoint) is False


class TestReadCheckpoint:
    def test_returns_none_when_no_checkpoint(self):
        s3 = _make_s3_mock()
        s3.get_object.side_effect = NoSuchKey("not found")
        mod = _import_module(s3_mock=s3)
        assert mod.read_checkpoint() is None

    def test_returns_none_on_tampered_checkpoint(self):
        s3 = _make_s3_mock()
        bad_checkpoint = json.dumps(
            {
                "scan_id": "2026-01-01",
                "completed": [],
                "signature": "invalid_signature_value",
            }
        ).encode()
        s3.get_object.return_value = {"Body": MagicMock(read=lambda: bad_checkpoint)}
        mod = _import_module(s3_mock=s3)
        assert mod.read_checkpoint() is None

    def test_returns_valid_checkpoint(self):
        s3 = _make_s3_mock()
        mod = _import_module(s3_mock=s3)

        # Build a correctly signed checkpoint
        cp = {
            "scan_id": "2026-06-01",
            "completed": ["111/us-east-1"],
            "completed_count": 1,
        }
        cp["signature"] = mod._generate_checkpoint_signature(cp)
        s3.get_object.return_value = {
            "Body": MagicMock(read=lambda: json.dumps(cp).encode())
        }

        result = mod.read_checkpoint()
        assert result is not None
        assert result["scan_id"] == "2026-06-01"


class TestWriteCheckpoint:
    def test_write_calls_put_object(self):
        s3 = _make_s3_mock()
        mod = _import_module(s3_mock=s3)
        cp = {
            "scan_id": "2026-01-01",
            "completed": [],
            "completed_count": 0,
            "total_count": 5,
        }
        mod.write_checkpoint(cp)
        s3.put_object.assert_called_once()
        call_kwargs = s3.put_object.call_args[1]
        assert call_kwargs["Bucket"] == "test-analytics-bucket"
        body = json.loads(call_kwargs["Body"])
        assert "signature" in body

    def test_write_raises_on_s3_error(self):
        s3 = _make_s3_mock()
        s3.put_object.side_effect = Exception("S3 down")
        mod = _import_module(s3_mock=s3)
        with pytest.raises(Exception, match="S3 down"):
            mod.write_checkpoint(
                {"scan_id": "x", "completed_count": 0, "total_count": 1}
            )


# ── Account discovery tests ───────────────────────────────────────────────────


class TestDiscoverAccounts:
    def test_organizations_success(self):
        s3 = _make_s3_mock()
        mod = _import_module(s3_mock=s3)

        mock_orgs = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {
                "Accounts": [
                    {"Id": "111111111111", "Status": "ACTIVE"},
                    {"Id": "222222222222", "Status": "ACTIVE"},
                    {"Id": "333333333333", "Status": "SUSPENDED"},
                ]
            }
        ]
        mock_orgs.get_paginator.return_value = paginator

        with patch("boto3.client", return_value=mock_orgs):
            accounts = mod.list_accounts_from_organizations()

        assert "111111111111" in accounts
        assert "222222222222" in accounts
        assert "333333333333" not in accounts

    def test_discover_accounts_falls_back_to_re(self):
        s3 = _make_s3_mock()
        mod = _import_module(s3_mock=s3)

        from botocore.exceptions import ClientError

        error_resp = {"Error": {"Code": "AccessDeniedException", "Message": "denied"}}

        mock_re = MagicMock()
        mock_re.list_indexes.return_value = {
            "Indexes": [
                {
                    "Arn": "arn:aws:resource-explorer-2:us-east-1:444444444444:index/local"
                },
            ]
        }

        def client_factory(service, **kwargs):
            if service == "organizations":
                m = MagicMock()
                m.get_paginator.side_effect = ClientError(error_resp, "ListAccounts")
                return m
            if service == "resource-explorer-2":
                return mock_re
            return MagicMock()

        with patch("boto3.client", side_effect=client_factory):
            accounts = mod.discover_accounts()

        assert "444444444444" in accounts


# ── Worker invocation tests ───────────────────────────────────────────────────


class TestInvokeWorker:
    def test_sync_invoke_success(self):
        lam = MagicMock()
        lam.invoke.return_value = {"StatusCode": 200, "Payload": MagicMock()}
        mod = _import_module(lambda_mock=lam)

        result = mod.invoke_worker("111", "us-east-1", "scan-1", async_mode=False)
        assert result is True
        call_kwargs = lam.invoke.call_args[1]
        assert call_kwargs["InvocationType"] == "RequestResponse"

    def test_async_invoke_success(self):
        lam = MagicMock()
        lam.invoke.return_value = {"StatusCode": 202}
        mod = _import_module(lambda_mock=lam)

        result = mod.invoke_worker("111", "us-east-1", "scan-1", async_mode=True)
        assert result is True
        call_kwargs = lam.invoke.call_args[1]
        assert call_kwargs["InvocationType"] == "Event"

    def test_sync_invoke_function_error(self):
        lam = MagicMock()
        lam.invoke.return_value = {
            "StatusCode": 200,
            "FunctionError": "Unhandled",
            "Payload": MagicMock(
                read=lambda: json.dumps({"errorMessage": "boom"}).encode()
            ),
        }
        mod = _import_module(lambda_mock=lam)
        result = mod.invoke_worker("111", "us-east-1", "scan-1", async_mode=False)
        assert result is False

    def test_invoke_exception(self):
        lam = MagicMock()
        lam.invoke.side_effect = Exception("network error")
        mod = _import_module(lambda_mock=lam)
        result = mod.invoke_worker("111", "us-east-1", "scan-1")
        assert result is False


# ── Sequential scan tests ─────────────────────────────────────────────────────


class TestSequentialScan:
    def test_processes_all_pairs(self):
        s3 = _make_s3_mock()
        lam = MagicMock()
        lam.invoke.return_value = {"StatusCode": 200, "Payload": MagicMock()}
        mod = _import_module(s3_mock=s3, lambda_mock=lam)

        pairs = [("111", "us-east-1"), ("222", "us-west-2")]
        checkpoint = {
            "completed": [],
            "completed_count": 0,
            "total_count": 2,
            "errors": [],
            "last_updated": "",
        }
        ctx = _make_context(remaining_ms=900000)

        result = mod.sequential_scan(pairs, checkpoint, "scan-1", ctx)
        assert result["completed_count"] == 2
        assert "111/us-east-1" in result["completed"]
        assert "222/us-west-2" in result["completed"]

    def test_skips_already_completed(self):
        s3 = _make_s3_mock()
        lam = MagicMock()
        lam.invoke.return_value = {"StatusCode": 200, "Payload": MagicMock()}
        mod = _import_module(s3_mock=s3, lambda_mock=lam)

        pairs = [("111", "us-east-1"), ("222", "us-west-2")]
        checkpoint = {
            "completed": ["111/us-east-1"],
            "completed_count": 1,
            "total_count": 2,
            "errors": [],
            "last_updated": "",
        }
        ctx = _make_context()

        mod.sequential_scan(pairs, checkpoint, "scan-1", ctx)
        # Only invoked once for 222/us-west-2
        assert lam.invoke.call_count == 1

    def test_stops_at_timeout(self):
        s3 = _make_s3_mock()
        lam = MagicMock()
        lam.invoke.return_value = {"StatusCode": 200, "Payload": MagicMock()}
        mod = _import_module(s3_mock=s3, lambda_mock=lam)

        pairs = [("111", "us-east-1"), ("222", "us-west-2"), ("333", "eu-west-1")]
        checkpoint = {
            "completed": [],
            "completed_count": 0,
            "total_count": 3,
            "errors": [],
            "last_updated": "",
        }
        # Return enough time for first call, then signal timeout
        ctx = MagicMock()
        ctx.get_remaining_time_in_millis.side_effect = [900000, 100000, 100000]

        result = mod.sequential_scan(pairs, checkpoint, "scan-1", ctx)
        # Should have processed only 1 (second call sees 100000 < TIMEOUT_BUFFER_MS=120000)
        assert lam.invoke.call_count == 1


# ── Fan-out scan tests ────────────────────────────────────────────────────────


class TestFanoutScan:
    def test_dispatches_all_remaining(self):
        lam = MagicMock()
        lam.invoke.return_value = {"StatusCode": 202}
        mod = _import_module(lambda_mock=lam)

        remaining = [("111", "us-east-1"), ("222", "us-west-2")]
        result = mod.fanout_scan(remaining, "scan-1")
        assert result["dispatched"] == 2
        assert result["errors"] == 0
        assert lam.invoke.call_count == 2

    def test_counts_errors(self):
        lam = MagicMock()
        lam.invoke.side_effect = [{"StatusCode": 202}, Exception("throttled")]
        mod = _import_module(lambda_mock=lam)

        remaining = [("111", "us-east-1"), ("222", "us-west-2")]
        result = mod.fanout_scan(remaining, "scan-1")
        assert result["dispatched"] == 1
        assert result["errors"] == 1


# ── Main handler tests ────────────────────────────────────────────────────────


class TestLambdaHandler:
    def test_raises_if_bucket_not_set(self):
        env = {**ENV_VARS, "CENTRAL_ANALYTICS_BUCKET": ""}
        with patch.dict(os.environ, env):
            s3 = _make_s3_mock()
            mod = _import_module(s3_mock=s3)
            mod.CENTRAL_ANALYTICS_BUCKET = ""
            with pytest.raises(ValueError, match="CENTRAL_ANALYTICS_BUCKET"):
                mod.lambda_handler({}, _make_context())

    def test_raises_if_worker_not_set(self):
        s3 = _make_s3_mock()
        mod = _import_module(s3_mock=s3)
        mod.WORKER_FUNCTION_NAME = ""
        mod.CENTRAL_ANALYTICS_BUCKET = "bucket"
        with pytest.raises(ValueError, match="WORKER_FUNCTION_NAME"):
            mod.lambda_handler({}, _make_context())

    def test_returns_error_when_no_accounts(self):
        s3 = _make_s3_mock()
        s3.get_object.side_effect = NoSuchKey("none")
        mod = _import_module(s3_mock=s3)

        with patch.object(mod, "discover_accounts", return_value=[]):
            result = mod.lambda_handler({"trigger_source": "test"}, _make_context())
        assert result["status"] == "error"

    def test_complete_scan_in_single_invocation(self):
        s3 = _make_s3_mock()
        s3.get_object.side_effect = NoSuchKey("none")
        lam = MagicMock()
        lam.invoke.return_value = {"StatusCode": 200, "Payload": MagicMock()}
        mod = _import_module(s3_mock=s3, lambda_mock=lam)

        with patch.object(mod, "discover_accounts", return_value=["111"]):
            with patch.object(
                mod, "build_account_region_list", return_value=[("111", "us-east-1")]
            ):
                result = mod.lambda_handler({"trigger_source": "test"}, _make_context())

        assert result["status"] == "complete"
        assert result["completed"] == 1

    def test_fanout_triggered_when_scan_too_old(self):
        s3 = _make_s3_mock()
        # Simulate a checkpoint that started 5 days ago
        old_start = (datetime.utcnow() - timedelta(days=5)).isoformat()
        cp = {
            "scan_id": "2026-06-10",
            "started_at": old_start,
            "completed": [],
            "completed_count": 0,
            "total_count": 2,
            "errors": [],
            "last_updated": old_start,
        }
        lam = MagicMock()
        lam.invoke.return_value = {"StatusCode": 202}
        mod = _import_module(s3_mock=s3, lambda_mock=lam)
        cp["signature"] = mod._generate_checkpoint_signature(cp)
        s3.get_object.return_value = {
            "Body": MagicMock(read=lambda: json.dumps(cp).encode())
        }

        with patch.object(mod, "discover_accounts", return_value=["111", "222"]):
            with patch.object(
                mod,
                "build_account_region_list",
                return_value=[("111", "us-east-1"), ("222", "us-west-2")],
            ):
                result = mod.lambda_handler({"trigger_source": "test"}, _make_context())

        assert result["status"] == "fanout_dispatched"
