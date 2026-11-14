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

CHECKPOINT_ARN = "arn:aws:secretsmanager:us-east-1:123456789012:secret:ti-checkpoint-AbCdEf"
ENV_VARS = {
    "ENVIRONMENT": "test",
    "HUB_REGION": "us-east-1",
    "CENTRAL_ANALYTICS_BUCKET": "amzn-s3-demo-analytics-bucket",
    "WORKER_FUNCTION_NAME": "test-worker-function",
    "MAX_SCAN_DAYS": "3",
    "CHECKPOINT_SECRET_ARN": CHECKPOINT_ARN,
    "LOG_LEVEL": "WARNING",
}
TEST_HMAC_KEY = "deadbeef" * 8


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
            # Pre-seed the per-container cache so tests make no Secrets Manager call
            mod._cached_signing_secret = TEST_HMAC_KEY
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


class TestSigningSecretFromSecretsManager:
    """The HMAC key comes from Secrets Manager, never a Lambda env var."""

    ARN = ENV_VARS["CHECKPOINT_SECRET_ARN"]

    def _stubbed(self, mod):
        import boto3 as real_boto3
        from botocore.stub import Stubber

        mod._cached_signing_secret = None
        with patch.dict(os.environ):
            # A profile named in the shell would not exist under AWS_CONFIG_FILE=/dev/null.
            os.environ.pop("AWS_PROFILE", None)
            os.environ.pop("AWS_DEFAULT_PROFILE", None)
            client = real_boto3.session.Session().client("secretsmanager", region_name="us-east-1")
        return client, Stubber(client)

    def test_reads_secret_once_per_container(self):
        mod = _import_module()
        client, stub = self._stubbed(mod)
        stub.add_response(
            "get_secret_value",
            {"ARN": self.ARN, "Name": "ti-checkpoint", "SecretString": TEST_HMAC_KEY},
            {"SecretId": self.ARN},
        )
        with stub, patch("boto3.client", return_value=client) as factory:
            assert mod._checkpoint_signature_secret() == TEST_HMAC_KEY
            sig = mod._generate_checkpoint_signature({"scan_id": "2026-01-01"})
            assert mod._validate_checkpoint_signature({"scan_id": "2026-01-01", "signature": sig})
            stub.assert_no_pending_responses()
        assert factory.call_args_list == [(("secretsmanager",), {})]

    def test_env_var_path_removed_and_missing_arn_raises(self):
        mod = _import_module()
        mod._cached_signing_secret = None
        env = {k: v for k, v in ENV_VARS.items() if k != "CHECKPOINT_SECRET_ARN"}
        env["CHECKPOINT_SIGNING_SECRET"] = TEST_HMAC_KEY
        with patch.dict(os.environ, env, clear=True), patch("boto3.client") as factory:
            with pytest.raises(RuntimeError, match="CHECKPOINT_SECRET_ARN"):
                mod._checkpoint_signature_secret()
        factory.assert_not_called()

    def test_read_failure_stops_handler_before_scanning(self):
        mod = _import_module()
        client, stub = self._stubbed(mod)
        stub.add_client_error("get_secret_value", service_error_code="AccessDeniedException")
        with stub, patch("boto3.client", return_value=client):
            with pytest.raises(RuntimeError, match="AccessDeniedException"):
                mod.lambda_handler({}, _make_context())
        assert mod._cached_signing_secret is None
        mod.s3.get_object.assert_not_called()
        mod.lambda_client.invoke.assert_not_called()


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


    def test_other_read_errors_propagate(self):
        # Returning None here would start a new cycle and overwrite progress.
        s3 = _make_s3_mock()
        s3.get_object.side_effect = RuntimeError("SlowDown")
        mod = _import_module(s3_mock=s3)
        with pytest.raises(RuntimeError, match="SlowDown"):
            mod.read_checkpoint()


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
        assert call_kwargs["Bucket"] == "amzn-s3-demo-analytics-bucket"
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

    def test_discover_accounts_fails_loudly_without_org_access(self):
        mod = _import_module()

        from botocore.exceptions import ClientError

        error_resp = {"Error": {"Code": "AccessDeniedException", "Message": "denied"}}
        orgs = MagicMock()
        orgs.get_paginator.side_effect = ClientError(error_resp, "ListAccounts")

        with patch("boto3.client", return_value=orgs):
            with pytest.raises(RuntimeError, match="delegated administrator"):
                mod.discover_accounts()

    def test_region_list_paginates_list_indexes(self):
        mod = _import_module()
        re_client = MagicMock()
        re_client.get_paginator.return_value.paginate.return_value = [
            {"Indexes": [{"Arn": "arn:aws:resource-explorer-2:us-east-1:111122223333:index/a"}]},
            {"Indexes": [{"Arn": "arn:aws:resource-explorer-2:eu-west-1:111122223333:index/b"}]},
        ]
        with patch("boto3.client", return_value=re_client):
            pairs = mod.build_account_region_list(["111122223333"])
        assert pairs == [("111122223333", "us-east-1"), ("111122223333", "eu-west-1"),
                         ("111122223333", "global")]

    def test_region_list_includes_global(self):
        # Global resources (IAM, CloudFront, Route 53) only match region:global:
        # https://docs.aws.amazon.com/resource-explorer/latest/userguide/using-search-query-syntax.html
        mod = _import_module()
        re_client = MagicMock()
        re_client.get_paginator.return_value.paginate.return_value = [
            {"Indexes": [{"Arn": "arn:aws:resource-explorer-2:us-east-1:111122223333:index/a"}]},
        ]
        with patch("boto3.client", return_value=re_client):
            pairs = mod.build_account_region_list(["111122223333"])
        assert ("111122223333", "global") in pairs
        assert pairs.count(("111122223333", "global")) == 1

    def test_region_list_error_is_not_hidden(self):
        """G4: a ListIndexes failure must not shrink the cycle to the hub Region."""
        from botocore.exceptions import ClientError
        mod = _import_module()
        re_client = MagicMock()
        re_client.get_paginator.return_value.paginate.side_effect = ClientError(
            {"Error": {"Code": "ThrottlingException", "Message": "slow down"}}, "ListIndexes")
        with patch("boto3.client", return_value=re_client):
            with pytest.raises(ClientError):
                mod.build_account_region_list(["123456789012"])


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


    def test_checkpoint_written_only_after_successful_pairs(self):
        s3 = _make_s3_mock()
        lam = MagicMock()
        lam.invoke.side_effect = Exception("boom")
        mod = _import_module(s3_mock=s3, lambda_mock=lam)
        pairs = [(f"{i:012d}", "us-east-1") for i in range(150)]
        checkpoint = {"completed": [], "completed_count": 0, "total_count": 150,
                      "errors": [], "last_updated": ""}

        result = mod.sequential_scan(pairs, checkpoint, "scan-1", _make_context())

        # Every pair failed: only the final save happens, and errors are capped.
        assert s3.put_object.call_count == 1
        assert len(result["errors"]) == mod.MAX_CHECKPOINT_ERRORS
        assert result["errors"][-1]["pair"] == "000000000149/us-east-1"

    def test_successful_retry_clears_earlier_errors(self):
        s3 = _make_s3_mock()
        lam = MagicMock()
        lam.invoke.return_value = {"StatusCode": 200, "Payload": MagicMock()}
        mod = _import_module(s3_mock=s3, lambda_mock=lam)
        pairs = [("111", "us-east-1")]
        checkpoint = {"completed": [], "completed_count": 0, "total_count": 1,
                      "errors": [{"pair": "111/us-east-1", "timestamp": "t"},
                                 {"pair": "222/us-west-2", "timestamp": "t"}],
                      "last_updated": ""}

        result = mod.sequential_scan(pairs, checkpoint, "scan-1", _make_context())

        assert result["errors"] == [{"pair": "222/us-west-2", "timestamp": "t"}]


class TestLambdaClientConfig:
    def test_sync_invoke_does_not_time_out_or_retry_before_worker(self):
        with patch.dict(os.environ, ENV_VARS), patch("boto3.client") as client:
            import lambda_handlers.aggregator_coordinator  # noqa: F401
        lambda_calls = [c for c in client.call_args_list if c.args[0] == "lambda"]
        assert len(lambda_calls) == 1
        kwargs = lambda_calls[0].kwargs
        assert "region_name" not in kwargs
        config = kwargs["config"]
        assert config.read_timeout == 900
        assert config.retries["max_attempts"] == 0


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
        # Cycle is closed: completion marker written and checkpoint removed,
        # so the next run starts a new cycle instead of fanning out again.
        marker = [c.kwargs for c in s3.put_object.call_args_list
                  if c.kwargs["Key"].startswith(mod.COMPLETION_KEY_PREFIX)]
        assert len(marker) == 1
        assert json.loads(marker[0]["Body"])["mode"] == "fanout"
        assert all(c.kwargs["Key"] != mod.CHECKPOINT_KEY for c in s3.put_object.call_args_list)
        s3.delete_object.assert_called_once_with(
            Bucket="amzn-s3-demo-analytics-bucket", Key=mod.CHECKPOINT_KEY
        )

    def test_run_after_fanout_starts_fresh_cycle_instead_of_fanning_out_again(self):
        """B5: the invocation AFTER a fan-out must not re-fan the same pairs."""
        store = {}
        s3 = _make_s3_mock()

        def get_object(Bucket, Key):
            if Key not in store:
                raise NoSuchKey()
            body = store[Key]
            return {"Body": MagicMock(read=lambda: body)}

        def put_object(Bucket, Key, Body, **kwargs):
            store[Key] = Body.encode() if isinstance(Body, str) else Body

        s3.get_object.side_effect = get_object
        s3.put_object.side_effect = put_object
        s3.delete_object.side_effect = lambda Bucket, Key: store.pop(Key, None)

        lam = MagicMock()
        lam.invoke.side_effect = lambda **kw: {
            "StatusCode": 202 if kw["InvocationType"] == "Event" else 200
        }
        mod = _import_module(s3_mock=s3, lambda_mock=lam)

        old_start = (datetime.utcnow() - timedelta(days=5)).isoformat()
        cp = {
            "scan_id": "2026-06-10", "started_at": old_start, "completed": [],
            "completed_count": 0, "total_count": 2, "errors": [],
            "last_updated": old_start,
        }
        cp["signature"] = mod._generate_checkpoint_signature(cp)
        store[mod.CHECKPOINT_KEY] = json.dumps(cp).encode()

        pairs = [("111", "us-east-1"), ("222", "us-west-2")]
        with patch.object(mod, "discover_accounts", return_value=["111", "222"]), \
                patch.object(mod, "build_account_region_list", return_value=pairs):
            first = mod.lambda_handler({"trigger_source": "test"}, _make_context())
            async_after_first = [c for c in lam.invoke.call_args_list
                                 if c.kwargs["InvocationType"] == "Event"]
            second = mod.lambda_handler({"trigger_source": "test"}, _make_context())

        assert first["status"] == "fanout_dispatched"
        assert len(async_after_first) == 2
        # Second run: fresh cycle, sequential scan, no second fan-out.
        assert second["status"] == "complete"
        assert second["scan_id"] != "2026-06-10"
        async_total = [c for c in lam.invoke.call_args_list
                       if c.kwargs["InvocationType"] == "Event"]
        assert len(async_total) == 2


class TestFanoutTimeout:
    """G3: a fan-out that runs out of time must checkpoint, not redo itself daily."""

    def test_fanout_stops_before_timeout_and_resumes(self):
        store = {}
        s3 = _make_s3_mock()

        def get_object(Bucket, Key):
            if Key not in store:
                raise NoSuchKey()
            body = store[Key]
            return {"Body": MagicMock(read=lambda: body)}

        def put_object(Bucket, Key, Body, **kwargs):
            store[Key] = Body.encode() if isinstance(Body, str) else Body

        s3.get_object.side_effect = get_object
        s3.put_object.side_effect = put_object
        s3.delete_object.side_effect = lambda Bucket, Key: store.pop(Key, None)
        lam = MagicMock()
        lam.invoke.return_value = {"StatusCode": 202}
        mod = _import_module(s3_mock=s3, lambda_mock=lam)

        old_start = (datetime.utcnow() - timedelta(days=5)).isoformat()
        cp = {"scan_id": "2026-06-10", "started_at": old_start, "completed": [],
              "completed_count": 0, "total_count": 3, "errors": [], "last_updated": old_start}
        cp["signature"] = mod._generate_checkpoint_signature(cp)
        store[mod.CHECKPOINT_KEY] = json.dumps(cp).encode()

        pairs = [("111", "us-east-1"), ("222", "us-east-1"), ("333", "us-east-1")]
        # First run: time runs out after two dispatches.
        first_ctx = MagicMock()
        first_ctx.get_remaining_time_in_millis.side_effect = [900000, 900000] + [1000] * 10
        with patch.object(mod, "discover_accounts", return_value=["111", "222", "333"]), \
                patch.object(mod, "build_account_region_list", return_value=pairs):
            first = mod.lambda_handler({"trigger_source": "test"}, first_ctx)
            dispatched_first = [json.loads(c.kwargs["Payload"])["account_id"]
                                for c in lam.invoke.call_args_list]
            second = mod.lambda_handler({"trigger_source": "test"}, _make_context())

        assert dispatched_first == ["111", "222"]
        assert first["status"] == "fanout_partial"
        dispatched_all = [json.loads(c.kwargs["Payload"])["account_id"] for c in lam.invoke.call_args_list]
        assert dispatched_all == ["111", "222", "333"]  # second run sends only the rest
        assert second["status"] == "fanout_dispatched"
        assert mod.CHECKPOINT_KEY not in store

    def test_fanout_partial_does_not_mark_failed_invoke_completed(self):
        """L2: a pair whose async invoke was refused must be retried, not marked completed."""
        store = {}
        s3 = _make_s3_mock()

        def get_object(Bucket, Key):
            if Key not in store:
                raise NoSuchKey()
            body = store[Key]
            return {"Body": MagicMock(read=lambda: body)}

        def put_object(Bucket, Key, Body, **kwargs):
            store[Key] = Body.encode() if isinstance(Body, str) else Body

        s3.get_object.side_effect = get_object
        s3.put_object.side_effect = put_object
        s3.delete_object.side_effect = lambda Bucket, Key: store.pop(Key, None)
        lam = MagicMock()
        # First run: 111 is refused (throttled), 222 accepted. Second run: all accepted.
        lam.invoke.side_effect = [Exception("TooManyRequestsException"), {"StatusCode": 202},
                                  {"StatusCode": 202}, {"StatusCode": 202}]
        mod = _import_module(s3_mock=s3, lambda_mock=lam)

        old_start = (datetime.utcnow() - timedelta(days=5)).isoformat()
        cp = {"scan_id": "2026-06-10", "started_at": old_start, "completed": [],
              "completed_count": 0, "total_count": 3, "errors": [], "last_updated": old_start}
        cp["signature"] = mod._generate_checkpoint_signature(cp)
        store[mod.CHECKPOINT_KEY] = json.dumps(cp).encode()

        pairs = [("111", "us-east-1"), ("222", "us-east-1"), ("333", "us-east-1")]
        first_ctx = MagicMock()
        first_ctx.get_remaining_time_in_millis.side_effect = [900000, 900000] + [1000] * 10
        with patch.object(mod, "discover_accounts", return_value=["111", "222", "333"]), \
                patch.object(mod, "build_account_region_list", return_value=pairs):
            first = mod.lambda_handler({"trigger_source": "test"}, first_ctx)
            saved = json.loads(store[mod.CHECKPOINT_KEY])
            second = mod.lambda_handler({"trigger_source": "test"}, _make_context())

        assert first["status"] == "fanout_partial"
        assert saved["completed"] == ["222/us-east-1"]
        assert [e["pair"] for e in saved["errors"]] == ["111/us-east-1"]
        dispatched = [json.loads(c.kwargs["Payload"])["account_id"] for c in lam.invoke.call_args_list]
        assert dispatched == ["111", "222", "111", "333"]  # the refused pair is sent again
        assert second["status"] == "fanout_dispatched"
        completion = [k for k in store if k.startswith(mod.COMPLETION_KEY_PREFIX)]
        assert len(completion) == 1
        assert mod.CHECKPOINT_KEY not in store
