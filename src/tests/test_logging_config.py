"""
Tests that logging configuration sets diagnose=False in Lambda environments
to prevent credential leakage via loguru's variable introspection.
"""

import os
import sys
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


class TestDiagnoseFlag:
    """Verify diagnose flag is False in Lambda, True in local dev."""

    def _call_setup_logging(self, env_vars):
        """Import and call setup_logging with a fresh mock logger."""
        mock_logger = MagicMock()
        mock_loguru = MagicMock(logger=mock_logger)

        with patch.dict(sys.modules, {"loguru": mock_loguru}):
            # Remove cached module so it re-imports with our mock
            for key in list(sys.modules.keys()):
                if "logging_config" in key:
                    del sys.modules[key]

            with patch.dict(os.environ, env_vars, clear=True):
                # Preserve PATH and HOME for basic functionality
                os.environ.setdefault("PATH", "/usr/bin")
                from utils.logging_config import setup_logging

                setup_logging(enable_file_logging=False)

        return mock_logger

    def test_lambda_env_sets_diagnose_false(self):
        """In Lambda, diagnose must be False to avoid leaking variables."""
        mock_logger = self._call_setup_logging({"AWS_LAMBDA_FUNCTION_NAME": "my-func"})

        add_calls = mock_logger.add.call_args_list
        assert len(add_calls) >= 1, "Expected at least one logger.add call"

        kwargs = add_calls[0].kwargs or dict(
            zip(
                ["sink", "format", "level", "colorize", "backtrace", "diagnose"],
                add_calls[0].args + (None,) * 6,
            )
        )
        if "diagnose" not in kwargs:
            # logger.add may use positional args — check all calls
            for call in add_calls:
                if call.kwargs and "diagnose" in call.kwargs:
                    kwargs = call.kwargs
                    break

        assert kwargs.get("diagnose") is False, (
            "Lambda environment must use diagnose=False to prevent "
            "credential leakage in exception tracebacks"
        )

    def test_local_env_sets_diagnose_true(self):
        """In local dev (no Lambda), diagnose should be True for debugging."""
        mock_logger = self._call_setup_logging({})

        add_calls = mock_logger.add.call_args_list
        assert len(add_calls) >= 1, "Expected at least one logger.add call"

        kwargs = add_calls[0].kwargs or {}
        if "diagnose" not in kwargs:
            for call in add_calls:
                if call.kwargs and "diagnose" in call.kwargs:
                    kwargs = call.kwargs
                    break

        assert kwargs.get("diagnose") is True, (
            "Local dev environment should use diagnose=True for "
            "full variable introspection during debugging"
        )

    def test_lambda_env_disables_colorize(self):
        """Lambda should also disable colors for clean CloudWatch logs."""
        mock_logger = self._call_setup_logging({"AWS_LAMBDA_FUNCTION_NAME": "my-func"})

        add_calls = mock_logger.add.call_args_list
        assert len(add_calls) >= 1
        kwargs = add_calls[0].kwargs or {}
        assert kwargs.get("colorize") is False
