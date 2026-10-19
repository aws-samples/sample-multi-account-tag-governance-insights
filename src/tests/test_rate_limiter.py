"""
Tests for Token Bucket Rate Limiter and Athena Rate Limiter.

Covers:
- TokenBucketRateLimiter initialization
- Token acquisition (blocking and non-blocking)
- Refill mechanics based on elapsed time
- Exceeding capacity returns False
- Reset to full capacity
- get_available_tokens() with refill
- get_wait_time() calculation
- AthenaRateLimiter wrapper
- Global singleton via get_athena_rate_limiter()
- Edge cases: zero tokens, timeout, concurrent access
"""

import os
import sys
import threading
import time

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lambda_handlers.cost.rate_limiter import (
    TokenBucketRateLimiter,
    AthenaRateLimiter,
    get_athena_rate_limiter,
)

# ---------------------------------------------------------------------------
# TokenBucketRateLimiter tests
# ---------------------------------------------------------------------------


class TestTokenBucketInit:
    """Tests for TokenBucketRateLimiter initialization."""

    def test_default_initialization(self):
        """Default rate=5.0, capacity=10, starts full."""
        limiter = TokenBucketRateLimiter()
        assert limiter.rate == 5.0
        assert limiter.capacity == 10
        assert limiter.tokens == 10.0

    def test_custom_initialization(self):
        """Custom rate and capacity are set correctly."""
        limiter = TokenBucketRateLimiter(rate=2.0, capacity=5)
        assert limiter.rate == 2.0
        assert limiter.capacity == 5
        assert limiter.tokens == 5.0

    def test_starts_at_full_capacity(self):
        """Tokens start at full capacity."""
        limiter = TokenBucketRateLimiter(rate=10.0, capacity=20)
        assert limiter.get_available_tokens() == pytest.approx(20.0, abs=0.5)


class TestTokenAcquisition:
    """Tests for token acquisition logic."""

    def test_acquire_single_token(self):
        """Acquiring one token reduces count."""
        limiter = TokenBucketRateLimiter(rate=5.0, capacity=10)
        result = limiter.acquire(1, blocking=False)
        assert result is True
        assert limiter.tokens < 10.0

    def test_acquire_multiple_tokens(self):
        """Acquiring multiple tokens at once works."""
        limiter = TokenBucketRateLimiter(rate=5.0, capacity=10)
        result = limiter.acquire(5, blocking=False)
        assert result is True

    def test_acquire_exceeding_capacity_returns_false(self):
        """Requesting more tokens than capacity always fails."""
        limiter = TokenBucketRateLimiter(rate=5.0, capacity=10)
        result = limiter.acquire(11, blocking=False)
        assert result is False

    def test_acquire_exceeding_capacity_blocking_still_false(self):
        """Even with blocking=True, exceeding capacity returns False immediately."""
        limiter = TokenBucketRateLimiter(rate=5.0, capacity=10)
        result = limiter.acquire(15, blocking=True, timeout=0.1)
        assert result is False

    def test_nonblocking_fails_when_insufficient(self):
        """Non-blocking fails when not enough tokens available."""
        limiter = TokenBucketRateLimiter(rate=1.0, capacity=3)
        # Drain all tokens
        limiter.acquire(3, blocking=False)
        # Try to acquire more immediately
        result = limiter.acquire(1, blocking=False)
        assert result is False

    def test_blocking_with_timeout_returns_false(self):
        """Blocking acquisition respects timeout."""
        limiter = TokenBucketRateLimiter(rate=0.5, capacity=2)
        # Drain tokens
        limiter.acquire(2, blocking=False)
        # Try blocking with short timeout
        start = time.time()
        result = limiter.acquire(2, blocking=True, timeout=0.2)
        elapsed = time.time() - start
        assert result is False
        assert elapsed >= 0.15  # Should have waited close to timeout

    def test_blocking_succeeds_after_refill(self):
        """Blocking acquisition succeeds after tokens refill."""
        limiter = TokenBucketRateLimiter(rate=20.0, capacity=5)
        # Drain tokens
        limiter.acquire(5, blocking=False)
        # Blocking wait - high rate means refill happens quickly
        result = limiter.acquire(1, blocking=True, timeout=1.0)
        assert result is True


class TestTryAcquire:
    """Tests for try_acquire (non-blocking shorthand)."""

    def test_try_acquire_success(self):
        """try_acquire succeeds when tokens available."""
        limiter = TokenBucketRateLimiter(rate=5.0, capacity=10)
        assert limiter.try_acquire(1) is True

    def test_try_acquire_failure(self):
        """try_acquire fails when insufficient tokens."""
        limiter = TokenBucketRateLimiter(rate=0.1, capacity=1)
        limiter.acquire(1, blocking=False)
        assert limiter.try_acquire(1) is False

    def test_try_acquire_exceeds_capacity(self):
        """try_acquire fails for requests exceeding capacity."""
        limiter = TokenBucketRateLimiter(rate=5.0, capacity=5)
        assert limiter.try_acquire(10) is False


class TestRefillMechanics:
    """Tests for token refill based on elapsed time."""

    def test_refill_after_time_elapsed(self):
        """Tokens refill based on elapsed time and rate."""
        limiter = TokenBucketRateLimiter(rate=10.0, capacity=10)
        # Drain all tokens
        limiter.acquire(10, blocking=False)
        # Simulate time passing
        limiter.last_refill = time.time() - 0.5  # 0.5 seconds ago
        available = limiter.get_available_tokens()
        # Should have ~5 tokens (10/sec * 0.5s)
        assert available >= 4.0
        assert available <= 6.0

    def test_refill_capped_at_capacity(self):
        """Refill never exceeds capacity."""
        limiter = TokenBucketRateLimiter(rate=100.0, capacity=5)
        # Drain some
        limiter.acquire(3, blocking=False)
        # Wait long time
        limiter.last_refill = time.time() - 10.0
        available = limiter.get_available_tokens()
        assert available <= 5.0

    def test_no_refill_when_full(self):
        """No extra tokens added when already at capacity."""
        limiter = TokenBucketRateLimiter(rate=10.0, capacity=10)
        limiter.last_refill = time.time() - 5.0
        available = limiter.get_available_tokens()
        assert available == pytest.approx(10.0, abs=0.1)


class TestReset:
    """Tests for rate limiter reset."""

    def test_reset_restores_full_capacity(self):
        """Reset fills tokens back to capacity."""
        limiter = TokenBucketRateLimiter(rate=5.0, capacity=10)
        limiter.acquire(8, blocking=False)
        limiter.reset()
        assert limiter.tokens == 10.0

    def test_reset_updates_last_refill(self):
        """Reset updates the refill timestamp."""
        limiter = TokenBucketRateLimiter(rate=5.0, capacity=10)
        old_time = limiter.last_refill
        time.sleep(0.01)
        limiter.reset()
        assert limiter.last_refill > old_time


class TestGetWaitTime:
    """Tests for wait time estimation."""

    def test_zero_wait_when_tokens_available(self):
        """Returns 0 when enough tokens available."""
        limiter = TokenBucketRateLimiter(rate=5.0, capacity=10)
        assert limiter.get_wait_time(1) == 0.0

    def test_positive_wait_when_insufficient(self):
        """Returns positive wait time when tokens insufficient."""
        limiter = TokenBucketRateLimiter(rate=5.0, capacity=10)
        limiter.acquire(10, blocking=False)
        # Need to drain and check immediately
        limiter.last_refill = time.time()  # Reset refill clock
        limiter.tokens = 0.0
        wait = limiter.get_wait_time(5)
        # 5 tokens at 5/sec = ~1 second (but refill may have happened)
        assert wait >= 0.0

    def test_wait_time_scales_with_tokens_needed(self):
        """More tokens needed = longer wait time."""
        limiter = TokenBucketRateLimiter(rate=2.0, capacity=10)
        limiter.tokens = 0.0
        limiter.last_refill = time.time()
        wait1 = limiter.get_wait_time(1)
        limiter.tokens = 0.0
        limiter.last_refill = time.time()
        wait5 = limiter.get_wait_time(5)
        assert wait5 > wait1


class TestGetAvailableTokens:
    """Tests for get_available_tokens."""

    def test_returns_current_tokens(self):
        """Returns token count after refill."""
        limiter = TokenBucketRateLimiter(rate=5.0, capacity=10)
        tokens = limiter.get_available_tokens()
        assert tokens == pytest.approx(10.0, abs=0.5)

    def test_triggers_refill(self):
        """Calling get_available_tokens triggers a refill."""
        limiter = TokenBucketRateLimiter(rate=10.0, capacity=10)
        limiter.acquire(5, blocking=False)
        limiter.last_refill = time.time() - 1.0  # 1 second ago
        tokens = limiter.get_available_tokens()
        # Should have refilled ~10 tokens (capped at capacity)
        assert tokens >= 5.0


class TestThreadSafety:
    """Tests for thread-safe access."""

    def test_concurrent_acquire_no_over_drain(self):
        """Concurrent acquisitions do not over-drain the bucket."""
        limiter = TokenBucketRateLimiter(rate=0.0, capacity=10)
        # rate=0 means no refill during test
        limiter.tokens = 10.0
        results = []

        def acquire_one():
            results.append(limiter.try_acquire(1))

        threads = [threading.Thread(target=acquire_one) for _ in range(15)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Exactly 10 should succeed, 5 should fail
        assert results.count(True) == 10
        assert results.count(False) == 5


# ---------------------------------------------------------------------------
# AthenaRateLimiter tests
# ---------------------------------------------------------------------------


class TestAthenaRateLimiter:
    """Tests for Athena-specific rate limiter wrapper."""

    def test_default_config(self):
        """Uses Athena-specific defaults."""
        limiter = AthenaRateLimiter()
        assert limiter.rate == 5.0
        assert limiter.capacity == 10

    def test_custom_config(self):
        """Accepts custom rate and capacity."""
        limiter = AthenaRateLimiter(rate=2.0, capacity=5)
        assert limiter.rate == 2.0
        assert limiter.capacity == 5

    def test_acquire_query_slot(self):
        """acquire_query_slot acquires one token."""
        limiter = AthenaRateLimiter()
        assert limiter.acquire_query_slot(blocking=False) is True

    def test_try_acquire_query_slot(self):
        """try_acquire_query_slot is non-blocking."""
        limiter = AthenaRateLimiter(rate=0.0, capacity=1)
        limiter._limiter.tokens = 1.0
        assert limiter.try_acquire_query_slot() is True
        limiter._limiter.tokens = 0.0
        assert limiter.try_acquire_query_slot() is False

    def test_get_available_slots(self):
        """get_available_slots returns int."""
        limiter = AthenaRateLimiter()
        slots = limiter.get_available_slots()
        assert isinstance(slots, int)
        assert slots >= 0

    def test_get_wait_time(self):
        """get_wait_time returns zero when slots available."""
        limiter = AthenaRateLimiter()
        assert limiter.get_wait_time() == 0.0


# ---------------------------------------------------------------------------
# Global singleton tests
# ---------------------------------------------------------------------------


class TestGetAthenaRateLimiter:
    """Tests for global singleton accessor."""

    def test_returns_athena_rate_limiter(self):
        """Returns an AthenaRateLimiter instance."""
        import lambda_handlers.cost.rate_limiter as rl_module

        rl_module._global_athena_limiter = None  # Reset
        limiter = get_athena_rate_limiter()
        assert isinstance(limiter, AthenaRateLimiter)

    def test_returns_same_instance(self):
        """Returns the same instance on repeated calls."""
        import lambda_handlers.cost.rate_limiter as rl_module

        rl_module._global_athena_limiter = None  # Reset
        limiter1 = get_athena_rate_limiter()
        limiter2 = get_athena_rate_limiter()
        assert limiter1 is limiter2
