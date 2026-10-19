#!/usr/bin/env python3
"""
Token Bucket Rate Limiter

Implements token bucket algorithm for rate limiting Athena queries to prevent throttling.

Features:
- Token bucket algorithm with configurable rate and burst capacity
- Thread-safe for concurrent Lambda invocations
- Auto-refill based on time elapsed
- Blocking and non-blocking acquisition modes
"""

import time
import threading
from typing import Optional

# Graceful fallback for Lambda environments without loguru
try:
    from loguru import logger
except ImportError:
    import logging
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


class TokenBucketRateLimiter:
    """
    Token bucket rate limiter for controlling API request rates
    
    Allows bursts up to bucket capacity while maintaining average rate.
    """
    
    def __init__(self, rate: float = 5.0, capacity: int = 10):
        """
        Initialize rate limiter
        
        Args:
            rate: Token refill rate per second (e.g., 5.0 = 5 queries per second)
            capacity: Maximum burst capacity (max tokens in bucket)
        """
        self.rate = rate  # tokens per second
        self.capacity = capacity  # maximum tokens
        self.tokens = float(capacity)  # current tokens (start full)
        self.last_refill = time.time()
        self.lock = threading.Lock()
        
        logger.debug(f"TokenBucketRateLimiter initialized: rate={rate} tokens/sec, capacity={capacity}")
    
    def acquire(self, tokens: int = 1, blocking: bool = True, timeout: Optional[float] = None) -> bool:
        """
        Acquire tokens from the bucket
        
        Args:
            tokens: Number of tokens to acquire (default: 1)
            blocking: If True, wait until tokens are available
            timeout: Maximum time to wait in seconds (None = wait forever)
            
        Returns:
            bool: True if tokens were acquired, False otherwise
        """
        if tokens > self.capacity:
            logger.warning(f"Requested {tokens} tokens exceeds capacity {self.capacity}")
            return False
        
        start_time = time.time()
        
        while True:
            with self.lock:
                # Refill tokens based on time elapsed
                self._refill()
                
                # Check if enough tokens available
                if self.tokens >= tokens:
                    self.tokens -= tokens
                    logger.debug(f"Acquired {tokens} tokens, {self.tokens:.2f} remaining")
                    return True
            
            # If non-blocking, return immediately
            if not blocking:
                logger.debug(f"Failed to acquire {tokens} tokens (non-blocking)")
                return False
            
            # Check timeout
            if timeout is not None:
                elapsed = time.time() - start_time
                if elapsed >= timeout:
                    logger.debug(f"Failed to acquire {tokens} tokens (timeout after {elapsed:.2f}s)")
                    return False
            
            # Wait a bit before retrying (adaptive sleep based on tokens needed)
            sleep_time = min((tokens - self.tokens) / self.rate, 0.1)
            time.sleep(sleep_time)
    
    def _refill(self):
        """
        Refill tokens based on time elapsed since last refill
        
        Must be called with lock held.
        """
        now = time.time()
        elapsed = now - self.last_refill
        
        # Calculate tokens to add based on elapsed time
        tokens_to_add = elapsed * self.rate
        
        if tokens_to_add > 0:
            self.tokens = min(self.capacity, self.tokens + tokens_to_add)
            self.last_refill = now
            logger.debug(f"Refilled {tokens_to_add:.2f} tokens, now at {self.tokens:.2f}")
    
    def try_acquire(self, tokens: int = 1) -> bool:
        """
        Non-blocking token acquisition
        
        Args:
            tokens: Number of tokens to acquire
            
        Returns:
            bool: True if acquired, False otherwise
        """
        return self.acquire(tokens, blocking=False)
    
    def get_available_tokens(self) -> float:
        """
        Get current number of available tokens
        
        Returns:
            float: Number of tokens currently available
        """
        with self.lock:
            self._refill()
            return self.tokens
    
    def wait_for_tokens(self, tokens: int = 1, timeout: Optional[float] = None) -> bool:
        """
        Wait until specified number of tokens are available
        
        Args:
            tokens: Number of tokens to wait for
            timeout: Maximum wait time in seconds
            
        Returns:
            bool: True if tokens became available, False on timeout
        """
        return self.acquire(tokens, blocking=True, timeout=timeout)
    
    def reset(self):
        """Reset the rate limiter to full capacity"""
        with self.lock:
            self.tokens = float(self.capacity)
            self.last_refill = time.time()
            logger.debug("Rate limiter reset to full capacity")
    
    def get_wait_time(self, tokens: int = 1) -> float:
        """
        Calculate estimated wait time for tokens to become available
        
        Args:
            tokens: Number of tokens needed
            
        Returns:
            float: Estimated wait time in seconds
        """
        with self.lock:
            self._refill()
            
            if self.tokens >= tokens:
                return 0.0
            
            # Calculate time needed to accumulate required tokens
            tokens_needed = tokens - self.tokens
            wait_time = tokens_needed / self.rate
            
            return wait_time


class AthenaRateLimiter:
    """
    Specialized rate limiter for Athena queries
    
    Pre-configured with Athena-specific limits and burst handling.
    """
    
    # Athena limits: 25 concurrent queries per account
    # We use 5 QPS per worker to stay well under limit with 50 workers
    DEFAULT_RATE = 5.0  # queries per second per worker
    DEFAULT_CAPACITY = 10  # allow bursts of up to 10 queries
    
    def __init__(self, rate: Optional[float] = None, capacity: Optional[int] = None):
        """
        Initialize Athena-specific rate limiter
        
        Args:
            rate: Query rate per second (defaults to 5 QPS)
            capacity: Burst capacity (defaults to 10)
        """
        self.rate = rate or self.DEFAULT_RATE
        self.capacity = capacity or self.DEFAULT_CAPACITY
        
        self._limiter = TokenBucketRateLimiter(
            rate=self.rate,
            capacity=self.capacity
        )
        
        logger.info(f"AthenaRateLimiter initialized: {self.rate} QPS, burst={self.capacity}")
    
    def acquire_query_slot(self, blocking: bool = True, timeout: Optional[float] = None) -> bool:
        """
        Acquire a slot to execute an Athena query
        
        Args:
            blocking: If True, wait until slot is available
            timeout: Maximum wait time in seconds
            
        Returns:
            bool: True if slot acquired, False otherwise
        """
        return self._limiter.acquire(tokens=1, blocking=blocking, timeout=timeout)
    
    def try_acquire_query_slot(self) -> bool:
        """
        Try to acquire a query slot without blocking
        
        Returns:
            bool: True if slot acquired, False otherwise
        """
        return self._limiter.try_acquire(tokens=1)
    
    def get_available_slots(self) -> int:
        """
        Get number of immediately available query slots
        
        Returns:
            int: Number of available slots
        """
        return int(self._limiter.get_available_tokens())
    
    def get_wait_time(self) -> float:
        """
        Get estimated wait time for next query slot
        
        Returns:
            float: Wait time in seconds
        """
        return self._limiter.get_wait_time(tokens=1)


# Global instance for Lambda reuse across invocations
_global_athena_limiter = None


def get_athena_rate_limiter() -> AthenaRateLimiter:
    """
    Get or create global Athena rate limiter instance
    
    Returns:
        AthenaRateLimiter: Shared rate limiter instance
    """
    global _global_athena_limiter
    
    if _global_athena_limiter is None:
        _global_athena_limiter = AthenaRateLimiter()
        logger.debug("Created global Athena rate limiter")
    
    return _global_athena_limiter


if __name__ == "__main__":
    # Simple test/demo
    import time
    
    print("🧪 Testing Token Bucket Rate Limiter...")
    
    # Test basic rate limiter
    limiter = TokenBucketRateLimiter(rate=5.0, capacity=10)
    
    print(f"Initial tokens: {limiter.get_available_tokens():.2f}")
    
    # Acquire some tokens
    print("\nAcquiring 3 tokens...")
    success = limiter.acquire(3)
    print(f"Success: {success}, Remaining: {limiter.get_available_tokens():.2f}")
    
    # Try to acquire more than capacity
    print("\nTrying to acquire 15 tokens (exceeds capacity)...")
    success = limiter.try_acquire(15)
    print(f"Success: {success} (should be False)")
    
    # Wait for refill
    print("\nWaiting 1 second for refill...")
    time.sleep(1.0)
    print(f"Tokens after refill: {limiter.get_available_tokens():.2f}")
    
    # Test Athena-specific limiter
    print("\n🧪 Testing Athena Rate Limiter...")
    athena_limiter = get_athena_rate_limiter()
    
    print(f"Available query slots: {athena_limiter.get_available_slots()}")
    
    # Simulate burst of queries
    print("\nSimulating burst of 5 queries...")
    for i in range(5):
        success = athena_limiter.try_acquire_query_slot()
        print(f"  Query {i+1}: {success}, remaining slots: {athena_limiter.get_available_slots()}")
    
    print("\n✅ Rate limiter tests completed")
