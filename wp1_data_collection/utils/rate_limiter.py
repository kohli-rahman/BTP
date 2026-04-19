import time
import functools
from wp1_data_collection.utils.logger import logger


def rate_limited(max_calls: int, period: float):
    """Decorator: allow at most max_calls per period (seconds)."""
    min_interval = period / max_calls
    last_called = [0.0]

    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            elapsed = time.monotonic() - last_called[0]
            wait = min_interval - elapsed
            if wait > 0:
                time.sleep(wait)
            last_called[0] = time.monotonic()
            return fn(*args, **kwargs)
        return wrapper
    return decorator


def retry_on_rate_limit(max_retries: int = 3, base_wait: float = 60.0):
    """Decorator: retry on HTTP 429 / RateLimitError with exponential back-off."""
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_retries + 1):
                try:
                    return fn(*args, **kwargs)
                except Exception as e:
                    if "rate limit" in str(e).lower() or "429" in str(e):
                        wait = base_wait * attempt
                        logger.warning(f"Rate limit hit on attempt {attempt}. Waiting {wait}s …")
                        time.sleep(wait)
                    else:
                        raise
            logger.error(f"{fn.__name__} failed after {max_retries} retries.")
            return None
        return wrapper
    return decorator
