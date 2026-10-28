"""Retry dengan exponential backoff + jitter untuk error sementara."""
import random, time
from typing import Callable, TypeVar

T = TypeVar("T")


class TransientError(Exception):
    """Error sementara (mis. HTTP 500/503/429, timeout) -> boleh di-retry."""


class PermanentError(Exception):
    """Error permanen (mis. 400/404) -> TIDAK boleh di-retry."""


def backoff_delay(attempt: int, base: float, max_delay: float, jitter: float = 0.1) -> float:
    """delay = min(max_delay, base * 2**attempt) + jitter acak. attempt mulai dari 0."""
    delay = min(max_delay, base * (2 ** attempt))
    return delay + random.uniform(0, jitter * delay)


def retry_with_backoff(fn: Callable[[], T], *, max_retries: int, base: float, max_delay: float,
                       jitter: float = 0.1, sleep: Callable[[float], None] = time.sleep,
                       logger=None, label: str = "call") -> T:
    """Jalankan fn(); bila TransientError, ulangi sampai max_retries kali.
    Total percobaan = 1 + max_retries. PermanentError langsung dilempar ulang."""
    for attempt in range(max_retries + 1):
        try:
            return fn()
        except TransientError as exc:
            if attempt == max_retries:
                if logger:
                    logger.error("%s gagal permanen setelah %d percobaan: %s", label, attempt + 1, exc)
                raise
            delay = backoff_delay(attempt, base, max_delay, jitter)
            if logger:
                logger.warning("%s gagal (percobaan %d/%d): %s -> retry dalam %.2fs",
                               label, attempt + 1, max_retries + 1, exc, delay)
            sleep(delay)
    raise RuntimeError("tidak tercapai")
