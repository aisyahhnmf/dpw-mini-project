"""Adapter 'REST API' lokal: CSV yang dipaginasi dengan page & page_size.

Untuk mengganti dengan REST API sungguhan, cukup membuat kelas lain dengan method
get_orders(page, page_size, updated_since) yang memanggil requests.get(url, params=...)
dan mengubah HTTP 429/5xx/timeout menjadi TransientError (lihat README bagian 'REST API sungguhan').
"""
import csv
from datetime import datetime, timezone
from math import ceil
from pathlib import Path
from typing import Optional

from .retry import TransientError

TS_FORMAT = "%Y-%m-%dT%H:%M:%S"


def parse_ts(value: str) -> Optional[datetime]:
    """Parse ISO-8601 (mendukung akhiran 'Z') -> datetime naive UTC; None jika tidak valid."""
    try:
        dt = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except (ValueError, TypeError, AttributeError):
        return None
    return dt.astimezone(timezone.utc).replace(tzinfo=None) if dt.tzinfo else dt


class LocalCsvApi:
    def __init__(self, csv_path: Path, fail_first_n_calls: int = 0):
        """fail_first_n_calls: simulasi error sementara -> N panggilan pertama melempar TransientError."""
        with open(csv_path, newline="", encoding="utf-8") as f:
            self._rows = list(csv.DictReader(f))
        self._fail_left = fail_first_n_calls
        self.calls = 0

    def get_orders(self, page: int = 1, page_size: int = 100, updated_since: Optional[str] = None) -> dict:
        self.calls += 1
        if self._fail_left > 0:
            self._fail_left -= 1
            raise TransientError("HTTP 503 Service Unavailable (simulasi)")
        rows = self._rows
        if updated_since:
            since = parse_ts(updated_since)
            # server tidak bisa mengevaluasi filter pada order_ts rusak -> baris itu hanya muncul pada full load
            rows = [r for r in rows if (ts := parse_ts(r["order_ts"])) is not None and ts > since]
        total = len(rows)
        start = (page - 1) * page_size
        return {
            "page": page, "page_size": page_size, "total": total,
            "total_pages": ceil(total / page_size) if total else 0,
            "has_next": start + page_size < total,
            "data": rows[start:start + page_size],
        }
