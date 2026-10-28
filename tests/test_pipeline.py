"""Unit test (pytest). Jalankan: pytest -q   (butuh pydantic & polars terpasang)"""
import io, json
import pytest
from src.retry import TransientError, backoff_delay, retry_with_backoff
from src.api_adapter import LocalCsvApi
from src import config as C


def test_backoff_delay_doubles_and_caps():
    assert [backoff_delay(i, 0.5, 8.0, jitter=0) for i in range(6)] == [0.5, 1.0, 2.0, 4.0, 8.0, 8.0]


def test_retry_recovers_after_transient_errors():
    api = LocalCsvApi(C.SOURCE_CSV, fail_first_n_calls=2)
    sleeps = []
    out = retry_with_backoff(lambda: api.get_orders(1, 10), max_retries=5, base=0.5, max_delay=8, jitter=0, sleep=sleeps.append)
    assert len(out["data"]) == 10 and sleeps == [0.5, 1.0]


def test_retry_gives_up():
    def always():
        raise TransientError("x")
    with pytest.raises(TransientError):
        retry_with_backoff(always, max_retries=2, base=0.01, max_delay=0.02, sleep=lambda s: None)


def test_pagination_covers_all_rows_once():
    api = LocalCsvApi(C.SOURCE_CSV)
    page, ids = 1, []
    while True:
        r = api.get_orders(page, 100)
        ids += [x["order_id"] for x in r["data"]]
        if not r["has_next"]:
            break
        page += 1
    assert len(ids) == r["total"]


def test_watermark_filter_excludes_old_rows():
    api = LocalCsvApi(C.SOURCE_CSV)
    assert api.get_orders(1, 100, updated_since="2999-01-01T00:00:00")["total"] == 0


GOOD = {"order_id": "A1", "customer_id": "C1", "order_ts": "2026-08-01T10:00:00", "product_category": "home",
        "quantity": "2", "unit_price": "100000", "discount_pct": "0.1", "payment_method": "cod", "status": "paid"}


@pytest.mark.parametrize("patch,field", [
    ({"order_id": ""}, "order_id"), ({"customer_id": ""}, "customer_id"), ({"quantity": "0"}, "quantity"),
    ({"quantity": "-1"}, "quantity"), ({"quantity": "2.5"}, "quantity"), ({"unit_price": "-1"}, "unit_price"),
    ({"discount_pct": "1.5"}, "discount_pct"), ({"discount_pct": "-0.1"}, "discount_pct"),
    ({"order_ts": "bukan tanggal"}, "order_ts"), ({"status": "pending"}, "status"),
])
def test_contract_rejects(patch, field):
    pytest.importorskip("pydantic")
    from src.contract import validate_record
    ok, err = validate_record({**GOOD, **patch})
    assert ok is None and field in err["fields"]


def test_contract_accepts_valid_and_boundaries():
    pytest.importorskip("pydantic")
    from src.contract import validate_record
    for patch in ({}, {"unit_price": "0"}, {"discount_pct": "0"}, {"discount_pct": "1"}, {"status": "refunded"}):
        ok, err = validate_record({**GOOD, **patch})
        assert err is None and ok["order_ts"] == "2026-08-01T10:00:00"


def test_fail_fast_raises():
    pytest.importorskip("pydantic")
    from src.contract import ContractViolation, validate_batch
    with pytest.raises(ContractViolation):
        validate_batch([GOOD, {**GOOD, "quantity": "0"}], fail_fast=True)


def test_consumer_sink_is_idempotent():
    pytest.importorskip("pydantic")
    from src.kafka_consumer import process_message
    vf, rf, seen, seen_rej = io.StringIO(), io.StringIO(), set(), set()
    c = {"read": 0, "valid": 0, "rejected": 0, "duplicate_skipped": 0}
    msg = json.dumps(GOOD).encode()
    process_message(msg, seen, vf, rf, c, {}, seen_rej)
    process_message(msg, seen, vf, rf, c, {}, seen_rej)          # event dikirim 2x
    assert c == {"read": 2, "valid": 1, "rejected": 0, "duplicate_skipped": 1}
    bad = json.dumps({**GOOD, "order_id": "A2", "quantity": "0"}).encode()
    process_message(bad, seen, vf, rf, c, {}, seen_rej)
    process_message(bad, seen, vf, rf, c, {}, seen_rej)          # replay rejected
    assert c["rejected"] == 1 and len(rf.getvalue().splitlines()) == 1


def test_transform_amounts():
    pl = pytest.importorskip("polars")
    from src.transform import add_amounts
    df = pl.DataFrame({"order_id": ["A"], "customer_id": ["C"], "order_ts": ["2026-08-01T10:00:00"], "product_category": ["home"],
                       "quantity": [3], "unit_price": [200000.0], "discount_pct": [0.1], "payment_method": ["cod"], "status": ["paid"]})
    out = add_amounts(df.lazy()).collect()
    assert out["gross_amount"][0] == 600000.0 and out["net_amount"][0] == pytest.approx(540000.0)
    assert str(out.schema["order_ts"]).startswith("Datetime")
