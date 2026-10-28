"""Data contract (Pydantic v2) untuk record pesanan.

Aturan: order_id & customer_id wajib; quantity integer > 0; unit_price >= 0;
discount_pct 0..1; order_ts dapat di-parse; status in {paid, shipped, cancelled, refunded}.
"""
from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator


class ContractViolation(Exception):
    """Dilempar pada mode fail-fast ketika ada record yang melanggar kontrak."""


class OrderContract(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    order_id: str = Field(min_length=1)
    customer_id: str = Field(min_length=1)
    order_ts: datetime
    product_category: str = Field(min_length=1)
    quantity: int = Field(gt=0)
    unit_price: float = Field(ge=0)
    discount_pct: float = Field(ge=0, le=1)
    payment_method: str = Field(min_length=1)
    status: Literal["paid", "shipped", "cancelled", "refunded"]

    @field_validator("order_ts")
    @classmethod
    def _normalize_ts(cls, v: datetime) -> datetime:
        # timestamp tanpa zona waktu dianggap UTC; yang berzona dikonversi ke UTC
        return v.replace(tzinfo=None) if v.tzinfo is None else v.astimezone(timezone.utc).replace(tzinfo=None)


def _format_errors(exc: ValidationError) -> tuple[str, str]:
    fields, parts = [], []
    for err in exc.errors():
        loc = ".".join(str(x) for x in err["loc"]) or "record"
        fields.append(loc)
        parts.append(f"{loc}: {err['msg']} [{err['type']}]")
    return "; ".join(parts), ",".join(sorted(set(fields)))


def validate_record(record: dict):
    """Return (valid_dict, None) atau (None, {'reason':..., 'fields':...})."""
    try:
        obj = OrderContract(**record)
    except ValidationError as exc:
        reason, fields = _format_errors(exc)
        return None, {"reason": reason, "fields": fields}
    except TypeError as exc:  # key bukan string, dll.
        return None, {"reason": str(exc), "fields": "record"}
    d = obj.model_dump()
    d["order_ts"] = d["order_ts"].strftime("%Y-%m-%dT%H:%M:%S")
    return d, None


def validate_batch(records: list[dict], fail_fast: bool = False):
    """Pisahkan valid vs rejected. Rejected menyimpan record ASLI + alasan (audit trail)."""
    valid, rejected = [], []
    for rec in records:
        ok, err = validate_record(rec)
        if ok is not None:
            valid.append(ok)
        else:
            if fail_fast:
                raise ContractViolation(f"order_id={rec.get('order_id')!r}: {err['reason']}")
            rejected.append({**rec, "reject_reason": err["reason"], "reject_fields": err["fields"]})
    return valid, rejected
