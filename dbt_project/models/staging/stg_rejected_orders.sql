select
    nullif(order_id, '')    as order_id,
    reject_reason,
    reject_fields,
    source,
    rejected_at
from {{ source('raw', 'rejected_orders') }}
