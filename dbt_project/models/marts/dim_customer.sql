select
    customer_id,
    min(ordered_at) as first_order_at,
    max(ordered_at) as last_order_at,
    count(*)        as lifetime_orders
from {{ ref('stg_orders') }}
group by customer_id
