-- Fact table. GRAIN: satu baris per pesanan (order_id).
select
    o.order_id,
    o.customer_id,
    md5(o.product_category) as category_key,
    md5(o.payment_method)   as payment_method_key,
    o.order_date            as date_key,
    o.ordered_at,
    o.status,
    o.quantity,
    o.unit_price,
    o.discount_pct,
    o.gross_amount,
    o.net_amount,
    o.source
from {{ ref('stg_orders') }} o
