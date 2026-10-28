-- Staging: nama & tipe data konsisten. Grain: 1 baris per order_id.
select
    cast(order_id as varchar)              as order_id,
    cast(customer_id as varchar)           as customer_id,
    cast(order_ts as timestamp)            as ordered_at,
    cast(order_ts as date)                 as order_date,
    lower(trim(product_category))          as product_category,
    cast(quantity as integer)              as quantity,
    cast(unit_price as decimal(18, 2))     as unit_price,
    cast(discount_pct as decimal(5, 4))    as discount_pct,
    lower(trim(payment_method))            as payment_method,
    lower(trim(status))                    as status,
    cast(gross_amount as decimal(18, 2))   as gross_amount,
    cast(net_amount as decimal(18, 2))     as net_amount,
    source
from {{ source('raw', 'orders') }}
