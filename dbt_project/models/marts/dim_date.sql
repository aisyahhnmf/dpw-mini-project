-- Dimensi tanggal dari tanggal-tanggal yang muncul pada pesanan.
select distinct
    order_date                        as date_key,
    extract(year from order_date)     as year,
    extract(month from order_date)    as month,
    extract(day from order_date)      as day,
    strftime(order_date, '%A')        as day_name,
    extract(isodow from order_date) in (6, 7) as is_weekend
from {{ ref('stg_orders') }}
