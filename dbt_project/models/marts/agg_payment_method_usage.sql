select
    p.payment_method,
    count(*)                                         as total_orders,
    round(100.0 * count(*) / sum(count(*)) over (), 2) as pct_of_orders
from {{ ref('fct_orders') }} f
join {{ ref('dim_payment_method') }} p using (payment_method_key)
group by p.payment_method
