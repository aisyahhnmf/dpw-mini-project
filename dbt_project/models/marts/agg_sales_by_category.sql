select
    c.product_category,
    count(*)                                                                as total_orders,
    sum(f.net_amount)                                                       as net_amount_all_status,
    sum(case when f.status in ('paid','shipped') then f.net_amount else 0 end) as net_sales_paid_shipped
from {{ ref('fct_orders') }} f
join {{ ref('dim_product_category') }} c using (category_key)
group by c.product_category
