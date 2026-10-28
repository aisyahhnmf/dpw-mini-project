-- Penjualan harian. Definisi: net_sales_paid_shipped = hanya status paid/shipped (penjualan terealisasi);
-- net_amount_all_status = seluruh status (untuk pembanding).
select
    date_key                                                                       as order_date,
    count(*)                                                                       as total_orders,
    sum(quantity)                                                                  as total_units,
    sum(gross_amount)                                                              as gross_amount,
    sum(net_amount)                                                                as net_amount_all_status,
    sum(case when status in ('paid', 'shipped') then net_amount else 0 end)        as net_sales_paid_shipped
from {{ ref('fct_orders') }}
group by date_key
