-- Singular test: gagal jika ada baris yang net_amount-nya tidak sama dengan quantity*unit_price*(1-discount_pct) (toleransi 0.02)
select order_id
from {{ ref('fct_orders') }}
where abs(net_amount - (quantity * unit_price * (1 - discount_pct))) > 0.02
