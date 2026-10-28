-- Singular test: record yang ditolak data contract TIDAK BOLEH ada di fact.
select f.order_id
from {{ ref('fct_orders') }} f
join {{ ref('stg_rejected_orders') }} r on f.order_id = r.order_id
