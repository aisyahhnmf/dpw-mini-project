select status, count(*) as total_orders, sum(net_amount) as net_amount
from {{ ref('fct_orders') }}
group by status
