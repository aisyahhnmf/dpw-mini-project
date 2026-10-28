select distinct
    md5(payment_method) as payment_method_key,
    payment_method
from {{ ref('stg_orders') }}
