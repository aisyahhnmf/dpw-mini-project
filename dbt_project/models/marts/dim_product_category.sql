select distinct
    md5(product_category) as category_key,
    product_category
from {{ ref('stg_orders') }}
