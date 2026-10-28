-- Penyebab penolakan per FIELD (satu record bisa melanggar >1 field).
select
    trim(field) as rejected_field,
    count(*)    as total_rejected_records
from {{ ref('stg_rejected_orders') }},
     unnest(string_split(reject_fields, ',')) as u(field)
group by trim(field)
