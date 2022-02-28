
{{ config(materialized = 'table') }}

select 
    stg_store.store_id,
    stg_address.address,
    stg_address.phone,
    stg_address.district,
    stg_staff.last_name
from {{ ref('stg_store') }}
left join {{ ref('stg_address') }} on stg_store.address_id = stg_address.address_id
left join {{ ref('stg_staff') }} on stg_store.manager_staff_id = stg_staff.staff_id
