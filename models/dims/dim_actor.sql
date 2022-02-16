
{{ config(materialized = 'table') }}

select 
    actor_id as actor_key,
    concat_ws(' ', last_name, first_name ) as full_name
from
    {{ ref('stg_actor') }}
