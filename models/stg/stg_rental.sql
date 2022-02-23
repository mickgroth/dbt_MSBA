
{{ config(materialized='table') }}

select * from {{ source('mysql_rds_sakila', 'rental') }}
where _fivetran_deleted = FALSE
