
{{ config(materialized='table') }}

select * from {{ source('mysql_rds_sakila', 'inventory') }}
where _fivetran_deleted = FALSE
