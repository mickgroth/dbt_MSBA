
{{ config(materialized='table') }}

select * from {{ source('mysql_rds_sakila', 'customer') }}
where _fivetran_deleted = FALSE
