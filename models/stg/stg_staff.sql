
{{ config(materialized='table') }}

select * from {{ source('mysql_rds_sakila', 'staff') }}
where _fivetran_deleted = FALSE
