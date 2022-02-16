
{{ config(materialized='table') }}

select * from {{ source('mysql_rds_sakila', 'store') }}
where _fivetran_deleted = FALSE
