
{{ config(materialized='table') }}

select * from {{ source('mysql_rds_sakila', 'category') }}
where _fivetran_deleted = FALSE
