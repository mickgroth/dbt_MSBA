
{{ config(materialized='table') }}

select * from {{ source('mysql_rds_sakila', 'language') }}
where _fivetran_deleted = FALSE
