
{{ config(materialized='table') }}

select * from {{ source('mysql_rds_sakila', 'payment') }}
where _fivetran_deleted = FALSE
