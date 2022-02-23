
{{ config(materialized='table') }}

select * from {{ source('mysql_rds_sakila', 'country') }}
where _fivetran_deleted = FALSE
