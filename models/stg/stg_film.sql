
{{ config(materialized='table') }}

select * from {{ source('mysql_rds_sakila', 'film') }}
where _fivetran_deleted = FALSE
