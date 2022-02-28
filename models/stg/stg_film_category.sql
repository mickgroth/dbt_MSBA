
{{ config(materialized='table') }}

select * from {{ source('mysql_rds_sakila', 'film_category') }}
where _fivetran_deleted = FALSE
