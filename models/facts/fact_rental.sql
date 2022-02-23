
{{ config(materialized = 'table') }}

SELECT 
    stg_rental.rental_id,
    dim_customer_type_two.customer_key,
    dim_staff.staff_key,
    1 as rental_quantity
FROM {{ ref('stg_rental') }}
LEFT JOIN {{ ref('dim_customer_type_two') }} ON stg_rental.customer_id = dim_customer_type_two.customer_id
LEFT JOIN {{ ref('dim_staff') }} ON stg_rental.staff_id = dim_staff.staff_id
