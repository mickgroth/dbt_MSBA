
{{ config(materialized = 'table') }}

SELECT 
    d.DATE_KEY as rental_date_key,
    tod.TIMEOFDAY_KEY as rental_timeofday_key,
    //HOUR(r.rental_date),
    //r.rental_date,
    r.rental_id,
    c.CUSTOMER_KEY,
    f.FILM_KEY,
    sd.staff_key,
    sl.STORE_LOCATION_KEY,
    1 as rental_quantity,
    p.amount
FROM {{ ref('stg_rental') }} r
     JOIN {{ source('analytics', 'date_dim') }} d ON to_number(to_varchar(to_date(r.rental_date),'YYYYMMDD')) = d.DATE_KEY
     JOIN {{ source('analytics', 'timeofday_dim') }} tod ON HOUR(r.rental_date) = tod.HROFDAY AND MINOFDAY =0
     JOIN {{ ref('dim_customer') }} c ON r.customer_id = c.customer_id
     JOIN {{ ref('stg_inventory') }} i ON r.inventory_id = i.inventory_id
     JOIN {{ ref('dim_store_location') }} sl ON i.store_id = sl.STORE_ID
     JOIN {{ ref('dim_film') }} f on  f.film_id = i.film_id
     JOIN {{ ref('dim_staff') }} sd ON r.staff_id = sd.STAFF_ID
     JOIN {{ ref('stg_payment') }} p ON r.rental_id = p.rental_id
        WHERE to_date(r.rental_date) < '2020-01-01'
