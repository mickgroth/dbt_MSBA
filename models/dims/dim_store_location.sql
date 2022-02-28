
{{ config(materialized = 'table') }}

SELECT 
    {{ dbt_utils.surrogate_key(['s.STORE_ID']) }} as STORE_LOCATION_KEY,
    s.store_id,
	a.postal_code,
	c.city,
	a.district,
	co.country,
	a.phone
FROM {{ ref('stg_store') }} s
JOIN {{ ref('stg_address') }} a ON s.address_id = a.address_id
JOIN {{ ref('stg_city') }} c ON a.city_id = c.city_id
JOIN {{ ref('stg_country') }} co ON c.country_id = co.country_id
