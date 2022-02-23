
{% snapshot dim_customer_type_two %}

{{
    config(
      unique_key='CUSTOMER_KEY',
      strategy='check',
      check_cols='all',
      target_schema='dimensions'
    )
}}

SELECT 
    {{ dbt_utils.surrogate_key(['c.CUSTOMER_ID']) }} as CUSTOMER_KEY, 
    c.CUSTOMER_ID,
	c.FIRST_NAME,
	c.LAST_NAME,
	CONCAT (FIRST_NAME,	',',LAST_NAME	) AS firstlastname,
	c.EMAIL,
	a.address AS customer_address,
	a.address2 AS customer_address2,
	ci.city AS customer_city,
	a.district AS customer_district,
	a.postal_code AS customer_postal_code,
	co.country AS customer_country,
	a.phone AS customer_phone,
	case c.active
    when c.active=1 then 'Yes'
    else 'No'
    end AS is_active,
	c.create_date AS registration_date,
	to_timestamp(c.last_update) AS customer_last_udpate
FROM {{ ref('stg_customer') }} c
JOIN {{ ref('stg_address') }} a ON c.address_id = a.address_id
JOIN {{ ref('stg_city') }} ci ON a.city_id = ci.city_id
JOIN {{ ref('stg_country') }} co ON ci.country_id = co.country_id

{% endsnapshot %}
