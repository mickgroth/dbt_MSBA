
{{ config(materialized = 'table') }}

SELECT
    {{ dbt_utils.surrogate_key(['s.STAFF_ID']) }} as STAFF_KEY,
    s.staff_id,
	s.first_name,
	s.last_name,
	CONCAT (FIRST_NAME,	',',LAST_NAME) AS firstlastname,
	s.email,
	CASE s.ACTIVE
		WHEN 'TRUE'
			THEN 'Yes'
		ELSE 'No'
		END AS active
FROM {{ ref('stg_staff') }} s
