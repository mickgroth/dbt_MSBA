
{{ config(materialized = 'table') }}

SELECT 
    {{ dbt_utils.surrogate_key(['f.FILM_ID']) }} as FILM_KEY, 
    f.film_id,
	f.title AS film_title,
	l.name AS LANGUAGE,
	c.name AS category_name,
	f.rental_duration,
	f.RENTAL_RATE,
	f.REPLACEMENT_COST,
	f.LENGTH
FROM {{ ref('stg_film') }} f
JOIN {{ ref('stg_language') }} l ON f.language_id = l.language_id
JOIN {{ ref('stg_film_category') }} fc ON f.film_id = fc.film_id
JOIN {{ ref('stg_category') }} c ON fc.category_id = c.category_id
