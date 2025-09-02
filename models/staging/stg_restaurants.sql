{{
  config(
    materialized='view',
    description='Staging table for Google Places restaurants - cleaned and standardized'
  )
}}

-- Restaurant data cleaning and standardization
SELECT 
    place_id
  , name AS restaurant_name
  -- Quality metrics
  , COALESCE(rating, 0) AS rating
  , COALESCE(user_ratings_total, 0) AS total_reviews_count
  , CASE 
        WHEN rating >= 4.5 THEN 'Excellent'
        WHEN rating >= 4.0 THEN 'Very Good'
        WHEN rating >= 3.5 THEN 'Good'
        WHEN rating >= 3.0 THEN 'Average'
        WHEN rating > 0 THEN 'Poor'
        ELSE 'Not Rated'
    END AS rating_category
  -- Pricing
  , CASE 
        WHEN price_level = 1 THEN 'Budget'
        WHEN price_level = 2 THEN 'Moderate'
        WHEN price_level = 3 THEN 'Expensive'
        WHEN price_level = 4 THEN 'Very Expensive'
        ELSE 'Not Specified'
    END AS price_category
  , price_level
  -- Location
  , formatted_address
  , latitude
  , longitude
  -- Contact information
  , formatted_phone_number
  , website
  -- Categorization
  , types
  , primary_type
  , CASE 
        WHEN primary_type LIKE '%restaurant%' THEN 'Restaurant'
        WHEN primary_type LIKE '%bar%' THEN 'Bar'
        WHEN primary_type LIKE '%food%' THEN 'Food'
        ELSE 'Other'
    END AS business_category
  -- Operating hours (simplified)
  , CASE 
        WHEN opening_hours IS NOT NULL AND opening_hours != '[]' THEN true
        ELSE false
    END AS has_opening_hours
  -- Metadata
  , collected_at::timestamp AS collected_at
  , data_source
  -- Quality indicators
  , CASE 
        WHEN rating >= 4.0 AND user_ratings_total >= 100 THEN true
        ELSE false
    END AS is_highly_rated
  , CASE 
        WHEN user_ratings_total >= 500 THEN 'Very Popular'
        WHEN user_ratings_total >= 100 THEN 'Popular'
        WHEN user_ratings_total >= 20 THEN 'Well Known'
        ELSE 'Lesser Known'
    END AS popularity_level
FROM {{ ref('restaurants') }}
-- Basic quality filtering
WHERE name IS NOT NULL 
  AND place_id IS NOT NULL 