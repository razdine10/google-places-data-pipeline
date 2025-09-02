{{
  config(
    materialized='view'
    , description='Staging table for Google Places restaurants - cleaned and standardized'
  )
}}

-- Restaurant data cleaning and standardization
SELECT 
    -- Identifiers
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
    , NULL as formatted_phone_number  -- Non disponible dans données diversifiées
    , NULL as website  -- Non disponible dans données diversifiées
    
    -- Categorization
    , types
    , types as primary_type  -- Utilise la colonne types disponible
    , CASE 
        WHEN types LIKE '%restaurant%' THEN 'Restaurant'
        WHEN types LIKE '%bar%' THEN 'Bar'
        WHEN types LIKE '%food%' THEN 'Food'
        ELSE 'Other'
    END AS business_category
    
    -- Operating hours (simplified)
    , false AS has_opening_hours  -- Horaires non disponibles dans données diversifiées
    
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
  -- AND place_id IS NOT NULL  -- Temporairement désactivé pour données diversifiées
