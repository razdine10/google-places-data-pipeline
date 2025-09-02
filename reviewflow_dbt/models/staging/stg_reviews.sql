{{
  config(
    materialized='view'
    , description='Staging table for Google Places reviews with sentiment analysis'
  )
}}

-- Review data cleaning and enrichment
SELECT 
    -- Identifiers
    place_id
    , restaurant_name
    
    -- Author information
    , COALESCE(author_name, 'Anonymous') AS author_name
    
    -- Rating and sentiment
    , rating
    , CASE 
        WHEN rating >= 5 THEN 'Excellent'
        WHEN rating >= 4 THEN 'Positive'
        WHEN rating >= 3 THEN 'Neutral'
        WHEN rating >= 2 THEN 'Negative'
        ELSE 'Very Negative'
    END AS sentiment_category
    
    , CASE 
        WHEN rating >= 4 THEN 'Positive'
        WHEN rating = 3 THEN 'Neutral'
        ELSE 'Negative'
    END AS sentiment_simple
    
    -- Content analysis
    , COALESCE(text, '') AS review_text
    , LENGTH(COALESCE(text, '')) AS review_length
    , CASE 
        WHEN LENGTH(COALESCE(text, '')) > 200 THEN 'Detailed'
        WHEN LENGTH(COALESCE(text, '')) > 50 THEN 'Medium'
        WHEN LENGTH(COALESCE(text, '')) > 0 THEN 'Short'
        ELSE 'Empty'
    END AS review_length_category
    
    -- Temporal analysis
    , time AS review_timestamp
    , relative_time_description
    , CASE 
        WHEN relative_time_description LIKE '%day%' THEN 'Recent'
        WHEN relative_time_description LIKE '%week%' THEN 'This Week'
        WHEN relative_time_description LIKE '%month%' THEN 'This Month'
        WHEN relative_time_description LIKE '%year%' THEN 'This Year'
        ELSE 'Old'
    END AS review_age_category
    
    -- Review quality indicators
    , CASE 
        WHEN LENGTH(COALESCE(text, '')) >= 50 AND rating IS NOT NULL THEN true
        ELSE false
    END AS is_detailed_review
    
    , CASE 
        WHEN rating IN (1, 5) THEN true
        ELSE false
    END AS is_extreme_rating
    
    -- Metadata
    , collected_at::timestamp AS collected_at
    , data_source
    
    -- Basic content analysis (keywords)
    , CASE 
        WHEN LOWER(text) LIKE '%excellent%' OR LOWER(text) LIKE '%perfect%' OR LOWER(text) LIKE '%amazing%' 
          OR LOWER(text) LIKE '%fantastic%' OR LOWER(text) LIKE '%wonderful%' THEN true
        ELSE false
    END AS contains_positive_keywords
    
    , CASE 
        WHEN LOWER(text) LIKE '%bad%' OR LOWER(text) LIKE '%terrible%' OR LOWER(text) LIKE '%awful%'
          OR LOWER(text) LIKE '%disappointing%' OR LOWER(text) LIKE '%poor%' THEN true
        ELSE false
    END AS contains_negative_keywords
    
    , CASE 
        WHEN LOWER(text) LIKE '%service%' THEN true
        ELSE false
    END AS mentions_service
    
    , CASE 
        WHEN LOWER(text) LIKE '%food%' OR LOWER(text) LIKE '%dish%' OR LOWER(text) LIKE '%meal%' 
          OR LOWER(text) LIKE '%cuisine%' OR LOWER(text) LIKE '%taste%' THEN true
        ELSE false
    END AS mentions_food

FROM {{ ref('reviews') }}

-- Quality filtering
WHERE rating IS NOT NULL
  AND rating BETWEEN 1 AND 5
  -- AND place_id IS NOT NULL  -- Temporairement désactivé pour données diversifiées
