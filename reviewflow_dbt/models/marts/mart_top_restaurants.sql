{{
  config(
    materialized='table'
    , description='Final mart table for top restaurants with comprehensive metrics'
  )
}}

-- Final table for top restaurants with complete analysis
WITH restaurant_metrics AS (
    SELECT 
        r.place_id
        , r.restaurant_name
        , r.rating
        , r.total_reviews_count
        , r.rating_category
        , r.price_category
        , r.formatted_address
        , r.latitude
        , r.longitude
        , r.website
        , r.formatted_phone_number
        , r.business_category
        , r.popularity_level
        , r.is_highly_rated
        
        -- Review metrics
        , COUNT(rv.rating) AS reviews_collected
        , ROUND(AVG(rv.rating), 2) AS avg_review_rating
        
        -- Sentiment analysis (use only matched reviews and avoid div-by-zero)
        , COALESCE(ROUND(
            COUNT(CASE WHEN rv.sentiment_simple = 'Positive' THEN 1 END) * 100.0 / NULLIF(COUNT(rv.rating), 0), 
            1
        ), 0) AS positive_sentiment_pct
        
        , COALESCE(ROUND(
            COUNT(CASE WHEN rv.sentiment_simple = 'Negative' THEN 1 END) * 100.0 / NULLIF(COUNT(rv.rating), 0), 
            1
        ), 0) AS negative_sentiment_pct
        
        -- Review quality metrics
        , COALESCE(ROUND(
            COUNT(CASE WHEN rv.is_detailed_review THEN 1 END) * 100.0 / NULLIF(COUNT(rv.rating), 0), 
            1
        ), 0) AS detailed_reviews_pct
        
        -- Topic mentions
        , COUNT(CASE WHEN rv.mentions_service THEN 1 END) AS service_mentions
        , COUNT(CASE WHEN rv.mentions_food THEN 1 END) AS food_mentions
        , COUNT(CASE WHEN rv.contains_positive_keywords THEN 1 END) AS positive_keywords_count
        , COUNT(CASE WHEN rv.contains_negative_keywords THEN 1 END) AS negative_keywords_count
        
    FROM {{ ref('stg_restaurants') }} r
    LEFT JOIN {{ ref('stg_reviews') }} rv 
        ON (
            r.place_id IS NOT NULL AND rv.place_id IS NOT NULL AND r.place_id = rv.place_id
        )
        OR (
            r.place_id IS NULL AND rv.place_id IS NULL AND r.restaurant_name = rv.restaurant_name
        )
    GROUP BY 
        r.place_id, r.restaurant_name, r.rating, r.total_reviews_count
        , r.rating_category, r.price_category, r.formatted_address
        , r.latitude, r.longitude, r.website, r.formatted_phone_number
        , r.business_category, r.popularity_level, r.is_highly_rated
)

, final AS (
    SELECT 
        *
        -- Composite quality score (0-100)
        , ROUND(
            (rating * 15) +                                    -- Rating out of 5 * 15 = 75 points max
            (LEAST(positive_sentiment_pct / 4, 20)) +          -- Positive sentiment = 20 points max
            (CASE WHEN reviews_collected >= 5 THEN 5 ELSE 0 END) -- Bonus for sufficient reviews
        , 1) AS quality_score
        
        -- Restaurant tier classification
        , CASE 
            WHEN rating >= 4.5 AND positive_sentiment_pct >= 80 THEN 'Premium'
            WHEN rating >= 4.0 AND positive_sentiment_pct >= 70 THEN 'Excellent'
            WHEN rating >= 3.5 AND positive_sentiment_pct >= 60 THEN 'Very Good'
            WHEN rating >= 3.0 THEN 'Good'
            ELSE 'Average'
        END AS restaurant_tier
        
        -- Business recommendation
        , CASE 
            WHEN rating >= 4.0 AND positive_sentiment_pct >= 75 AND reviews_collected >= 3 THEN 'Highly Recommended'
            WHEN rating >= 3.5 AND positive_sentiment_pct >= 60 THEN 'Recommended'
            WHEN rating >= 3.0 THEN 'Average'
            ELSE 'Not Recommended'
        END AS recommendation
        
    FROM restaurant_metrics
)

SELECT * FROM final
ORDER BY quality_score DESC, rating DESC, positive_sentiment_pct DESC
