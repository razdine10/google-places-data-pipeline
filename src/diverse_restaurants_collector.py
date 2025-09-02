#!/usr/bin/env python3
"""
DIVERSE RESTAURANTS COLLECTOR - FOR QUALITY DASHBOARD
====================================================

This script collects restaurants with GREAT DIVERSITY of ratings
to create an interesting dashboard with:
- Excellent restaurants (4.5-5.0 stars)
- Good restaurants (4.0-4.5 stars) 
- Average restaurants (3.0-4.0 stars)
- Poor restaurants (2.0-3.0 stars)

Uses 6 different queries to maximize diversity!
"""

import os
import json
import pandas as pd
import requests
import logging
import time
from datetime import datetime
from dotenv import load_dotenv
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/diverse_collector.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DiverseRestaurantsCollector:
    """
    Specialized collector to obtain DIVERSE restaurants
    perfect for a quality dashboard with rating variation
    """
    
    def __init__(self):
        """Initialize the diverse collector"""
        logger.info("Initializing diverse restaurant collector...")
        
        self.load_config()
        self.validate_config()
        self.setup_directories()
        
        self.search_strategies = [
            {
                "query": "michelin restaurants fine dining",
                "expected_rating": "4.5-5.0",
                "target": 10,
                "description": "High-end restaurants"
            },
            {
                "query": "restaurants",
                "expected_rating": "4.0-4.5", 
                "target": 15,
                "description": "Classic restaurants"
            },
            {
                "query": "fast food quick service",
                "expected_rating": "3.5-4.0",
                "target": 10,
                "description": "Fast food"
            },
            {
                "query": "cheap restaurants budget dining",
                "expected_rating": "3.0-4.0",
                "target": 10,
                "description": "Budget restaurants"
            },
            {
                "query": "street food food trucks",
                "expected_rating": "2.5-4.0",
                "target": 8,
                "description": "Street food"
            },
            {
                "query": "bars pubs bistros",
                "expected_rating": "3.0-4.5",
                "target": 7,
                "description": "Bars and bistros"
            }
        ]
        
        logger.info("Diverse collector initialized with 6 search strategies")
    
    def load_config(self):
        """Load configuration"""
        env_file = "config/.env"
        if os.path.exists(env_file):
            load_dotenv(env_file)
        
        self.google_api_key = os.getenv("GOOGLE_PLACES_API_KEY", "")
        self.target_city = os.getenv("TARGET_CITY", "Paris")
        self.target_country = os.getenv("TARGET_COUNTRY", "France")
        
        self.base_url = "https://maps.googleapis.com/maps/api/place"
    
    def validate_config(self):
        """Validate configuration"""
        if not self.google_api_key:
            raise ValueError("GOOGLE_PLACES_API_KEY is required in config/.env")
        
        logger.info("Configuration validated")
    
    def setup_directories(self):
        """Create necessary directories"""
        os.makedirs("data", exist_ok=True)
        os.makedirs("logs", exist_ok=True)
    
    def search_diverse_restaurants(self):
        """Search for diverse restaurants using multiple strategies"""
        logger.info("STARTING DIVERSE COLLECTION")
        logger.info("=" * 50)
        
        all_restaurants = []
        
        for i, strategy in enumerate(self.search_strategies, 1):
            logger.info(f"Strategy {i}/6: {strategy['description']}")
            logger.info(f"   Query: {strategy['query']}")
            logger.info(f"   Expected ratings: {strategy['expected_rating']}")
            
            try:
                url = f"{self.base_url}/textsearch/json"
                params = {
                    'query': f"{strategy['query']} in {self.target_city} {self.target_country}",
                    'key': self.google_api_key,
                    'type': 'restaurant'
                }
                
                response = requests.get(url, params=params)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if data['status'] == 'OK':
                        results = data.get('results', [])
                        
                        existing_place_ids = {r.get('place_id') for r in all_restaurants}
                        existing_names = {r.get('name') for r in all_restaurants}
                        
                        new_restaurants = []
                        for result in results:
                            place_id = result.get('place_id')
                            name = result.get('name', '')
                            
                            if place_id not in existing_place_ids and name not in existing_names:
                                result['search_strategy'] = strategy['description']
                                result['expected_rating_range'] = strategy['expected_rating']
                                new_restaurants.append(result)
                                
                                if len(new_restaurants) >= strategy['target']:
                                    break
                        
                        all_restaurants.extend(new_restaurants)
                        
                        logger.info(f"   Found {len(new_restaurants)} new restaurants")
                        
                        for j, restaurant in enumerate(new_restaurants[:3]):
                            name = restaurant.get('name', 'Unknown')
                            rating = restaurant.get('rating', 'N/A')
                            total_ratings = restaurant.get('user_ratings_total', 0)
                            logger.info(f"      {j+1}. {name} - {rating} stars ({total_ratings} reviews)")
                        
                        logger.info(f"   Total collected so far: {len(all_restaurants)} restaurants")
                    else:
                        logger.warning(f"   API error: {data.get('status', 'Unknown')}")
                else:
                    logger.error(f"   HTTP error: {response.status_code}")
                    
            except Exception as e:
                logger.error(f"   Search error: {str(e)}")
            
            if i < len(self.search_strategies):
                logger.info("   Pausing for 2 seconds...")
                time.sleep(2)
                logger.info("-" * 40)
        
        logger.info(f"COLLECTION COMPLETED: {len(all_restaurants)} diverse restaurants")
        return all_restaurants
    
    def collect_restaurant_details(self, restaurants):
        """Collect detailed information for each restaurant"""
        logger.info(f"COLLECTING DETAILS for {len(restaurants)} restaurants...")
        
        detailed_restaurants = []
        
        for restaurant in restaurants:
            try:
                place_id = restaurant.get('place_id')
                name = restaurant.get('name', 'Unknown')
                rating = restaurant.get('rating', 'N/A')
                strategy = restaurant.get('search_strategy', 'Unknown')
                
                if not place_id:
                    continue
                
                url = f"{self.base_url}/details/json"
                params = {
                    'place_id': place_id,
                    'key': self.google_api_key,
                    'fields': 'name,rating,user_ratings_total,reviews,formatted_address,geometry,types,price_level,opening_hours,formatted_phone_number,website'
                }
                
                response = requests.get(url, params=params)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if data['status'] == 'OK':
                        result = data['result']
                        result['search_strategy'] = strategy
                        result['expected_rating_range'] = restaurant.get('expected_rating_range', '')
                        detailed_restaurants.append(result)
                        
                        logger.info(f"   {name} - {rating} stars ({strategy})")
                    else:
                        logger.warning(f"   Details error for {name}: {data.get('status')}")
                else:
                    logger.error(f"   HTTP details error for {name}: {response.status_code}")
                    
            except Exception as e:
                logger.error(f"   Details error {name}: {str(e)}")
            
            time.sleep(0.2)
        
        logger.info(f"Details collected for {len(detailed_restaurants)} restaurants")
        return detailed_restaurants
    
    def prepare_data_for_export(self, restaurants):
        """Prepare data for CSV export"""
        logger.info("Preparing data for export...")
        
        restaurants_data = []
        reviews_data = []
        
        for restaurant in restaurants:
            restaurant_info = {
                'place_id': restaurant.get('place_id'),
                'name': restaurant.get('name'),
                'rating': restaurant.get('rating'),
                'user_ratings_total': restaurant.get('user_ratings_total', 0),
                'formatted_address': restaurant.get('formatted_address'),
                'latitude': restaurant.get('geometry', {}).get('location', {}).get('lat'),
                'longitude': restaurant.get('geometry', {}).get('location', {}).get('lng'),
                'types': ', '.join(restaurant.get('types', [])),
                'price_level': restaurant.get('price_level'),
                'formatted_phone_number': restaurant.get('formatted_phone_number'),
                'website': restaurant.get('website'),
                'search_strategy': restaurant.get('search_strategy'),
                'expected_rating_range': restaurant.get('expected_rating_range'),
                'collected_at': datetime.now(),
                'data_source': 'google_places_diverse'
            }
            restaurants_data.append(restaurant_info)
            
            reviews = restaurant.get('reviews', [])
            for review in reviews:
                review_info = {
                    'place_id': restaurant.get('place_id'),
                    'restaurant_name': restaurant.get('name'),
                    'author_name': review.get('author_name'),
                    'rating': review.get('rating'),
                    'text': review.get('text'),
                    'time': review.get('time'),
                    'relative_time_description': review.get('relative_time_description'),
                    'data_source': 'google_places_diverse',
                    'collected_at': datetime.now()
                }
                reviews_data.append(review_info)
        
        restaurants_df = pd.DataFrame(restaurants_data)
        reviews_df = pd.DataFrame(reviews_data)
        
        logger.info(f"Data prepared: {len(restaurants_df)} restaurants, {len(reviews_df)} reviews")
        
        return restaurants_df, reviews_df
    
    def save_data_locally(self, restaurants_df, reviews_df):
        """Save data to local CSV files"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        restaurants_file = f"data/restaurants_diverse_{timestamp}.csv"
        reviews_file = f"data/reviews_diverse_{timestamp}.csv"
        
        restaurants_df.to_csv(restaurants_file, index=False)
        reviews_df.to_csv(reviews_file, index=False)
        
        logger.info(f"Data saved:")
        logger.info(f"   - Restaurants: {restaurants_file}")
        logger.info(f"   - Reviews: {reviews_file}")
        
        return [restaurants_file, reviews_file]
    
    def analyze_diversity(self, restaurants_df):
        """Analyze rating diversity for dashboard quality"""
        logger.info("DIVERSITY ANALYSIS")
        logger.info("=" * 40)
        
        rating_distribution = restaurants_df['rating'].value_counts().sort_index()
        logger.info("Rating distribution:")
        
        for rating, count in rating_distribution.items():
            logger.info(f"   {rating:.1f} stars: {count} restaurants")
        
        strategy_stats = restaurants_df.groupby('search_strategy').agg({
            'rating': ['count', 'mean']
        }).round(2)
        
        logger.info("\nBy search strategy:")
        for strategy, row in strategy_stats.iterrows():
            count = row[('rating', 'count')]
            avg_rating = row[('rating', 'mean')]
            logger.info(f"   {strategy}: {count} restaurants (average rating: {avg_rating:.2f})")
        
        avg_rating = restaurants_df['rating'].mean()
        min_rating = restaurants_df['rating'].min()
        max_rating = restaurants_df['rating'].max()
        
        logger.info(f"\nGlobal statistics:")
        logger.info(f"   Average rating: {avg_rating:.2f}")
        logger.info(f"   Minimum rating: {min_rating:.1f}")
        logger.info(f"   Maximum rating: {max_rating:.1f}")
        logger.info(f"   Range: {max_rating - min_rating:.1f} points")
        
        return {
            'total_restaurants': len(restaurants_df),
            'avg_rating': avg_rating,
            'min_rating': min_rating,
            'max_rating': max_rating,
            'rating_range': max_rating - min_rating,
            'strategies': len(restaurants_df['search_strategy'].unique())
        }
    
    def run(self):
        """Execute the complete diverse collection process"""
        try:
            logger.info("STARTING DIVERSE COLLECTOR")
            logger.info("=" * 60)
            
            restaurants = self.search_diverse_restaurants()
            
            if not restaurants:
                logger.error("No restaurants found")
                return None
            
            detailed_restaurants = self.collect_restaurant_details(restaurants)
            
            restaurants_df, reviews_df = self.prepare_data_for_export(detailed_restaurants)
            
            files = self.save_data_locally(restaurants_df, reviews_df)
            
            diversity_stats = self.analyze_diversity(restaurants_df)
            
            logger.info("DIVERSE COLLECTION COMPLETED SUCCESSFULLY")
            logger.info("=" * 50)
            logger.info(f"Results:")
            logger.info(f"   • {diversity_stats['total_restaurants']} restaurants collected")
            logger.info(f"   • Average rating: {diversity_stats['avg_rating']:.2f}")
            logger.info(f"   • Rating range: {diversity_stats['rating_range']:.1f} points")
            logger.info(f"   • Files: {len(files)} created")
            logger.info("Perfect data for a quality dashboard")
            
            return files
            
        except Exception as e:
            logger.error(f"Fatal error: {str(e)}")
            raise

def main():
    """Main execution function"""
    print("Diverse Restaurants Collector - Quality Dashboard Version")
    print("=" * 60)
    
    collector = DiverseRestaurantsCollector()
    files = collector.run()
    
    if files:
        print(f"\nSuccess! Files created: {len(files)}")
        for file in files:
            print(f"  - {file}")
    else:
        print("\nCollection failed. Check logs for details.")

if __name__ == "__main__":
    main() 