#!/usr/bin/env python3
"""
Load latest CSV data into DuckDB.
Must be executed before running dbt.
"""

import os
import glob
import duckdb
import pandas as pd
from datetime import datetime

def load_latest_data():
    """Load the most recent CSV files into DuckDB."""

    # Connect to DuckDB
    conn = duckdb.connect('reviewflow.duckdb')

    # Path to the data directory
    data_dir = '../data'

    if not os.path.exists(data_dir):
        print("Data folder not found: ../data")
        return False

    # Find the most recent CSV files (prioritize diverse dataset)
    restaurant_files = glob.glob(f'{data_dir}/restaurants_diverse_*.csv') or glob.glob(f'{data_dir}/restaurants_google_*.csv')
    review_files = glob.glob(f'{data_dir}/reviews_diverse_*.csv') or glob.glob(f'{data_dir}/reviews_google_*.csv')

    if not restaurant_files or not review_files:
        print("CSV files not found")
        print(f"Restaurants: {len(restaurant_files)} file(s)")
        print(f"Reviews: {len(review_files)} file(s)")
        return False

    # Take the most recent files
    latest_restaurant_file = max(restaurant_files, key=os.path.getmtime)
    latest_review_file = max(review_files, key=os.path.getmtime)

    print(f"Loading restaurants: {os.path.basename(latest_restaurant_file)}")
    print(f"Loading reviews: {os.path.basename(latest_review_file)}")

    try:
        # Load restaurants
        restaurants_df = pd.read_csv(latest_restaurant_file)

        # Add missing columns if necessary
        if 'collected_at' not in restaurants_df.columns:
            restaurants_df['collected_at'] = datetime.now()
        if 'data_source' not in restaurants_df.columns:
            restaurants_df['data_source'] = 'google_places'

        # Create/replace restaurants table
        conn.execute("DROP TABLE IF EXISTS restaurants")
        conn.execute(
            """
            CREATE TABLE restaurants AS 
            SELECT * FROM restaurants_df
            """
        )

        restaurant_count = conn.execute("SELECT COUNT(*) FROM restaurants").fetchone()[0]
        print(f"{restaurant_count} restaurants loaded")

        # Load reviews
        reviews_df = pd.read_csv(latest_review_file)

        # Add missing columns if necessary
        if 'collected_at' not in reviews_df.columns:
            reviews_df['collected_at'] = datetime.now()
        if 'data_source' not in reviews_df.columns:
            reviews_df['data_source'] = 'google_places'

        # Create/replace reviews table
        conn.execute("DROP TABLE IF EXISTS reviews")
        conn.execute(
            """
            CREATE TABLE reviews AS 
            SELECT * FROM reviews_df
            """
        )

        review_count = conn.execute("SELECT COUNT(*) FROM reviews").fetchone()[0]
        print(f"{review_count} reviews loaded")

        # Close connection
        conn.close()

        print("Data successfully loaded into DuckDB")
        return True

    except Exception as e:
        print(f"Error while loading data: {str(e)}")
        conn.close()
        return False

if __name__ == "__main__":
    print("Loading data into DuckDB")
    print("=" * 40)

    success = load_latest_data()

    if success:
        print("\nYou can now run: dbt run")
        exit(0)
    else:
        print("\nData loading failed")
        exit(1) 