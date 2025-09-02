#!/usr/bin/env python3
"""
Google Places API Test Script
=============================

Quickly verifies your connection to the Google Places API
without running the full data collection.

Usage: python test_google_places_api.py
"""

import os
import requests
from dotenv import load_dotenv

def test_google_places_connection():
    """Simple connectivity test for the Google Places API."""

    print("Google Places API connectivity test")
    print("=" * 45)

    # Load configuration
    if os.path.exists("config/.env"):
        load_dotenv("config/.env")
        print("Found config/.env")
    else:
        print("config/.env not found")

    # Get API key
    api_key = os.getenv("GOOGLE_PLACES_API_KEY", "")

    if not api_key:
        print("ERROR: Missing Google Places API key")
        print("Add GOOGLE_PLACES_API_KEY to config/.env")
        print("Get a key at: https://console.cloud.google.com/")
        return False

    print(f"API key detected: {api_key[:20]}...")

    # Simple request test
    print("Sending test request to the API...")

    url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    params = {
        "query": "restaurants in Paris, France",
        "type": "restaurant",
        "key": api_key,
        "language": "en"
    }

    try:
        response = requests.get(url, params=params)

        if response.status_code == 200:
            data = response.json()

            if data.get("status") == "OK":
                restaurants = data.get("results", [])

                print("Connection successful")
                print(f"Found {len(restaurants)} restaurants. Examples:")

                for i, restaurant in enumerate(restaurants[:3], 1):
                    name = restaurant.get("name", "Unknown name")
                    rating = restaurant.get("rating", 0)
                    user_ratings_total = restaurant.get("user_ratings_total", 0)
                    address = restaurant.get("formatted_address", "Unknown address")
                    print(f"  {i}. {name}")
                    print(f"     Rating: {rating}/5 ({user_ratings_total} reviews)")
                    print(f"     Address: {address}")
                    print()

                print("Your Google Places configuration looks good.")
                print("You can now run: python src/google_places_collector.py")
                return True

            elif data.get("status") == "REQUEST_DENIED":
                print("ERROR: Request denied")
                print("Ensure the Places API is enabled in your Google Cloud project")
                print("https://console.cloud.google.com/apis/library/places-backend.googleapis.com")
                return False

            elif data.get("status") == "INVALID_REQUEST":
                print("ERROR: Invalid request")
                print("Check your API key and parameters")
                return False

            else:
                print(f"API ERROR: {data.get('status')}")
                if "error_message" in data:
                    print(f"  Message: {data['error_message']}")
                return False

        elif response.status_code == 403:
            print("ERROR 403: Access denied")
            print("Verify your API key permissions")
            print("https://console.cloud.google.com/apis/credentials")
            return False

        else:
            print(f"HTTP ERROR {response.status_code}")
            print(f"  Message: {response.text}")
            return False

    except Exception as e:
        print(f"Connection error: {str(e)}")
        print("Check your internet connection and API configuration")
        return False

def show_setup_instructions():
    """Print minimal setup instructions."""
    print("\nSETUP INSTRUCTIONS")
    print("=" * 35)
    print("1) Go to https://console.cloud.google.com/")
    print("2) Create or select a project")
    print("3) Enable the Places API:")
    print("   https://console.cloud.google.com/apis/library/places-backend.googleapis.com")
    print("4) Create an API key:")
    print("   https://console.cloud.google.com/apis/credentials")
    print("5) Put the key into config/.env:")
    print("   GOOGLE_PLACES_API_KEY=your_key_here")
    print("\nPricing: free credit up to $300/month for new accounts.")

def main():
    """Entry point."""
    success = test_google_places_connection()

    if success:
        print("\nTest passed. Configuration looks correct.")
        print("Run: python src/google_places_collector.py")
    else:
        print("\nTest failed. Configuration required.")
        show_setup_instructions()

if __name__ == "__main__":
    main() 