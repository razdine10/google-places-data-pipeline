# Google Places Data Collector – Beginner Guide

## Description

This project collects restaurant data from the Google Places API and saves it as CSV/JSON files, with an optional upload to AWS S3.

What the script does:
1. Connects to the Google Places API
2. Retrieves a list of restaurants for a target city (default: Paris)
3. Fetches reviews for each restaurant
4. Saves the data as CSV and JSON
5. Optionally uploads the files to AWS S3

## Installation and Setup

### 1) Install dependencies
```bash
pip install -r requirements.txt
```

### 2) Set up the Google Places API

Step A: Create a Google Cloud project
1. Open Google Cloud Console: https://console.cloud.google.com/
2. Create a new project (or select an existing one)

Step B: Enable the Places API
1. Open: https://console.cloud.google.com/apis/library/places-backend.googleapis.com
2. Click “ENABLE”

Step C: Create an API key
1. Open: https://console.cloud.google.com/apis/credentials
2. Click “CREATE CREDENTIALS” → “API key”
3. Copy your API key

### 3) Project configuration
1. Copy the template file:
```bash
cp config/config_template.txt config/.env
```
2. Edit `config/.env` and add your API key:
```
GOOGLE_PLACES_API_KEY=your_google_places_api_key
```

### 4) Optional: AWS S3
If you want to upload to S3, add the following to `config/.env`:
```
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
S3_BUCKET_NAME=your-unique-bucket-name
```

## Usage

### Connection test (recommended)
```bash
python test_google_places_api.py
```

### Run the collector
```bash
python src/google_places_collector.py
```

### What happens
1. Search: the script queries restaurants in the target city
2. Details: it fetches details and reviews for each restaurant
3. Save: data is written to the `data/` folder
4. Upload: if configured, files are uploaded to S3

### Generated files
- `data/restaurants_google_YYYYMMDD_HHMMSS.csv` – Restaurants
- `data/reviews_google_YYYYMMDD_HHMMSS.csv` – Reviews
- `data/google_places_data_complete_YYYYMMDD_HHMMSS.json` – Combined data
- `logs/google_places_collector.log` – Execution log

## Data Structure

### Restaurants (CSV)
```
place_id, name, rating, user_ratings_total, formatted_address, latitude, longitude, types, opening_hours, ...
```

### Reviews (CSV)
```
place_id, restaurant_name, author_name, rating, text, relative_time_description, ...
```

## Google Places Costs (summary)
- Free credit: $300/month (new Google Cloud accounts)
- Text Search and Place Details are billed per 1000 requests
- For small demos (tens of restaurants), the cost is typically negligible

Refer to official pricing: https://developers.google.com/maps/documentation/places/web-service/usage-and-billing

## Customization

Edit `config/.env`:
```bash
# City and country
TARGET_CITY=Paris
TARGET_COUNTRY=France

# Number of restaurants (recommended: 20–60)
MAX_RESTAURANTS=30

# AWS region (if using S3)
AWS_REGION=eu-west-1
```

## Troubleshooting

### “Missing API key”
- Ensure `GOOGLE_PLACES_API_KEY` is set in `config/.env`
- Test with: `python test_google_places_api.py`

### “REQUEST_DENIED”
- Make sure the Places API is enabled in your Google Cloud project
- Check: https://console.cloud.google.com/apis/library/places-backend.googleapis.com

### AWS S3 errors
- S3 upload is optional; the script works locally without AWS
- If using S3, verify your AWS credentials and bucket name

### No restaurants found
- Check `TARGET_CITY` in `config/.env`
- Try a well-known city (e.g., “Paris” or “Lyon”)

## Project Structure

```
google-places-data-engineering/
├── src/
│   └── google_places_collector.py      # Main collector
├── config/
│   ├── config_template.txt             # Configuration template
│   └── .env                            # Your configuration (create this)
├── data/                               # Collected data
├── logs/                               # Execution logs
├── test_google_places_api.py           # Connection test script
├── requirements.txt                    # Python dependencies
└── README.md                           # This file
```

## Tips
1. Test first: `python test_google_places_api.py`
2. Start small: set `MAX_RESTAURANTS=5` for a quick run
3. Review logs: `logs/google_places_collector.log`
4. AWS is optional: local CSV output works fine

## Next Steps
- Analyze CSV files (Excel, Pandas, etc.)
- Run basic sentiment analysis on reviews
- Plot locations on a map using latitude/longitude
- Compare ratings by neighborhood or price level

## Useful Links
- Google Cloud Console: https://console.cloud.google.com/
- Places API Docs: https://developers.google.com/maps/documentation/places/web-service
- Pricing: https://developers.google.com/maps/documentation/places/web-service/usage-and-billing 