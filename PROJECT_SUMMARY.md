# Restaurant Data Pipeline – End-to-End Project

## Overview

This repository contains an end-to-end data engineering project for analyzing restaurant reviews. It covers automated data ingestion, dbt transformations, lightweight NLP sentiment analysis, orchestration, and an interactive Streamlit dashboard.

## Completed Components

### 1. Data Ingestion
- Sources: Google Places API (primary)
- Collectors: `src/google_places_collector.py`
- Storage: Local CSV files; optional AWS S3
- Configuration: `config/.env` for API keys and parameters

### 2. dbt Transformations
- dbt project: `reviewflow_dbt/`
- Staging models: `stg_restaurants.sql`, `stg_reviews.sql`
- Mart model: `mart_top_restaurants.sql`
- Data quality tests: 30+ automated checks
- Warehouse: Local DuckDB

### 3. NLP & Sentiment
- Sentiment categories: Positive / Neutral / Negative (rule-based)
- Content features: length, keywords, basic topic flags
- Quality scoring: composite score using rating, sentiment, and volume
- Recommendations: simple tiering based on metrics

### 4. Orchestration
- Local orchestrator: `orchestration/local_orchestrator.py`
- Pipeline: Ingestion → dbt → Tests → Notifications
- Scheduling: optional cron (daily at 08:00)
- Monitoring: logs and optional notifications

### 5. Streamlit Dashboard
- Two pages: Home and Analytics
- Visualizations built with Plotly
- Real-time KPIs from DuckDB
- Dynamic filters and responsive layout

## Architecture

```
Restaurant Data Pipeline
├── Ingestion
│   └── Google Places API → CSV → (optional) S3
│
├── Transformation (dbt)
│   ├── Raw → Staging
│   ├── Staging → Marts
│   └── Data Quality Tests
│
├── Analytics & NLP
│   ├── Sentiment Analysis
│   ├── Content Features
│   └── Quality Scoring
│
├── Orchestration
│   ├── Local Runner
│   ├── Cron Scheduling
│   └── Notifications
│
└── Visualization
    ├── Streamlit Dashboard
    ├── Interactive Charts
    └── KPIs
```

## Technology Stack

| Area | Technologies |
|------|--------------|
| Data Engineering | Python 3.9+, dbt, DuckDB, Pandas |
| APIs | Google Places API |
| Cloud (optional) | AWS S3, AWS Lambda |
| Orchestration | Cron, Bash, Python |
| Analytics | Rule-based NLP, statistics, Plotly |
| Visualization | Streamlit, Plotly, CSS/HTML |
| Testing | dbt tests, data quality checks |

## How to Use

### 1) Initial Setup
```bash
# Clone and configure
git clone <repository>
cd restaurant-data-pipeline

# Configure APIs
cp config/config_template.txt config/.env
# Edit config/.env with your API keys
```

### 2) Install
```bash
# Core dependencies
pip install -r requirements.txt

# dbt
pip install dbt-core dbt-duckdb

# Dashboard
cd dashboard && pip install -r requirements.txt
```

### 3) Run the Pipeline
```bash
# Full local pipeline
dcd orchestration
python3 local_orchestrator.py

# Or step-by-step
python3 src/google_places_collector.py   # Ingestion
cd reviewflow_dbt && dbt run             # Transformation
cd ../dashboard && streamlit run app.py  # Dashboard
```

### 4) Optional Scheduling
```bash
cd orchestration
./setup_local_cron.sh
```

### 5) Dashboard
```bash
cd dashboard
./run_dashboard.sh
# Open http://localhost:8501
```

## Results and Metrics

### Pipeline Performance (typical local run)
- Ingestion: ~20 restaurants and ~100 reviews in a few seconds
- Transformation: 3 dbt models in under 10 seconds
- Data quality: 30+ automated checks
- Dashboard: real-time via DuckDB

### Available Analytics
- Restaurant metrics: quality score, recommendation tier
- Sentiment: distribution of Positive / Neutral / Negative
- Content: review length, keywords, simple topics
- Geography: coordinates and addresses

### Automation
- Frequency: daily at 08:00 (optional cron)
- Monitoring: logs and optional notifications
- Local cost: none

## Project Structure

```
restaurant-data-pipeline/
├── src/                          # Data collectors
│   └── google_places_collector.py
├── config/                       # Configuration
│   ├── .env
│   └── config_template.txt
├── data/                         # Collected data (CSV)
├── reviewflow_dbt/               # dbt project
│   ├── models/
│   │   ├── staging/
│   │   └── marts/
│   ├── tests/
│   └── profiles.yml
├── orchestration/                # Local automation
│   ├── local_orchestrator.py
│   ├── setup_local_cron.sh
│   └── README.md
├── dashboard/                    # Streamlit app
│   ├── app.py
│   ├── run_dashboard.sh
│   └── requirements.txt
└── logs/                         # Runtime logs
```

## Key Features

### Data Engineering
- Multi-source ingestion ready (Google Places; other sources pluggable)
- Raw storage (CSV and optional S3)
- dbt transformations (staging → marts)
- Automated data quality tests
- Orchestrated and automated pipeline

### Analytics & NLP
- Review sentiment analysis (rule-based)
- Content features and basic topic flags
- Composite quality scoring
- Business-oriented recommendations

### Visualization & UX
- Interactive Streamlit dashboard
- Plotly charts and real-time KPIs
- Responsive layout and clear navigation

### Automation
- Fully runnable locally
- Optional daily cron scheduling
- Logging and notifications

## Strengths

### Completeness
- End-to-end: from APIs to a usable dashboard
- Production-minded: tests, logging, orchestration
- Modular and extensible design

### Technical
- Solid practices: dbt, testing, logging
- Good performance with DuckDB
- Maintainable code and documentation

### Business Value
- Actionable insights: recommendations and quality scoring
- Sentiment trends to understand customer perception
- Repeatable process for new geographies or sources

## Roadmap

### Potential Improvements
- Advanced ML for sentiment and topic modeling
- Near real-time streaming (e.g., Kafka)
- Full cloud deployment on AWS/GCP
- Public APIs for downstream apps

### Scalability
- Multi-city expansion
- Additional data sources
- Big data processing with Spark if needed

---

This project demonstrates practical, modern data engineering: robust architecture, automation, clear analytics, and a usable dashboard. It is suitable for a technical walkthrough or a professional portfolio. 