# Streamlit Dashboard - Restaurant Data Pipeline

## Description

Interactive dashboard for visualizing restaurant pipeline data. Showcases the complete Data Engineering project with real-time analytics.

## Available Pages

### 1. Home Page
- Data Engineering project overview
- Pipeline architecture (Ingestion → Transformation → Analytics)
- Technology stack details
- Real-time performance metrics
- Automated orchestration and monitoring

### 2. Dashboard Page
- Key performance indicators: Restaurant count, reviews, average ratings
- Interactive visualizations:
  - Top 10 restaurants by quality score
  - Rating distribution
  - Sentiment analysis (Positive/Neutral/Negative)
  - Review length analysis
- Dynamic filters by category, tier, minimum rating
- Interactive restaurant table with sorting and search

## Quick Start

### Option 1: Automated Script
```bash
./run_dashboard.sh
```

### Option 2: Manual Setup
```bash
# Install dependencies
pip install -r requirements.txt

# Launch dashboard
streamlit run app.py
```

## Access

Once launched, the dashboard is available at:
**http://localhost:8501**

## Prerequisites

### Required Data
The dashboard requires the pipeline to have been executed at least once:

```bash
# From project root
cd orchestration
python3 local_orchestrator.py
```

This generates the DuckDB database: `reviewflow_dbt/reviewflow.duckdb`

### Python Dependencies
- `streamlit >= 1.28.0` - Dashboard framework
- `plotly >= 5.15.0` - Interactive charts
- `pandas >= 2.0.3` - Data manipulation
- `duckdb >= 0.8.1` - Analytics database
- `numpy >= 1.24.0` - Numerical computations

## Features

### Interface
- Responsive design for mobile/desktop
- Intuitive sidebar navigation
- Custom CSS with consistent theme
- Clean icons and colors for optimal UX

### Analytics
- Real-time metrics from DuckDB
- Interactive charts with Plotly
- Multi-criteria dynamic filters
- Data export capabilities

### Data Management
- Automatic DuckDB connection
- Robust error handling
- Data freshness indicators
- Pipeline status in sidebar

## Technical Architecture

```
app.py
├── load_data() → DuckDB Connection
├── home_page() → Project Presentation
├── dashboard_page() → Data Visualization
└── main() → Navigation & Sidebar
```

### Data Structure
```sql
-- Tables used
mart_top_restaurants  -- Aggregated KPIs and scores
stg_reviews          -- Reviews with sentiment analysis
restaurants          -- Raw collected data
```

## Troubleshooting

### Issue: "No data available"
**Solution:**
```bash
cd orchestration
python3 local_orchestrator.py
```

### Issue: "Module streamlit not found"
**Solution:**
```bash
pip install -r requirements.txt
```

### Issue: "Database not found"
**Check:**
- The file `reviewflow_dbt/reviewflow.duckdb` exists
- The pipeline executed successfully
- Read permissions on the file

## Screenshots

### Home Page
- Professional project presentation
- 4-step pipeline architecture
- Detailed technology stack
- Performance metrics

### Dashboard Page
- 5 main KPIs in header
- 4 interactive charts
- Dynamic filters and table
- Sidebar with data status

## Integration

The dashboard integrates seamlessly with:
- Orchestration pipeline (automatic data)
- dbt transformations (optimized tables)
- NLP analysis (pre-calculated sentiments)
- Monitoring (logs and notifications)

---

