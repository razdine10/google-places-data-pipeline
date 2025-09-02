#!/bin/bash

# Script to launch the Streamlit dashboard
# Restaurant Data Pipeline Dashboard

echo "Starting Restaurant Data Pipeline Dashboard"
echo "=========================================="

# Check if Streamlit is installed
if ! command -v streamlit &> /dev/null; then
    echo "Warning: Streamlit not found. Installing dependencies..."
    pip install -r requirements.txt
fi

# Check if data exists
DB_PATH="../reviewflow_dbt/reviewflow.duckdb"
if [ ! -f "$DB_PATH" ]; then
    echo "Error: DuckDB database not found: $DB_PATH"
    echo "Please run the pipeline first:"
    echo "   cd ../orchestration"
    echo "   python3 local_orchestrator.py"
    exit 1
fi

echo "Database found successfully"
echo "Launching dashboard at http://localhost:8501"
echo ""
echo "Available pages:"
echo "   - Home: Data Engineering project overview"
echo "   - Dashboard: Visualizations and analytics"
echo ""
echo "To stop: Press Ctrl+C"
echo ""

# Launch Streamlit
streamlit run app.py --server.port 8501 --server.address localhost 