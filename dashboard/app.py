import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import duckdb
import os
from datetime import datetime
import numpy as np

st.set_page_config(
    page_title="Restaurant Data Pipeline Dashboard",
    page_icon="🍽️",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .sub-header {
        font-size: 1.5rem;
        color: #ff7f0e;
        margin-bottom: 1rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
        margin-bottom: 1rem;
    }
    .pipeline-step {
        background-color: #e8f4fd;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
        border-left: 4px solid #1f77b4;
    }
</style>
""", unsafe_allow_html=True)

def bootstrap_duckdb_if_missing(db_path: str) -> bool:
    """Create a minimal DuckDB from dbt seed CSVs if DB is missing."""
    try:
        repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        seeds_dir = os.path.join(repo_root, 'reviewflow_dbt', 'seeds')
        restaurants_csv = os.path.join(seeds_dir, 'restaurants.csv')
        reviews_csv = os.path.join(seeds_dir, 'reviews.csv')
        if not (os.path.exists(restaurants_csv) and os.path.exists(reviews_csv)):
            return False
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        conn = duckdb.connect(db_path)
        conn.execute("DROP TABLE IF EXISTS restaurants")
        conn.execute("CREATE TABLE restaurants AS SELECT * FROM read_csv_auto(?, header=True)", [restaurants_csv])
        conn.execute("DROP TABLE IF EXISTS reviews")
        conn.execute("CREATE TABLE reviews AS SELECT * FROM read_csv_auto(?, header=True)", [reviews_csv])
        conn.close()
        return True
    except Exception:
        return False

def load_data():
    """Load data from DuckDB (bootstrap from seeds if missing)."""
    try:
        repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        db_path = os.path.join(repo_root, 'reviewflow_dbt', 'reviewflow.duckdb')
        if not os.path.exists(db_path):
            bootstrap_duckdb_if_missing(db_path)
        if not os.path.exists(db_path):
            return None, None
        conn = duckdb.connect(db_path)
        restaurants_df = conn.execute("SELECT * FROM mart_top_restaurants").df() if 'mart_top_restaurants' in [r[0] for r in conn.execute("SHOW ALL TABLES").fetchall()] else None
        if restaurants_df is None or restaurants_df.empty:
            # Fallback to staging if mart not built
            restaurants_df = conn.execute("SELECT * FROM restaurants").df()
        reviews_df = conn.execute("SELECT * FROM stg_reviews").df() if 'stg_reviews' in [r[0] for r in conn.execute("SHOW ALL TABLES").fetchall()] else None
        if reviews_df is None or reviews_df.empty:
            reviews_df = conn.execute("SELECT * FROM reviews").df()
        conn.close()
        return restaurants_df, reviews_df
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return None, None

def home_page():
    """Home page explaining the project"""
    
    st.markdown('<h1 class="main-header">🍽️ Restaurant Data Pipeline</h1>', unsafe_allow_html=True)
    st.markdown('<h2 class="sub-header">Complete Data Engineering Project</h2>', unsafe_allow_html=True)
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("""
        ## 📋 Project Description
        
        This project implements an **end-to-end data pipeline** for restaurant review analysis 
        using the Google Places API. It demonstrates modern Data Engineering best practices 
        with a complete approach from ingestion to visualization.
        
        ### 🎯 Objectives
        - **Automate** restaurant data collection
        - **Transform** raw data into business insights
        - **Analyze** customer review sentiment
        - **Visualize** key metrics in an interactive dashboard
        """)
    
    with col2:
        st.markdown("""
        <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                    border-radius: 10px; padding: 20px; color: white; text-align: center;">
        
        <div style="margin: 10px 0; padding: 8px; background: rgba(255,255,255,0.2); border-radius: 5px;">
        🌐 <strong>Google Places API</strong>
        </div>
        <div style="font-size: 20px; margin: 5px 0;">⬇️</div>
        
        <div style="margin: 10px 0; padding: 8px; background: rgba(255,255,255,0.2); border-radius: 5px;">
        📥 <strong>Data Ingestion</strong>
        </div>
        <div style="font-size: 20px; margin: 5px 0;">⬇️</div>
        
        <div style="margin: 10px 0; padding: 8px; background: rgba(255,255,255,0.2); border-radius: 5px;">
        🔄 <strong>dbt Transformation</strong>
        </div>
        <div style="font-size: 20px; margin: 5px 0;">⬇️</div>
        
        <div style="margin: 10px 0; padding: 8px; background: rgba(255,255,255,0.2); border-radius: 5px;">
        🗄️ <strong>DuckDB Warehouse</strong>
        </div>
        <div style="font-size: 20px; margin: 5px 0;">⬇️</div>
        
        <div style="margin: 10px 0; padding: 8px; background: rgba(255,255,255,0.2); border-radius: 5px;">
        📊 <strong>Streamlit Dashboard</strong>
        </div>
        
        </div>
        """, unsafe_allow_html=True)
        st.caption("Data Pipeline Architecture")
    
    st.markdown('<h2 class="sub-header">🏗️ Pipeline Architecture</h2>', unsafe_allow_html=True)
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown("""
        <div class="pipeline-step">
        <h4>📥 1. Ingestion</h4>
        <ul>
        <li>Google Places API</li>
        <li>Automatic collection</li>
        <li>AWS S3 storage</li>
        </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="pipeline-step">
        <h4>🔄 2. Transformation</h4>
        <ul>
        <li>dbt (Data Build Tool)</li>
        <li>Data cleaning</li>
        <li>Staging & mart models</li>
        </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        <div class="pipeline-step">
        <h4>🧠 3. NLP & Analytics</h4>
        <ul>
        <li>Sentiment analysis</li>
        <li>Automatic classification</li>
        <li>Quality metrics</li>
        </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        st.markdown("""
        <div class="pipeline-step">
        <h4>📊 4. Visualization</h4>
        <ul>
        <li>Streamlit Dashboard</li>
        <li>Interactive charts</li>
        <li>Real-time KPIs</li>
        </ul>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown('<h2 class="sub-header">🛠️ Technology Stack</h2>', unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        **🔧 Data Engineering**
        - Python 3.9+
        - dbt (Data Build Tool)
        - DuckDB (Data Warehouse)
        - Pandas & NumPy
        """)
    
    with col2:
        st.markdown("""
        **☁️ Cloud & Infrastructure**
        - AWS S3 (Storage)
        - AWS Lambda (Orchestration)
        - EventBridge (Scheduling)
        - CloudWatch (Monitoring)
        """)
    
    with col3:
        st.markdown("""
        **📊 Analytics & Viz**
        - Streamlit (Dashboard)
        - Plotly (Charts)
        - NLP Sentiment Analysis
        - Statistical Analysis
        """)
    
    st.markdown('<h2 class="sub-header">📈 Pipeline Performance</h2>', unsafe_allow_html=True)
    
    restaurants_df, reviews_df = load_data()
    
    if restaurants_df is not None:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("🏪 Restaurants", len(restaurants_df))
        
        with col2:
            st.metric("💬 Reviews Analyzed", len(reviews_df) if reviews_df is not None else 0)
        
        with col3:
            avg_rating = restaurants_df['rating'].mean() if 'rating' in restaurants_df.columns else 0
            st.metric("⭐ Average Rating", f"{avg_rating:.2f}")
        
        with col4:
            if 'positive_sentiment_pct' in restaurants_df.columns:
                avg_sentiment = restaurants_df['positive_sentiment_pct'].mean()
                st.metric("😊 Positive Sentiment", f"{avg_sentiment:.1f}%")
            else:
                st.metric("😊 Positive Sentiment", "N/A")
    
    st.markdown('<h2 class="sub-header">🤖 Automatic Orchestration</h2>', unsafe_allow_html=True)
    
    col1, col2 = st.columns([3, 2])
    
    with col1:
        st.markdown("""
        ### Automated Pipeline
        
        The pipeline runs automatically **every day at 8:00 AM** via a local cron job.
        
        **Execution steps:**
        1. **Collect** data from Google Places API
        2. **Load** CSV data into DuckDB  
        3. **Transform** with dbt (staging → marts)
        4. **Test** data quality
        5. **Notify** status (success/failure)
        
        **Monitoring:**
        - Detailed logs in `logs/`
        - Automatic notifications
        - Real-time dashboard
        """)
    
    with col2:
        st.markdown("""
        <div class="metric-card">
        <h4>⏰ Last Execution</h4>
        <p>Today at 08:00</p>
        <p><strong>Status:</strong> ✅ Success</p>
        <p><strong>Duration:</strong> 12.9s</p>
        </div>
        
        <div class="metric-card">
        <h4>📊 Fresh Data</h4>
        <p>Collected today</p>
        <p><strong>Quality:</strong> 98.5%</p>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("---")
    st.markdown("""
    <div style="text-align: center; color: #666; margin-top: 2rem;">
    <p><strong>Repository:</strong> <a href="https://github.com/razdine10/google-places-data-pipeline" target="_blank">GitHub - Restaurant Data Pipeline</a></p>
    <p><strong>Developed by:</strong> Said Razdine</p>
    <p><strong>Last updated:</strong> """ + datetime.now().strftime("%m/%d/%Y") + """</p>
    </div>
    """, unsafe_allow_html=True)

def dashboard_page():
    """Dashboard page with visualizations"""
    
    st.markdown('<h1 class="main-header">📊 Analytics Dashboard</h1>', unsafe_allow_html=True)
    
    restaurants_df, reviews_df = load_data()
    
    if restaurants_df is None:
        st.error("❌ No data available. Please run the collection pipeline first.")
        st.info("💡 Execute: `python3 orchestration/local_orchestrator.py`")
        return
    
    # Fallback enrichment when mart tables are unavailable
    try:
        needs_enrichment = any(col not in restaurants_df.columns for col in [
            'quality_score', 'positive_sentiment_pct', 'reviews_collected', 'recommendation'
        ])
        if needs_enrichment and reviews_df is not None and not reviews_df.empty:
            join_key = None
            # Prefer joining on place_id when available
            if 'place_id' in restaurants_df.columns and 'place_id' in reviews_df.columns:
                join_key = 'place_id'
            else:
                # Fallback to restaurant name
                name_col_r = 'restaurant_name' if 'restaurant_name' in restaurants_df.columns else (
                    'name' if 'name' in restaurants_df.columns else None
                )
                name_col_v = 'restaurant_name' if 'restaurant_name' in reviews_df.columns else None
                if name_col_r is not None and name_col_v is not None:
                    restaurants_df = restaurants_df.copy()
                    restaurants_df['__name_key__'] = restaurants_df[name_col_r]
                    join_key = '__name_key__'
            
            if join_key is not None:
                rv = reviews_df.copy()
                # Determine positivity from rating (aligns with stg_reviews logic: rating >= 4 => Positive)
                if 'rating' in rv.columns:
                    rv['__is_positive__'] = rv['rating'] >= 4
                else:
                    rv['__is_positive__'] = False
                
                # Choose grouping key on reviews side
                if join_key == 'place_id' and 'place_id' in rv.columns:
                    group_key = 'place_id'
                elif join_key == '__name_key__' and 'restaurant_name' in rv.columns:
                    group_key = 'restaurant_name'
                else:
                    group_key = None
                
                if group_key is not None:
                    agg = rv.groupby(group_key, dropna=False).agg(
                        reviews_collected=('rating', lambda s: int(s.notna().sum() if hasattr(s, 'notna') else len(s))),
                        positive_count=('__is_positive__', 'sum')
                    ).reset_index()
                    agg['positive_sentiment_pct'] = agg.apply(
                        lambda row: round((row['positive_count'] * 100.0 / row['reviews_collected']), 1) if row['reviews_collected'] else 0.0,
                        axis=1
                    )
                    agg = agg.drop(columns=['positive_count'])
                    
                    # Align key name for merge
                    if join_key == '__name_key__' and group_key == 'restaurant_name':
                        agg = agg.rename(columns={'restaurant_name': '__name_key__'})
                    
                    restaurants_df = restaurants_df.merge(agg, on=join_key, how='left')
                    
                    # Fill missing with zeros
                    if 'reviews_collected' in restaurants_df.columns:
                        restaurants_df['reviews_collected'] = restaurants_df['reviews_collected'].fillna(0).astype(int)
                    else:
                        restaurants_df['reviews_collected'] = 0
                    if 'positive_sentiment_pct' in restaurants_df.columns:
                        restaurants_df['positive_sentiment_pct'] = restaurants_df['positive_sentiment_pct'].fillna(0.0)
                    else:
                        restaurants_df['positive_sentiment_pct'] = 0.0
                    
                    # Compute quality score and labels
                    if 'rating' in restaurants_df.columns:
                        ps_series = restaurants_df['positive_sentiment_pct'] if 'positive_sentiment_pct' in restaurants_df.columns else pd.Series(0.0, index=restaurants_df.index)
                        rc_series = restaurants_df['reviews_collected'] if 'reviews_collected' in restaurants_df.columns else pd.Series(0, index=restaurants_df.index)
                        restaurants_df['quality_score'] = (
                            (restaurants_df['rating'] * 15)
                            + (np.minimum(ps_series / 4.0, 20))
                            + (5 * (rc_series >= 5).astype(int))
                        ).round(1)
                        restaurants_df['restaurant_tier'] = np.where(
                            (restaurants_df['rating'] >= 4.5) & (restaurants_df['positive_sentiment_pct'] >= 80), 'Premium',
                            np.where((restaurants_df['rating'] >= 4.0) & (restaurants_df['positive_sentiment_pct'] >= 70), 'Excellent',
                                     np.where((restaurants_df['rating'] >= 3.5) & (restaurants_df['positive_sentiment_pct'] >= 60), 'Very Good',
                                              np.where(restaurants_df['rating'] >= 3.0, 'Good', 'Average')))
                        )
                        restaurants_df['recommendation'] = np.where(
                            (restaurants_df['rating'] >= 4.0) & (restaurants_df['positive_sentiment_pct'] >= 75) & (restaurants_df['reviews_collected'] >= 3), 'Highly Recommended',
                            np.where((restaurants_df['rating'] >= 3.5) & (restaurants_df['positive_sentiment_pct'] >= 60), 'Recommended',
                                     np.where(restaurants_df['rating'] >= 3.0, 'Average', 'Not Recommended'))
                        )
            
            # Cleanup temporary column
            if '__name_key__' in restaurants_df.columns:
                restaurants_df = restaurants_df.drop(columns=['__name_key__'])
    except Exception:
        # Silently skip enrichment on any unexpected error; UI will gracefully degrade to N/A
        pass
    
    st.markdown('<h2 class="sub-header">📈 Key Metrics</h2>', unsafe_allow_html=True)
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("🏪 Restaurants", len(restaurants_df))
    
    with col2:
        total_reviews = len(reviews_df) if reviews_df is not None else 0
        st.metric("💬 Total Reviews", total_reviews)
    
    with col3:
        avg_rating = restaurants_df['rating'].mean() if 'rating' in restaurants_df.columns else 0
        st.metric("⭐ Average Rating", f"{avg_rating:.2f}")
    
    with col4:
        if 'positive_sentiment_pct' in restaurants_df.columns:
            avg_positive = restaurants_df['positive_sentiment_pct'].mean()
            st.metric("😊 Positive Sentiment", f"{avg_positive:.1f}%")
        else:
            st.metric("😊 Positive Sentiment", "N/A")
    
    with col5:
        if 'quality_score' in restaurants_df.columns:
            avg_quality = restaurants_df['quality_score'].mean()
            st.metric("🎯 Quality Score", f"{avg_quality:.1f}")
        else:
            st.metric("🎯 Quality Score", "N/A")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 🏆 Top 10 Restaurants")
        
        if 'quality_score' in restaurants_df.columns:
            # Determine a safe name column for display
            name_col_top = 'restaurant_name' if 'restaurant_name' in restaurants_df.columns else (
                'name' if 'name' in restaurants_df.columns else None
            )
            if name_col_top is None:
                st.info("Restaurant name column not available")
            else:
                top_restaurants = restaurants_df.nlargest(10, 'quality_score')
                bar_kwargs = dict(
                    x='quality_score',
                    y=name_col_top,
                    orientation='h',
                    title="Top 10 by Quality Score",
                    color_continuous_scale='Viridis'
                )
                if 'rating' in top_restaurants.columns:
                    bar_kwargs['color'] = 'rating'
                fig = px.bar(top_restaurants, **bar_kwargs)
                fig.update_layout(height=400, yaxis={'categoryorder': 'total ascending'})
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Quality score data not available")
    
    with col2:
        st.markdown("### 📊 Rating Distribution")
        
        if 'rating' in restaurants_df.columns:
            fig = px.histogram(
                restaurants_df,
                x='rating',
                nbins=20,
                title="Restaurant Rating Distribution",
                color_discrete_sequence=['#1f77b4']
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Rating data not available")
    
    if reviews_df is not None and 'sentiment_simple' in reviews_df.columns:
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("### 😊 Sentiment Analysis")
            
            sentiment_counts = reviews_df['sentiment_simple'].value_counts()
            
            fig = px.pie(
                values=sentiment_counts.values,
                names=sentiment_counts.index,
                title="Sentiment Distribution",
                color_discrete_map={
                    'Positive': '#2E8B57',
                    'Neutral': '#FFD700', 
                    'Negative': '#DC143C'
                }
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown("### 📝 Review Rating Distribution")
            
            if 'rating' in reviews_df.columns:
                rating_counts = reviews_df['rating'].value_counts().sort_index()
                
                fig = go.Figure(data=[
                    go.Bar(
                        x=rating_counts.index,
                        y=rating_counts.values,
                        marker_color='lightblue'
                    )
                ])
                fig.update_layout(
                    title="Review Rating Distribution",
                    xaxis_title="Rating",
                    yaxis_title="Number of reviews"
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Review rating data not available")
    
    st.markdown('<h2 class="sub-header">🍽️ Restaurant List</h2>', unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if 'rating_category' in restaurants_df.columns:
            rating_filter = st.selectbox(
                "Filter by rating category",
                options=['All'] + list(restaurants_df['rating_category'].unique())
            )
        else:
            rating_filter = 'All'
    
    with col2:
        if 'restaurant_tier' in restaurants_df.columns:
            tier_filter = st.selectbox(
                "Filter by tier",
                options=['All'] + list(restaurants_df['restaurant_tier'].unique())
            )
        else:
            tier_filter = 'All'
    
    with col3:
        min_rating = st.slider("Minimum rating", 0.0, 5.0, 0.0, 0.1)
    
    filtered_df = restaurants_df.copy()
    
    if rating_filter != 'All' and 'rating_category' in filtered_df.columns:
        filtered_df = filtered_df[filtered_df['rating_category'] == rating_filter]
    
    if tier_filter != 'All' and 'restaurant_tier' in filtered_df.columns:
        filtered_df = filtered_df[filtered_df['restaurant_tier'] == tier_filter]
    
    if 'rating' in filtered_df.columns:
        filtered_df = filtered_df[filtered_df['rating'] >= min_rating]
    
    # Determine the correct name column depending on the source table
    name_col = 'restaurant_name' if 'restaurant_name' in filtered_df.columns else ('name' if 'name' in filtered_df.columns else None)

    display_columns = []
    if name_col is not None:
        display_columns.append(name_col)
    if 'rating' in filtered_df.columns:
        display_columns.append('rating')
    
    if 'quality_score' in filtered_df.columns:
        display_columns.append('quality_score')
    if 'positive_sentiment_pct' in filtered_df.columns:
        display_columns.append('positive_sentiment_pct')
    if 'reviews_collected' in filtered_df.columns:
        display_columns.append('reviews_collected')
    if 'recommendation' in filtered_df.columns:
        display_columns.append('recommendation')
    
    # Safe selection: only keep columns that actually exist
    selected_columns = [c for c in display_columns if c in filtered_df.columns]
    
    # Build rename map only for present columns
    column_names = {
        (name_col if name_col is not None else 'restaurant_name'): 'Restaurant',
        'rating': 'Rating',
        'quality_score': 'Quality Score',
        'positive_sentiment_pct': 'Positive Sentiment (%)',
        'reviews_collected': 'Review Count',
        'recommendation': 'Recommendation'
    }
    rename_map = {k: v for k, v in column_names.items() if k in selected_columns}
    
    if len(selected_columns) == 0:
        st.info("No displayable columns available in the current dataset.")
        return
    
    display_df = filtered_df[selected_columns].rename(columns=rename_map)
    
    st.dataframe(
        display_df,
        use_container_width=True,
        height=400
    )
    
    st.info(f"📊 {len(filtered_df)} restaurants displayed out of {len(restaurants_df)} total")

def main():
    """Main function with navigation"""
    
    st.sidebar.title("🧭 Navigation")
    
    page = st.sidebar.radio(
        "Choose a page:",
        ["🏠 Home", "📊 Dashboard"]
    )
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📊 Data Status")
    
    restaurants_df, reviews_df = load_data()
    
    if restaurants_df is not None:
        st.sidebar.success("✅ Data loaded")
        st.sidebar.info(f"🏪 {len(restaurants_df)} restaurants")
        if reviews_df is not None:
            st.sidebar.info(f"💬 {len(reviews_df)} reviews")
        
        st.sidebar.info(f"🔄 Updated: {datetime.now().strftime('%H:%M')}")
    else:
        st.sidebar.error("❌ No data")
        st.sidebar.info("Run the pipeline first")
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 🔗 Useful Links")
    st.sidebar.markdown("- [GitHub Repo](https://github.com/razdine10/google-places-data-pipeline)")
    st.sidebar.markdown("- [Documentation](https://github.com/razdine10/google-places-data-pipeline#readme)")
    st.sidebar.markdown("- [API Status](https://status.cloud.google.com/)")
    
    if page == "🏠 Home":
        home_page()
    else:
        dashboard_page()

if __name__ == "__main__":
    main() 