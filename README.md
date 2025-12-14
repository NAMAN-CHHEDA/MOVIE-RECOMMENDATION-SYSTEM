# End-to-End Data Engineering Pipeline for Movie Recommendation

This project implements a fully automated **ELT data engineering pipeline** for movie recommendation analytics using:

- **Snowflake** – Cloud Data Warehouse  
- **Apache Airflow** – Workflow Orchestration  
- **dbt (Data Build Tool)** – In-warehouse Transformations  
- **Preset / Apache Superset** – BI Dashboarding  
- **TMDB 10,000 Movies Dataset (CSV)** – Primary Static Source  
- **TMDB API Enrichment** – Dynamic Metadata (genres, similar movies, popularity, etc.)

The pipeline loads raw movie data, enriches it using the TMDB API, processes it through dbt models, and visualizes results in a Preset dashboard designed for movie recommendation insights.

---

## **Project Overview**

This pipeline answers:  
**“Given any movie, what are the most similar films in terms of genre, popularity, rating, and release characteristics?”**

The workflow supports:

- Data ingestion from CSV  
- API-driven data enrichment  
- Snowflake transformation using dbt  
- Airflow DAG automation  
- Analytical dashboard for recommendations

---

##  **Repository Structure**

├── dags/
│ ├── movie_tmdb_etl_pipeline.py # Airflow ETL pipeline (CSV + API)
│ ├── movie_tmdb_dbt_pipeline.py # Airflow DAG triggering dbt run/test
│
├── dbt/
│ ├── models/
│ │ ├── staging/
│ │ │ ├── stg_tmdb_movies.sql
│ │ │ ├── stg_tmdb_enriched.sql
│ │ ├── marts/
│ │ │ ├── dim_movie.sql
│ │ │ ├── features_movie_content.sql
│ │ │ ├── movie_similar_content.sql
│ ├── tests/
│ │ ├── not_null_dim_movie_id.yml
│ │ ├── unique_dim_movie_id.yml
│
├── scripts/
│ ├── tmdb_api_enrichment.py # Python script to call TMDB API
│
├── data/
│ ├── TMDB_10000_Movies_Dataset.csv # Primary dataset
│
├── README.md # Project documentation
└── requirements.txt # Python dependencies
