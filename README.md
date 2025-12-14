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

A full Mermaid architecture diagram is included in the docs folder.

---

##  **Data Sources**

### **1. Static CSV Dataset**
- `TMDB_10000_Movies_Dataset.csv`
- Contains: titles, genres, budgets, revenues, popularity, vote averages, vote counts, release year.

### **2. TMDB API Enrichment**
Used to fetch:

- Genre details  
- Spoken languages  
- Updated ratings & popularity  
- Similar movies  
- Recommendations  

The API key is stored securely in **Airflow Variables**.

---

## **Snowflake Setup**

### **Schemas used:**
- `RAW` – Raw CSV & API ingested data  
- `ANALYTICS` – dbt-transformed analytical tables  

### **Core Tables:**
- `RAW.TMDB_MOVIES` (CSV ingest)  
- `RAW.TMDB_ENRICHED` (API enrichment)  
- `ANALYTICS.DIM_MOVIE`  
- `ANALYTICS.FEATURES_MOVIE_CONTENT`  
- `ANALYTICS.MOVIE_SIMILAR_CONTENT`

---

## **Airflow Pipelines**

### **1. ETL Pipeline – `movie_tmdb_etl_pipeline`**
Tasks:
- `create_raw_objects` – Create stage + raw tables  
- `load_tmdb_movies` – Load CSV into Snowflake RAW  
- `validate_tmdb_load` – Row-count validation  
- `enrich_with_tmdb` – API calls + JSON normalization  

### **2. dbt Pipeline – `movie_tmdb_dbt_pipeline`**
Tasks:
- `dbt run`  
- `dbt test`  

Enforces clean, reliable analytical models.

---

##  **dbt Models**

### **Staging Models**
- `stg_tmdb_movies.sql` – Type cleaning, date parsing, genre extraction  
- `stg_tmdb_enriched.sql` – API metadata normalization  

### **Marts / Features**
- `dim_movie` – Movie dimension table  
- `features_movie_content` – Ratings, popularity, genres  
- `movie_similar_content` – Similar movie pairs with rating, genre, year difference  

### **Tests**
- NOT NULL movie_id  
- UNIQUE movie_id  
- NOT NULL primary genre  
- Referential integrity between similar movies  

---

##  **Preset Dashboard**

The dashboard contains:

### KPI Cards
- Total recommendation pairs  
- Distinct movies  
- Average rating  
- Average year difference  

###  Charts
- Top Recommended Movies  
- Genre Distribution (Pie Chart)  
- Popularity vs Rating Scatter Plot  
- Year Difference Histogram  
- Similar Movie Table  
- Genre Similarity Bar Chart  
- Rating by Genre  

The dashboard supports filters for:
- Movie Title  
- Genre  

---

### **3. Configure Airflow**
- Add Snowflake connection  
- Add Airflow Variables:  
  - `snowflake_account`  
  - `snowflake_database`  
  - `TMDB_API_KEY`  

### **4. Run Airflow**


### **5. Trigger ETL DAG**
- Load CSV  
- Enrich using TMDB API  

### **6. Run dbt**
dbt run
dbt test


### **7. Open Preset Dashboard**
Visualize recommendations & insights.


## Similar Movies Query


SELECT *
FROM ANALYTICS.MOVIE_SIMILAR_CONTENT
WHERE MOVIE_ID = 27205
ORDER BY VOTE_AVERAGE DESC;



## **Authors** 

Ameya Satish Khond

Prathmesh Mankar

Naman Vipul Chheda

Nikhil Kanaparthi

## **Acknowledgements**## 

TMDB for API access

Snowflake for cloud computing credits

Preset for BI environment

Professor Keeyong Han (Data Warehouse 226)






