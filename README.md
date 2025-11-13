<!-- PROJECT BANNER -->
<p align="center">
  <img src="https://img.shields.io/badge/NHA--160-Movies%20Data%20Warehouse-181717?style=for-the-badge&logo=github" alt="Project Banner"/>
</p>

<h1 align="center">🎬 Movies Data Warehouse & Analytics</h1>

<p align="center">
  <i>A high-end, production-grade data engineering project built with ETL pipelines, SQL Star Schema, and advanced analytics.</i>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/License-MIT-green?style=flat-square"/>
  <img src="https://img.shields.io/badge/Python-3.x-blue?style=flat-square"/>
  <img src="https://img.shields.io/badge/Jupyter-Notebook-orange?style=flat-square"/>
  <img src="https://img.shields.io/badge/Data%20Warehouse-Star%20Schema-purple?style=flat-square"/>
</p>

---

## 📑 Table of Contents
- [📌 Overview](#-overview)
- [📂 Repository Structure](#-repository-structure)
- [🚀 Features](#-features)
- [🧰 Tech Stack](#-tech-stack)
- [⚙️ Setup & Installation](#️-setup--installation)
- [🔄 ETL Pipeline](#-etl-pipeline)
- [🧱 Data Warehouse Schema](#-data-warehouse-schema)
- [📚 SQL Analytics Examples](#-sql-analytics-examples)
- [📈 Visual Insights](#-visual-insights)
- [👥 Team](#-team)
- [📄 License](#-license)
- [📬 Contact](#-contact)

---

## 📌 Overview
NHA-160 is a **professional, end-to-end data engineering project** that transforms raw movie datasets into a fully structured **Data Warehouse** designed for analytical processing.

The project demonstrates real-world practices in:

- Data Cleaning  
- ETL Pipelines  
- Data Modeling (Star Schema)  
- SQL Fact/Dimension structure  
- Exploratory Data Analysis (EDA)  
- Analytical Dashboards  

It reflects industry-level workflows used in BI, analytics, and data engineering teams.

---

## 📂 Repository Structure
NHA-160/
├── data/ # Raw & processed datasets
│ ├── raw/ # Original collected data
│ └── processed/ # Cleaned + transformed data
│
├── ETL/ # Extract, Transform, Load notebooks
│ ├── 01_extract.ipynb
│ ├── 02_clean_transform.ipynb
│ ├── 03_enrich.ipynb
│ └── 04_load_to_dw.ipynb
│
├── EDA/ # Exploratory Data Analysis
│ ├── countries_analysis.ipynb
│ ├── languages_analysis.ipynb
│ ├── release_trends.ipynb
│ └── title_frequency.ipynb
│
├── SQL/ # Star schema DDL + analytical queries
│ ├── create_tables.sql
│ ├── insert_data.sql
│ └── analysis_queries.sql
│
├── figures/ # Exported charts & visuals
│
├── requirements.txt # Python dependencies
├── main.ipynb # Summary notebook
└── project.ipynb # Final report notebook


---

## 🚀 Features
### ✔ Full ETL Pipeline  
Clean, normalize, enrich, and load movie metadata into a warehouse.

### ✔ Star Schema Data Warehouse  
Professional BI-grade schema including Fact & Dimension tables.

### ✔ Deep Exploratory Data Analysis (EDA)  
Insights including:
- Top countries  
- Top languages  
- Release year trends  
- Common movie titles  
- Revenue vs budget patterns  

### ✔ Ready for Dashboards  
Clean, aggregated tables suitable for Power BI / Tableau / Looker.

---

## 🧰 Tech Stack
### **Languages**
- Python 3.x  
- SQL (SQL Server / PostgreSQL)

### **Python Libraries**
- pandas  
- numpy  
- matplotlib  
- seaborn  
- sqlalchemy  

### **Tools**
- Jupyter Notebook  
- Git & GitHub  
- Database Engine (PostgreSQL/MSSQL)

---

## ⚙️ Setup & Installation
```bash
git clone https://github.com/nhahub/NHA-160.git
cd NHA-160

python -m venv .venv
source .venv/bin/activate     # macOS/Linux
# .venv\Scripts\activate      # Windows

pip install -r requirements.txt
jupyter notebook


🔄 ETL Pipeline
1. Extract

Import raw CSV files

Optional: web scraping for extended metadata

2. Transform

Clean lists (origin_country, spoken_languages)

Convert codes → full names

Parse date formats

Normalize numerics (budget/revenue)

Remove duplicates

Fix missing values

3. Load

Create dimension tables

Build fact_movies table

Add indexing & relationships

Prepare final analytical layer

🧱 Data Warehouse Schema

               dim_country
                    │
                    │
dim_language ─── fact_movies ─── dim_movie
                    │
                    │
                 dim_date

📚 SQL Analytics Examples
⭐ Top Countries

SELECT c.country_name, COUNT(*) AS total_movies
FROM fact_movies f
JOIN dim_country c ON f.country_id = c.country_id
GROUP BY c.country_name
ORDER BY total_movies DESC;

⭐ Highest Profit Movies
SELECT title, budget, revenue, (revenue - budget) AS profit
FROM fact_movies
ORDER BY profit DESC;

⭐ Movies Per Year
SELECT d.year, COUNT(*) AS movie_count
FROM fact_movies f
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY d.year
ORDER BY d.year;

📈 Visual Insights

The figures/ directory contains:

Country distribution plots

Language usage charts

Release timeline graphs

Title frequency visualizations

Revenue & budget comparisons

All visuals are reproducible from EDA notebooks.

👥 Team

Mustafa Sayed Saeed

Omar

Omar

Fady

Esraa

Amal

📄 License

This project is licensed under the MIT License.




