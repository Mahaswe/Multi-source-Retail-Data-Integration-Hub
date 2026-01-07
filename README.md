# Multi-Source Retail Data Integration Hub

## Overview
This project implements a **complete ETL (Extract–Transform–Load) pipeline** using **PySpark** to integrate and analyze **retail sales data from multiple sources**, including **offline store data and online sales data**.

The pipeline transforms raw, inconsistent datasets into **clean, standardized, analytics-ready data** and generates actionable insights to support **data-driven decision-making** in marketing, inventory management, and revenue optimization.

---

## Objectives
- Integrate **offline and online retail sales data**
- Perform **data cleaning, standardization, and transformation** using PySpark
- Identify **monthly, category-wise, and region-wise sales trends**
- Compare **online vs offline revenue performance**
- Analyze **customer payment preferences**
- Generate **visual insights** for business interpretation

---

## Tools & Technologies
- **Python**
- **PySpark (Spark DataFrames)**
- **Pandas**
- **Matplotlib**
- **PyCharm IDE**
- **CSV / Excel data sources**

---

---

## Folder & File Explanation

### `data/` — Raw Input Data
Contains the **source datasets** used for the ETL pipeline:
- **Offline retail sales data**
- **Online sales transaction data**

These files represent the **raw layer** before any processing.

---

### `Analysis/` — ETL Logic & Intermediate Data
Contains the **core processing scripts**:

- **cleaning.py**
  - Removes duplicate records
  - Handles missing values
  - Standardizes column names (e.g., `Sales_Amount → revenue`, `Quantity_Sold → units_sold`)
  - Cleans categorical values (payment methods, regions)

- **transformation.py**
  - Revenue calculations
  - Date parsing and **month extraction**
  - Category-wise and region-wise aggregations
  - Online vs offline comparisons

- **visualization.py**
  - Generates charts for:
    - Category-wise revenue
    - Month-wise trends
    - Online vs offline revenue
    - Payment method preferences
    - Region-wise revenue

- **cleaned_sales.csv**, **cleaned_online_sales.csv**
  - Intermediate cleaned datasets produced after transformation

---

### `Output/` — Final Results
Contains **final analytical outputs**:
- Cleaned and combined CSV datasets
- Business insight visualizations:
  - Category-wise Revenue
  - Month-wise Category Revenue
  - Online vs Offline Revenue
  - Payment Method Preference
  - Region-wise Revenue

These outputs represent the **final results** of the ETL pipeline.

---

### `final_documentation/`
Contains the **detailed project report (PDF)** explaining:
- Problem statement
- ETL design
- Data transformations
- Insights and conclusions

---

## ETL Process

### Extract
- Loaded raw CSV/Excel files into **PySpark DataFrames**
- Inferred schema for structured processing

### Transform
- Removed duplicates
- Handled missing values
- Standardized column names and formats
- Cleaned payment methods (UPI, Card, Cash)
- Grouped regions into global categories
- Extracted month from date for trend analysis

### Load
- Saved cleaned datasets for analysis
- Generated aggregated datasets and visualizations

---

## Insights Generated
- **Clothing and Electronics** are top-performing categories
- **North America** is the highest revenue-generating region
- **UPI** is the most preferred payment method
- **Offline sales outperform online sales**
- Strong correlation between **units sold and total revenue**

---

## Conclusion
This project demonstrates how **PySpark can efficiently integrate and process multi-source retail data** using a structured ETL approach.

By converting raw data into clean, analytics-ready datasets and meaningful visual insights, the pipeline helps businesses improve **strategic planning, operational efficiency, and revenue growth**.

---

## Author
**Mahaswetha A S**  
Aspiring Data Engineer
