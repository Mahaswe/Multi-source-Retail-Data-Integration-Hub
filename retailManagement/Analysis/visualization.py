import sys
import os

# === Environment Setup ===
os.environ["JAVA_HOME"] = "C:/Program Files/Java/jdk-21"
os.environ["SPARK_HOME"] = "C:/Users/mahas/OneDrive/Desktop/python project/Revature training/retailManagement/.venv/Lib/site-packages/pyspark"
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("RetailVisualization").getOrCreate()

# Load cleaned datasets
sales = spark.read.csv("cleaned_sales.csv", header=True, inferSchema=True)
online_sales = spark.read.csv("cleaned_online_sales.csv", header=True, inferSchema=True)

print("Cleaned files loaded successfully!")

# Month-wise Category Revenue (Line Chart)
from pyspark.sql.functions import sum as spark_sum
month_category = sales.groupBy("month", "product_category") \
.agg(spark_sum("revenue").alias("total_revenue"))
pdf = month_category.toPandas()


plt.figure(figsize=(10,5))
for cat in pdf['product_category'].unique():
    d = pdf[pdf['product_category'] == cat]
    plt.plot(d['month'], d['total_revenue'], marker='o', label=cat)

plt.title("Month-wise Category Revenue")
plt.xlabel("Month")
plt.ylabel("Revenue")
plt.legend()
plt.show()

#Region-wise Revenue (Bar Chart)
region_revenue = sales.groupBy("region_group") .agg(spark_sum("revenue").alias("total_revenue"))
pdf = region_revenue.toPandas()

plt.figure(figsize=(8,5))
plt.bar(pdf['region_group'], pdf['total_revenue'])
plt.title("Region-wise Revenue")
plt.xlabel("Region")
plt.ylabel("Total Revenue")
plt.show()

#Category-wise Revenue (Bar Chart)
category_rev = sales.groupBy("product_category").agg(spark_sum("revenue").alias("total_revenue"))

pdf = category_rev.toPandas()

plt.figure(figsize=(10,6))
plt.bar(pdf['product_category'], pdf['total_revenue'])
plt.title("Category-wise Revenue")
plt.xlabel("Product Category")
plt.ylabel("Total Revenue")
plt.xticks(rotation=45)
plt.grid(axis='y', linestyle='--', alpha=0.5)
plt.show()
# 4. Payment Method Preference (Pie Chart)
# ------------------------------------------------
payment_pref = sales.groupBy("payment_method") \
.agg(spark_sum("revenue").alias("total_revenue"))


pdf = payment_pref.toPandas()


plt.figure(figsize=(7,7))
plt.pie(pdf['total_revenue'], labels=pdf['payment_method'], autopct='%1.1f%%')
plt.title("Payment Method Preference")
plt.show()

# 5. Online vs Offline Revenue (Bar Chart)
# ------------------------------------------------
offline_rev = sales.agg(spark_sum("revenue").alias("revenue")).toPandas()
offline_rev['channel'] = 'offline'


online_rev = online_sales.agg(spark_sum("total_revenue").alias("revenue")).toPandas()
online_rev['channel'] = 'online'


combined = pd.concat([offline_rev, online_rev])


plt.figure(figsize=(6,4))
plt.bar(combined['channel'], combined['revenue'])
plt.title("Online vs Offline Revenue")
plt.xlabel("Channel")
plt.ylabel("Revenue")
plt.show()


# Units Sold vs Revenue (Clean Scatter Plot)
avg_rev = sales.groupBy("units_sold") \
    .agg(spark_sum("revenue").alias("total_revenue"))
pdf = avg_rev.toPandas()

plt.figure(figsize=(10,6))
plt.scatter(pdf['units_sold'], pdf['total_revenue'], s=60, alpha=0.8)

plt.title("Units Sold vs Total Revenue (Aggregated)")
plt.xlabel("Units Sold")
plt.ylabel("Total Revenue")
plt.grid(True, linestyle='--', alpha=0.5)

plt.show()
