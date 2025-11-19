import sys
import os

# === Environment Setup ===
os.environ["JAVA_HOME"] = "C:/Program Files/Java/jdk-21"
os.environ["SPARK_HOME"] = "C:/Users/mahas/OneDrive/Desktop/python project/Revature training/retailManagement/.venv/Lib/site-packages/pyspark"
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


from pyspark.sql import SparkSession

from pyspark.sql.functions import col, sum as spark_sum, month

spark = SparkSession.builder.appName("RetailManagementTransformation").getOrCreate()
sales = spark.read.csv("cleaned_sales.csv", header=True, inferSchema=True)
online_sales = spark.read.csv("cleaned_online_sales.csv", header=True, inferSchema=True)

print("Cleaned files loaded successfully!")

#month wise category
print("Month-wise Category")
month_category = sales.groupBy("month", "product_category") \
    .agg(
        spark_sum("revenue").alias("total_revenue"),
        spark_sum("units_sold").alias("total_units_sold")
    )
month_category.show()
print("\n")

#Region-wise Revenue
print("Region-wise Revenue")
offline_region = sales.groupBy("region_group").agg(spark_sum("revenue").alias("offline_revenue"))
online_region  = online_sales.groupBy("region_group").agg(spark_sum("total_revenue").alias("online_revenue"))

region_compare = offline_region.join(online_region, "region_group", "outer")
region_compare.show()
print("\n")

#Category-wise Revenue (overall performance)
print("Category-wise Revenue")
category_revenue = sales.groupBy("product_category") \
    .agg(spark_sum("revenue").alias("total_revenue"))
category_revenue.show()
print("\n")

#Payment Method Preference
print("Payment Method Preference")
payment_pref = sales.groupBy("payment_method") \
    .agg(
        spark_sum("revenue").alias("total_revenue"),
        spark_sum("units_sold").alias("total_units_sold")
    )
payment_pref.show()
print("\n")

#Online vs Offline Revenue Comparison
print("Online vs Offline Revenue Comparison")
offline_rev = sales.agg(spark_sum("revenue").alias("offline_revenue"))
online_rev  = online_sales.agg(spark_sum("total_revenue").alias("online_revenue"))

offline_rev.show()
online_rev.show()
print("\n")

'''# Units Sold vs Revenue
profitability = sales.select(
    "product_category",
    "units_sold",
    "revenue"
)

profitability.show()
'''
sales.createOrReplaceTempView("sales_table")
profitability = spark.sql("""
    SELECT 
        product_category,
        units_sold,
        revenue
    FROM sales_table
""")
profitability.show()






