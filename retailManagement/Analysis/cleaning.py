import sys
import os

# === Environment Setup ===
os.environ["JAVA_HOME"] = "C:/Program Files/Java/jdk-21"
os.environ["SPARK_HOME"] = "C:/Users/mahas/OneDrive/Desktop/python project/Revature training/retailManagement/.venv/Lib/site-packages/pyspark"
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession


# Starting
spark = SparkSession.builder.appName("RetailManagementCleaning").getOrCreate()

# Loading
sales = spark.read.csv("C:/Users/mahas/OneDrive/Desktop/python project/Revature training/retailManagement/data/sales_data.csv", header=True, inferSchema=True)
online_sales = spark.read.csv("C:/Users/mahas/OneDrive/Desktop/python project/Revature training/retailManagement/data/Online Sales Data.csv", header=True, inferSchema=True)

#duplicates
sales_count = sales.count()
print(sales_count)
sales_no_dup_count = sales.dropDuplicates().count()
sales_dup = sales_count - sales_no_dup_count

print("Sales duplicates:", sales_dup)

online_count = online_sales.count()
online_no_dup_count = online_sales.dropDuplicates().count()
online_dup = online_count - online_no_dup_count

print("Online sales duplicates:", online_dup)

# checking missing data
from pyspark.sql.functions import col, sum as spark_sum

print("Missing Values in ONLINE_SALES DATA")

for c in online_sales.columns:
    # null → 1, not null → 0 and sum
    null_count = (
        online_sales.select(spark_sum(col(c).isNull().cast("int")).alias("nulls"))
             .collect()[0]["nulls"]
    )
    print(f"{c}: {null_count}")
print("\n")

print("Missing Values in SALES DATA")

for c in sales.columns:
# null → 1, not null → 0 and sum
    null_count = (
        sales.select(spark_sum(col(c).isNull().cast("int")).alias("nulls"))
             .collect()[0]["nulls"]
    )
    print(f"{c}: {null_count}")
print("\n")

#displaying schema
print("SCHEMA FOR SALES DATASET")
sales.printSchema()
print("SCHEMA FOR ONLINE_SALES DATASET")
online_sales.printSchema()

#standardizing column names
sales = sales \
    .withColumnRenamed("Sales_Amount", "revenue") \
    .withColumnRenamed("Quantity_Sold", "units_sold") \
    .withColumnRenamed("Payment_Method", "payment_method") \
    .withColumnRenamed("Product_Category", "product_category") \
    .withColumnRenamed("Unit_Price", "unit_price") \
    .withColumnRenamed("Region", "region") \
    .withColumnRenamed("Sale_Date", "date")

online_sales = online_sales.withColumnRenamed("total_revenue", "revenue")


for col_name in online_sales.columns:
    online_sales = online_sales.withColumnRenamed(
        col_name,
        col_name.replace(" ", "_").lower()
    )
#region
from pyspark.sql.functions import when, col

sales = sales.withColumn(
    "region_group",
    when(col("region") == "North", "North America")
    .when(col("region").isin("South", "East"), "Asia")
    .when(col("region") == "West", "Europe")
    .otherwise(col("region"))
)

online_sales = online_sales.withColumn(
    "region_group",
    when(col("region") == "North", "North America")
    .when(col("region").isin("South", "East"), "Asia")
    .when(col("region") == "West", "Europe")
    .otherwise(col("region"))
)
from pyspark.sql.functions import lower, trim
sales = sales.withColumn("payment_method", trim(lower(col("payment_method"))))
online_sales = online_sales.withColumn("payment_method", trim(lower(col("payment_method"))))

from pyspark.sql.functions import when

sales = sales.withColumn(
    "payment_method",
    when(col("payment_method").like("%card%"), "card")
    .when(col("payment_method").like("%upi%"), "upi")
    .when(col("payment_method").like("%cash%"), "cash")
    .otherwise(col("payment_method"))
)

online_sales = online_sales.withColumn(
    "payment_method",
    when(col("payment_method").like("%card%"), "card")
    .when(col("payment_method").like("%upi%"), "upi")
    .when(col("payment_method").like("%cash%"), "cash")
    .otherwise(col("payment_method"))
)

from pyspark.sql.functions import month
sales = sales.withColumn("month", month(col("date")))
online_sales = online_sales.withColumn("month", month(col("date")))

#final checking
sales.printSchema()
sales.show(5)

online_sales.printSchema()
online_sales.show(5)

#cleaned sales and online_dataset dataset
pdf = sales.toPandas()
pdf.to_csv("cleaned_sales.csv", index=False)
pdf1 = online_sales.toPandas()
pdf1.to_csv("cleaned_online_sales.csv", index=False)

