import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lit, udf
from pyspark.sql.types import DateType
from datetime import datetime

# -----------------------
# Setup Spark
# -----------------------
spark = SparkSession.builder \
    .appName("Bronze_Layer") \
    .getOrCreate()

# -----------------------
# Paths
# -----------------------
raw_path = "data/raw"
bronze_path = "data/bronze"

# Remove old Bronze folder if exists
if os.path.exists(bronze_path):
    shutil.rmtree(bronze_path)
os.makedirs(bronze_path, exist_ok=True)

# -----------------------
# Safe date parsing UDF
# -----------------------
def parse_date_safe(d):
    for fmt in ("%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(d, fmt)
        except:
            continue
    return None

parse_date_udf = udf(parse_date_safe, DateType())

# -----------------------
# Load CSVs
# -----------------------
sales_df = spark.read.option("header", True).csv(f"{raw_path}/sales.csv")
stores_df = spark.read.option("header", True).csv(f"{raw_path}/stores.csv")
products_df = spark.read.option("header", True).csv(f"{raw_path}/products.csv")
category_df = spark.read.option("header", True).csv(f"{raw_path}/category.csv")
warranty_df = spark.read.option("header", True).csv(f"{raw_path}/warranty.csv")

# -----------------------
# Clean & Cast Columns
# -----------------------
sales_df = sales_df.withColumn("order_date", parse_date_udf(col("order_date"))) \
                   .withColumn("quantity", col("quantity").cast("integer"))

products_df = products_df.withColumn("Price", col("Price").cast("double")) \
                         .withColumn("Launch_Date", parse_date_udf(col("Launch_Date")))

warranty_df = warranty_df.withColumn("claim_date", parse_date_udf(col("claim_date")))

# -----------------------
# Join Tables
# -----------------------
bronze_df = sales_df.join(stores_df, on="Store_ID", how="left") \
                    .join(products_df, on="Product_ID", how="left") \
                    .join(category_df, on="Category_ID", how="left") \
                    .join(warranty_df, on="sale_id", how="left")

# -----------------------
# Fill missing main table fields
# Only for essential columns; leave warranty info as NULL if missing
# -----------------------
bronze_df = bronze_df.fillna({
    "Store_Name": "Unknown",
    "City": "Unknown",
    "Country": "Unknown",
    "Product_Name": "Unknown",
    "Category_Name": "Unknown",
    "Price": 0.0,
    "Launch_Date": "1970-01-01"
})

# -----------------------
# Write Bronze Parquet
# Partition by order_date (yyyy-MM-dd)
# -----------------------
bronze_df.write.mode("overwrite") \
    .partitionBy("order_date") \
    .parquet(bronze_path)

# -----------------------
# Verify
# -----------------------
df_check = spark.read.parquet(bronze_path)
df_check.show(5, truncate=False)
print(f"Total rows in Bronze Parquet: {df_check.count()}")
print("Bronze layer ready ✅")
