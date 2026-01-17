import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count as spark_count

# -----------------------
# Setup Spark
# -----------------------
spark = SparkSession.builder \
    .appName("Gold_Layer_Single_CSV") \
    .getOrCreate()

# -----------------------
# Paths
# -----------------------
silver_path = "your_path"        # Local Silver layer parquet
gold_local_path = "your_path"      # Local output folder
s3_bucket_path = "s3://your-bucket-name/suffix/your_gold.csv"  # Replace with your bucket & your csv name

# -----------------------
# Clean old local Gold
# -----------------------
if os.path.exists(gold_local_path):
    shutil.rmtree(gold_local_path)
os.makedirs(gold_local_path, exist_ok=True)

# -----------------------
# Load Silver Layer
# -----------------------
df = spark.read.parquet(silver_path)

# -----------------------
# Clean & Remove duplicates
# -----------------------
df = df.dropDuplicates()
df = df.fillna({
    "Store_Name": "Unknown",
    "Product_Name": "Unknown",
    "Category_Name": "Unknown",
    "quantity": 0,
    "total_price": 0.0
})

# -----------------------
# Aggregations per store/product/category/date
# -----------------------
gold_df = df.groupBy(
    "order_date",
    "Category_Name",
    "Store_ID",
    "Store_Name",
    "Product_ID",
    "Product_Name",
    "Category_ID"
).agg(
    spark_sum("quantity").alias("total_quantity"),
    spark_sum("total_price").alias("total_sales"),
    spark_count("sale_id").alias("total_orders")
)

# -----------------------
# Derived metric
# -----------------------
gold_df = gold_df.withColumn("avg_price", col("total_sales") / col("total_quantity"))

# -----------------------
# Write single CSV locally
# -----------------------
single_csv_path = f"{gold_local_path}/gold_layer.csv"
gold_df.coalesce(1).write.mode("overwrite").option("header", True).csv(gold_local_path)

# Since Spark writes as "part-0000*.csv", rename to gold_layer.csv
import glob
import os
for file in glob.glob(f"{gold_local_path}/part-*.csv"):
    os.rename(file, single_csv_path)
# Remove leftover Spark folders
for folder in glob.glob(f"{gold_local_path}/_SUCCESS") + glob.glob(f"{gold_local_path}/part-*"):
    try:
        shutil.rmtree(folder)
    except:
        os.remove(folder)

print(f"✅ Single Gold CSV created locally: {single_csv_path}")

# -----------------------
# Upload to S3 using AWS CLI
# -----------------------
os.system(f"aws s3 cp {single_csv_path} {s3_bucket_path}")

print(f"✅ Gold layer uploaded to S3 as single file: {s3_bucket_path}")
