import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lit, row_number, mean, expr, max as spark_max
from pyspark.sql.window import Window

# -----------------------
# Setup Spark
# -----------------------
spark = SparkSession.builder.appName("Silver_Layer").getOrCreate()

# -----------------------
# Paths
# -----------------------
bronze_path = "data/bronze"
silver_path = "data/silver"

# Remove old Silver folder if exists
if os.path.exists(silver_path):
    shutil.rmtree(silver_path)
os.makedirs(silver_path, exist_ok=True)

# -----------------------
# Load Bronze
# -----------------------
df = spark.read.parquet(bronze_path)

# -----------------------
# Deduplicate Sale Rows
# Keep the latest warranty claim per sale_id if multiple exist
# Merge partial info intelligently using max/first
# -----------------------
window_spec = Window.partitionBy("sale_id").orderBy(col("claim_date").desc())

# Add row number to pick latest claim per sale_id
df = df.withColumn("row_num", row_number().over(window_spec)).filter(col("row_num") == 1).drop("row_num")

# -----------------------
# Null Handling & Cleaning
# -----------------------
# Fill essential text columns
df = df.fillna({
    "Store_Name": "Not Mentioned",
    "City": "Unknown",
    "Country": "Unknown",
    "Product_Name": "Unknown",
    "Category_Name": "Unknown",
    "repair_status": "None"
})

# Fill numeric columns
mean_quantity = df.select(mean(col("quantity"))).collect()[0][0] or 0
df = df.withColumn("quantity", coalesce(col("quantity"), lit(int(mean_quantity))))
df = df.withColumn("Price", coalesce(col("Price"), lit(0.0)))

# Fill dates if NULL (optional: leave as NULL if no value)
df = df.withColumn("Launch_Date", coalesce(col("Launch_Date"), lit(None).cast("date")))
df = df.withColumn("claim_date", coalesce(col("claim_date"), lit(None).cast("date")))

# -----------------------
# Derived Metrics
# -----------------------
df = df.withColumn("total_price", col("quantity") * col("Price")) \
       .withColumn("days_since_launch", expr("datediff(order_date, Launch_Date)"))

# -----------------------
# Ranking / Window Functions
# Rank products per store by total sales
# -----------------------
window_rank = Window.partitionBy("Store_ID").orderBy(col("total_price").desc())
df = df.withColumn("product_rank_in_store", row_number().over(window_rank))

# -----------------------
# CTE-like temporary view for advanced analytics
# -----------------------
df.createOrReplaceTempView("silver_temp")
# Example: top-selling products overall
top_products = spark.sql("""
SELECT Product_ID, Product_Name, SUM(total_price) as total_sales
FROM silver_temp
GROUP BY Product_ID, Product_Name
ORDER BY total_sales DESC
""")
top_products.show(10, truncate=False)

# -----------------------
# Write Silver Parquet
# Partitioning by order_date optional (can remove if not needed)
# -----------------------
df.write.mode("overwrite") \
    .partitionBy("order_date") \
    .parquet(silver_path)

# -----------------------
# Verify
# -----------------------
df_check = spark.read.parquet(silver_path)
df_check.show(5, truncate=False)
print(f"Total rows in Silver Parquet: {df_check.count()}")
print("Silver layer ready ✅")
