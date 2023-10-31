from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, window
import pyspark.sql.functions as func
# Create a SparkSession
spark = SparkSession.builder.appName("WindowFunctionExample").getOrCreate()

# Sample DataFrame containing sales transactions with timestamp
data = [
    ("2023-10-28 08:00:00", "ProductA", 100),
    ("2023-10-27 08:15:00", "ProductB", 150),
    ("2023-10-26 08:30:00", "ProductC", 200),
    ("2023-10-25 08:30:00", "ProductD", 600),
    ("2023-10-24 08:30:00", "ProductE", 200)
    ]

columns = ["timestamp", "product", "sales_amount"]

df = spark.createDataFrame(data, columns)

# Define a window specification for daily windows
window_spec = window(col("timestamp"),"2 days","1 day")

# Perform aggregation to calculate daily sales
daily_sales = df.groupBy(window_spec, col("product")).agg(sum(col("sales_amount")).alias("total_sales"))
daily_sales.sort(col("product"))

# Show the result
daily_sales.show(truncate=False)

# Stop the SparkSession
spark.stop()
