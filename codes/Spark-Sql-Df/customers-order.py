from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("Customers-Order").getOrCreate()

schema = StructType([ \
                     StructField("First", IntegerType(), True), \
                     StructField("Second", IntegerType(), True), \
                     StructField("Third", FloatType(), True), \
                    ])

# // Read the file as dataframe
df = spark.read.schema(schema).csv("datasets/customer-orders.csv")
#df.printSchema()
cus_temp = df.select("First","Third") 
result = cus_temp.groupBy("First").agg(func.round(func.sum("Third"),2).alias("Consumed-Sum")).sort("First")
result.show(result.count())

spark.stop()