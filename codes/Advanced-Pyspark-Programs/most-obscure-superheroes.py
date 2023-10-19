from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("datasets/Marvel-Names.txt")

lines = spark.read.text("datasets/Marvel-Graphs.txt")
#print(lines.head(1)[0][0])


# Split the "value" column by space and get the first element as "id"
connections = lines.withColumn("id", func.split(func.trim(lines.value), " ")[0])
#connections.show()
# Calculate the number of connections by splitting the "value" column by space and subtracting 1
connections = connections.withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1)
#connections.show()                                  
# Group by "id" and calculate the sum of connections
connections = connections.groupBy("id").agg(func.sum("connections").alias("connections"))
#connections.show()


mostPopular = connections.sort(func.col("connections").desc())
minConnection  = mostPopular.agg(func.min("connections")).first()[0]

minPopular  = mostPopular.filter(func.col("connections") == minConnection)
#print(minPopular.select("id").first()[0])
minConnectionWithName = minPopular.join(names,"id")
minConnectionWithName.show()
# print(minPopular[0][1].value)
minPopularName = names.filter(func.col("id") == minPopular.select("id").first()[0]).select("name").first()
# minPopularName.show()
print(minPopularName[0] + " is the min popular superhero with " + str(minConnection) + " co-appearances.")

