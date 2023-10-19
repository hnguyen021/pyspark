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
# 4649 4150 3236 3749 2983 4958 4648 5373 457 2666 2664 2286 4834 3078 188 3046 

# id_pattern = r'\W+'

# connections = lines.select(func.explode(func.split(lines.value, "\\W+")).alias("id"))
# connectionsWithoutEmptyString = connections.filter(connections.id != "")


# Split the "value" column by space and get the first element as "id"
connections = lines.withColumn("id", func.split(func.trim(lines.value), " ")[0])
#connections.show()
# Calculate the number of connections by splitting the "value" column by space and subtracting 1
connections = connections.withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1)
connections.show()                                  
# Group by "id" and calculate the sum of connections
connections = connections.groupBy("id").agg(func.sum("connections").alias("connections"))
connections.show()


mostPopular = connections.sort(func.col("connections").desc()).first()

mostPopularName = names.filter(func.col("id") == mostPopular[0]).select("name").first()
print(mostPopularName[0] + " is the most popular superhero with " + str(mostPopular[1]) + " co-appearances.")



# Small tweak vs. what's shown in the video: we trim each line of whitespace as that could
# throw off the counts.
# connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
#     .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
#     .groupBy("id").agg(func.sum("connections").alias("connections"))
    
# connections.show()
# mostPopular = connections.sort(func.col("connections").desc()).first()

# mostPopularName = names.filter(func.col("id") == mostPopular[0]).select("name").first()

# print(mostPopularName[0] + " is the most popular superhero with " + str(mostPopular[1]) + " co-appearances.")

