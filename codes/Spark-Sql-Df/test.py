from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

spark = SparkSession.builder.appName("ExplodeExample").getOrCreate()

data = [(1, ["A", "B"]), (2, ["C"]), (3, ["D", "E", "F"])]
columns = ["ID", "Projects"]
df = spark.createDataFrame(data, columns)

exploded_df = df.select("ID", explode("Projects").alias("Project"))

exploded_df.show()
