from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("MoviePipeline").getOrCreate()

# Bronze layer
df = spark.read.csv("data/movies.csv", header=True, inferSchema=True)

# Silver layer (cleaning)
df_clean = df.dropna(subset=["rating"])

df_filtered = df_clean.filter(col("rating") > 7)

# Save processed dataset
df_filtered.write.mode("overwrite").parquet("output/silver/movies")