from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ReadParquetFiles") \
    .getOrCreate()

parquet_path = "../Data/"

# Read the Parquet files into a DataFrame
df = spark.read.parquet(parquet_path)

# Show the content of the DataFrame (first 100 rows)
df.show(100, False)

spark.stop()
