from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ReadParquetFiles") \
    .getOrCreate()

# Path to the Parquet files (adjust according to your directory)
parquet_path = "../Data/"

# Read the Parquet files into a DataFrame
df = spark.read.parquet(parquet_path)

# Show the content of the DataFrame (first 20 rows)
df.show(truncate=False)

# Stop the Spark session
spark.stop()
