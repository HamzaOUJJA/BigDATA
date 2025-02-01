from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaSparkStreamingExample") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# Define Kafka Source
kafka_stream_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:19092") \
    .option("subscribe", "capteur") \
    .load()

# Convert Kafka value (binary) to string
transaction_df = kafka_stream_df.selectExpr("CAST(value AS STRING) as transaction")

# Define JSON schema (modify based on your data structure)
json_schema = StructType() \
    .add("id_transaction", StringType()) \
    .add("type_transaction", StringType()) \
    .add("montant", StringType()) \
    .add("devise", StringType()) \
    .add("date", StringType()) \
    .add("lieu", StringType()) \
    .add("moyen_paiement", StringType()) \
    .add("details", StructType().add("produit", StringType()) \
                                .add("quantite", StringType()) \
                                .add("prix_unitaire", StringType())) \
    .add("utilisateur", StructType().add("id_utilisateur", StringType()) \
                                .add("nom", StringType()) \
                                .add("adresse", StringType()) \
                                .add("email", StringType())) \


# Parse JSON message
json_df = transaction_df.withColumn("json_data", from_json(col("transaction"), json_schema))

# Select JSON fields to display properly
formatted_df = json_df.select("json_data.*")

# Print JSON output in console
query = formatted_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# Await termination of the stream
query.awaitTermination()
