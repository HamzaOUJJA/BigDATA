from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr, to_timestamp, when
from pyspark.sql.types import StructType, StringType, FloatType, IntegerType, TimestampType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()


# Define JSON schema
schema = StructType() \
                    .add("rank", IntegerType()) \
                    .add("id_transaction", StringType()) \
                    .add("type_transaction", StringType()) \
                    .add("montant", FloatType()) \
                    .add("devise", StringType()) \
                    .add("date", TimestampType()) \
                    .add("lieu", StringType()) \
                    .add("moyen_paiement", StringType()) \
                    .add("details", StructType()
                                .add("produit", StringType())
                                .add("quantite", IntegerType())
                                .add("prix_unitaire", FloatType())) \
                    .add("utilisateur", StructType()
                                .add("id_utilisateur", StringType())
                                .add("nom", StringType())
                                .add("adresse", StringType())
                                .add("email", StringType()))


# Read from Kafka
kafka_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:19092") \
    .option("subscribe", "capteur") \
    .load()




# Convert Kafka value (binary) to string 
# Parse JSON message
transaction_df = kafka_stream_df.selectExpr("CAST(value AS STRING) as transaction")
df = transaction_df.withColumn("json_data", from_json(col("transaction"), schema)).select("json_data.*")



# aggregats
# Convert USD to EUR (assuming 1 USD = 0.92 EUR)
df = df.withColumn("montant", when(col("devise") == "USD", col("montant") * 0.92).otherwise(col("montant"))) \
                 .withColumn("devise", when(col("devise") == "USD", "EUR").otherwise(col("devise")))


# Convert `date` string to proper timestamp & adjust TimeZone to 'Europe/Paris'
df = df.withColumn("date", to_timestamp("date"))
df = df.withColumn("date", expr("date + INTERVAL 1 HOUR"))


# Remove transactions with 'moyen_paiement' == 'erreur'
df = df.filter(col("moyen_paiement") != "erreur")


# Remove rows where 'adresse' is NULL
df = df.filter(
    (~col("utilisateur.adresse").contains("none")) &
    (~col("lieu").contains("none"))
)


df = df.select(
    col("rank"),
    col("id_transaction"),
    col("type_transaction"),
    col("montant"),
    col("devise"),
    col("date"),
    col("lieu"),
    col("moyen_paiement"),
    col("details.produit").alias("produit"),
    col("details.quantite").alias("quantite"),
    col("details.prix_unitaire").alias("prix_unitaire"),
    col("utilisateur.id_utilisateur").alias("utilisateur(id_utilisateur)"),
    col("utilisateur.nom").alias("utilisateur(nom)"),
    col("utilisateur.adresse").alias("utilisateur(adresse)"),
    col("utilisateur.email").alias("utilisateur(email)")
)


parquet_query = df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "../Data/") \
    .option("checkpointLocation", "../Data/metadata") \
    .start()



parquet_query.awaitTermination()




