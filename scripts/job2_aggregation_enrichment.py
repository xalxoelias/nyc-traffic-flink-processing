from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

# ------------------------------
# Spark session
# ------------------------------
spark = SparkSession.builder \
    .appName("Kinesis_to_MongoDB") \
    .getOrCreate()

# ------------------------------
# Kinesis stream schema
# Adjust fields based on your NYC traffic data
# ------------------------------
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("speed", FloatType(), True),
    StructField("borough", StringType(), True),
    StructField("link_points", StringType(), True)
])

# ------------------------------
# Read Kinesis stream
# ------------------------------
df = spark.readStream \
    .format("kinesis") \
    .option("streamName", "high_congestion_stream") \
    .option("region", "us-east-1") \
    .option("startingposition", "LATEST") \
    .load()

# Convert binary data to string and parse JSON
df_parsed = df.selectExpr("CAST(data AS STRING) as json_data") \
    .select(from_json(col("json_data"), schema).alias("data")) \
    .select("data.*")

# Optional: add alert column
df_transformed = df_parsed.withColumn(
    "alert",
    (col("speed") < 5).cast("string")
)

# ------------------------------
# Write to MongoDB
# ------------------------------
df_transformed.writeStream \
    .format("mongodb") \
    .option("spark.mongodb.connection.uri", "mongodb+srv://<username>:<password>@<cluster>/<database>.<collection>") \
    .option("checkpointLocation", "s3://glue-processing-kinesis-data/checkpoints/jobB/") \
    .start() \
    .awaitTermination()
