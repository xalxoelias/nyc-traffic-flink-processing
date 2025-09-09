from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Initialize Spark
spark = SparkSession.builder \
    .appName("HighCongestionFilterJob") \
    .getOrCreate()

# Schema for incoming JSON
schema = StructType([
    StructField("speed", StringType(), True),
    StructField("borough", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("link_points", StringType(), True)
])

# Read from Kinesis (raw traffic data)
raw_df = spark.readStream \
    .format("kinesis") \
    .option("streamName", "nyc_traffic_data") \
    .option("region", "us-east-1") \
    .option("initialPosition", "LATEST") \
    .load()

# Decode base64 Kinesis data → JSON
decoded_df = raw_df.selectExpr("CAST(data AS STRING) as json_string")
parsed_df = decoded_df.select(from_json(col("json_string"), schema).alias("data")).select("data.*")

# Filter: speed < 10
high_congestion_df = parsed_df.filter(col("speed").cast("float") < 10)

# Write filtered stream back to Kinesis
high_congestion_df \
    .selectExpr("to_json(struct(*)) AS data") \
    .writeStream \
    .format("kinesis") \
    .option("streamName", "high_congestion_stream") \
    .option("region", "us-east-1") \
    .option("checkpointLocation", "s3://glue-processing-kinesis-data/checkpoints/job1/") \
    .start() \
    .awaitTermination()
