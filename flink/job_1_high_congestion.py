# job1_high_congestion.py
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKinesisConsumer, FlinkKinesisProducer
from pyflink.common.serialization import SimpleStringSchema
import json

# ------------------------------
# Setup environment
# ------------------------------
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

# ------------------------------
# Kinesis source (raw traffic data)
# ------------------------------
source_properties = {
    "aws.region": "us-east-1",
    "stream.initial.position": "LATEST"
}

raw_stream = env.add_source(
    FlinkKinesisConsumer(
        stream="nyc_traffic_data",
        deserialization_schema=SimpleStringSchema(),
        properties=source_properties
    )
)

# ------------------------------
# Filter high congestion (speed < 10)
# ------------------------------
def filter_congestion(record_str):
    try:
        record = json.loads(record_str)
        speed = float(record.get("speed", 0))
        return speed < 10
    except Exception:
        return False

high_congestion_stream = raw_stream.filter(filter_congestion)

# ------------------------------
# Send filtered stream to new Kinesis stream
# ------------------------------
producer = FlinkKinesisProducer(
    stream="high_congestion_stream",
    serialization_schema=SimpleStringSchema(),
    producer_config={"aws.region": "us-east-1"}
)

high_congestion_stream.add_sink(producer)

# ------------------------------
# Execute job
# ------------------------------
env.execute("High Congestion Filter Job")
