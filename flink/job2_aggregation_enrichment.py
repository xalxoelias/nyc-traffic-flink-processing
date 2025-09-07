# job2_aggregation_alert_enrichment.py
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors import FlinkKinesisConsumer, FlinkKinesisProducer
from pyflink.common.typeinfo import Types
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common.time import Time
from pyflink.datastream.functions import ProcessWindowFunction
import json
from datetime import datetime

# ------------------------------
# Setup environment
# ------------------------------
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
env.set_stream_time_characteristic(TimeCharacteristic.ProcessingTime)

# Source: high_congestion_stream
source_properties = {
    "aws.region": "us-east-1",
    "stream.initial.position": "LATEST"
}

high_congestion_stream = env.add_source(
    FlinkKinesisConsumer(
        stream="high_congestion_stream",
        deserialization_schema=SimpleStringSchema(),
        properties=source_properties
    )
)

# ------------------------------
# Parse JSON records
# ------------------------------
def parse_record(record_str):
    try:
        record = json.loads(record_str)
        record["timestamp"] = record.get("timestamp", datetime.utcnow().isoformat())
        return record
    except Exception:
        return {"speed": 0.0, "borough": "unknown", "timestamp": datetime.utcnow().isoformat()}

parsed_stream = high_congestion_stream.map(
    lambda r: parse_record(r),
    output_type=Types.MAP(Types.STRING(), Types.STRING())
)

# ------------------------------
# Aggregation by borough (1-min tumbling window)
# ------------------------------
class BoroughAgg(ProcessWindowFunction):
    def process(self, key, context, elements, out):
        count = 0
        total_speed = 0.0
        for e in elements:
            speed = float(e.get("speed", 0))
            total_speed += speed
            count += 1
        avg_speed = total_speed / count if count > 0 else 0.0

        result = {
            "borough": key,
            "window_end": datetime.utcnow().isoformat(),
            "count": count,
            "avg_speed": avg_speed
        }

        # Generate alerts
        if avg_speed < 5:
            result["alert"] = "SEVERE_CONGESTION"
        else:
            result["alert"] = "NORMAL"

        # Enrichment (mock historical average, replace with Dynamo/S3 lookup)
        historical_avg = 12.5
        result["historical_avg"] = historical_avg
        result["trend"] = "worse" if avg_speed < historical_avg else "better"

        out.collect(json.dumps(result))

aggregated_stream = (
    parsed_stream
    .key_by(lambda r: r.get("borough", "unknown"))
    .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
    .process(BoroughAgg(), output_type=Types.STRING())
)

# ------------------------------
# Sink: processed_traffic_stream
# ------------------------------
producer = FlinkKinesisProducer(
    stream="processed_traffic_stream",
    serialization_schema=SimpleStringSchema(),
    producer_config={"aws.region": "us-east-1"}
)

aggregated_stream.add_sink(producer)

# ------------------------------
# Execute job
# ------------------------------
env.execute("Traffic Aggregation & Alert Job")
