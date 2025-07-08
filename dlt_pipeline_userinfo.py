
###############################
## DLT 적용
###############################

import dlt
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType

# JSON 스키마 정의
schema = StructType() \
    .add("Id", StringType()) \
    .add("Name", StringType())

# Kafka에서 스트리밍 데이터 읽기
@dlt.table(name="raw_kafka_data")
def read_kafka_stream():
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "sgkimevtns.servicebus.windows.net:9093")
        .option("subscribe", "databrickshub")
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", """
            org.apache.kafka.common.security.plain.PlainLoginModule required
            username="$ConnectionString"
            password="Endpoint=sb://sgkimevtns.servicebus.windows.net/;SharedAccessKeyName=databrickspolicy;SharedAccessKey=VGRAX13zYzdBVpHliSX+6r4P5peKQXnnQ+AEhCIXY/I=;EntityPath=databrickshub";
        """)
        .load()
        .selectExpr("CAST(value AS STRING) as json_str")
    )

# JSON 파싱 및 정제
@dlt.table(name="parsed_userinfo")
def parse_json():
    df = dlt.read("raw_kafka_data")
    return df.select(from_json(col("json_str"), schema).alias("userinfo")).select("userinfo.*")
