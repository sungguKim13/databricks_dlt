###############################
## DLT 적용 : Non Materialzed view
###############################

import dlt
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType

schema = StructType().add("Id", StringType()).add("Name", StringType())

@dlt.view(name="raw_kafka_data_view")
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

@dlt.table(name="nonma_userinfo")
def parse_json():
    df = dlt.read("raw_kafka_data_view")
    return df.select(from_json(col("json_str"), schema).alias("userinfo")).select("userinfo.*")
