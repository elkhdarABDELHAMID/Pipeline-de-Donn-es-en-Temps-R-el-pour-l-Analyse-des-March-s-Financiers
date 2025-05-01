from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, to_timestamp, sum, count
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, DoubleType, LongType
import signal
import sys

def signal_handler(sig, frame):
    print("Spark Streaming stop !!!")
    query_raw.stop()  # إيقاف استعلام البيانات الخام
    query_aggregated.stop()  # إيقاف استعلام البيانات المجمعة
    spark.stop()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

spark = SparkSession.builder \
    .appName("FinnhubKafkaConsumer") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
    .config("spark.default.parallelism", "4") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.streaming.kafka.maxRatePerPartition", "1000") \
    .getOrCreate()

schema = StructType([
    StructField("type", StringType(), True),
    StructField("data", ArrayType(StructType([
        StructField("c", StringType(), True),
        StructField("p", DoubleType(), True),
        StructField("s", StringType(), True),
        StructField("t", LongType(), True),
        StructField("v", DoubleType(), True)
    ])), True)
])

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "raw_financial_data") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "1000") \
    .option("failOnDataLoss", "false") \
    .load()

value_df = kafka_df.selectExpr("CAST(value AS STRING) as value")
json_df = value_df.withColumn("jsonData", from_json(col("value"), schema)).select("jsonData.*")
trade_df = json_df.selectExpr("explode(data) as data") \
    .select(
        col("data.s").alias("symbol"),
        col("data.p").alias("price"),
        col("data.v").alias("volume"),
        col("data.t").alias("timestamp")
    )

trade_df = trade_df.withColumn("timestamp", to_timestamp(col("timestamp") / 1000))

trade_df = trade_df.withWatermark("timestamp", "10 minutes")

windowed_df = trade_df.groupBy(
    window(col("timestamp"), "5 seconds").alias("window"),
    col("symbol")
).agg(
    avg("price").alias("avg_price"),
    sum("volume").alias("volume_sum"),
    count("*").alias("trade_count")
)

final_df = windowed_df.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("symbol"),
    col("avg_price"),
    col("volume_sum"),
    col("trade_count")
)

# كتابة البيانات الخام (trade_df) إلى جدول fintech.raw_trades في Cassandra
query_raw = trade_df.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("checkpointLocation", "/opt/spark-data/checkpoints_raw") \
    .option("keyspace", "fintech") \
    .option("table", "raw_trades") \
    .option("spark.cassandra.connection.host", "cassandra") \
    .outputMode("append") \
    .trigger(processingTime="5 seconds") \
    .start()

# كتابة البيانات المجمعة (final_df) إلى جدول fintech.aggregated_trades في Cassandra
query_aggregated = final_df.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("checkpointLocation", "/opt/spark-data/checkpoints_aggregated") \
    .option("keyspace", "fintech") \
    .option("table", "aggregated_trades") \
    .option("spark.cassandra.connection.host", "cassandra") \
    .outputMode("append") \
    .trigger(processingTime="5 seconds") \
    .start()

# انتظار إنهاء الاستعلامات
query_raw.awaitTermination()
query_aggregated.awaitTermination()