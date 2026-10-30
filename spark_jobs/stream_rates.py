import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, avg, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

def main():
    # 1. Initialize Spark Session with required packages for Kafka and ClickHouse
    spark = SparkSession.builder \
        .appName("KafkaToClickHouseStreaming") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "com.clickhouse:clickhouse-jdbc:0.6.3") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", "minioadmin")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 2. Define JSON schema for Kafka messages
    json_schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("rate", DoubleType(), True)
    ])

    # 3. Read stream from Kafka
    df_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "redpanda:9092") \
        .option("subscribe", "currency_rates") \
        .option("startingOffsets", "earliest") \
        .load()

    # 4. Parse JSON from the "value" column
    parsed_df = df_stream.select(
        from_json(col("value").cast("string"), json_schema).alias("data")
    ).select("data.*")

    # 5. Convert string to timestamp for windowing functions
    streaming_df = parsed_df.withColumn("event_time", to_timestamp(col("timestamp")))

    # 6. Aggregate data in a tumbling window (1 minute)
    windowed_df = streaming_df \
        .groupBy(
            window(col("event_time"), "1 minute"),
            col("currency")
        ) \
        .agg(avg("rate").alias("avg_rate"))

    # 7. Write to ClickHouse using foreachBatch
    def write_to_clickhouse(batch_df, batch_id):
        # batch_df contains data for the current microbatch
        final_df = batch_df.select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("currency"),
            col("avg_rate")
        )
        final_df.write \
            .format("jdbc") \
            .option("url", "jdbc:clickhouse://clickhouse:8123/rates_db") \
            .option("dbtable", "streaming_rates") \
            .option("user", os.getenv("CLICKHOUSE_USER", "default")) \
            .option("password", os.getenv("CLICKHOUSE_PASSWORD", "clickhouse")) \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .mode("append") \
            .save()
        print(f"Batch {batch_id} written to ClickHouse")

    # 8. Start the streaming query
    query = windowed_df.writeStream \
        .outputMode("complete") \
        .foreachBatch(write_to_clickhouse) \
        .option("checkpointLocation", "/tmp/spark-checkpoint") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()