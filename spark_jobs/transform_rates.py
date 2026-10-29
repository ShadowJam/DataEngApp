from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, to_date


def main():
    # 1. Initialize Spark Session
    spark = SparkSession.builder \
        .appName("RatesTransform") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 2. Read raw Parquet data from MinIO (Bronze layer)
    print("Reading data from MinIO...")
    df_bronze = spark.read.parquet("s3a://bronze/rates/")

    # 3. Transformation
    print("Transforming data...")
    df_fact = df_bronze.groupBy("date", "target_currency") \
        .agg(avg("rate").alias("avg_rate")) \
        .withColumn("avg_rate", col("avg_rate").cast("double")) \
        .withColumn("date", to_date(col("date")))  # Convert string to Date type for ClickHouse

    # 4. Write to ClickHouse
    print("Writing data to ClickHouse...")
    df_fact.write \
        .format("jdbc") \
        .option("url", "jdbc:clickhouse://clickhouse:8123/rates_db") \
        .option("dbtable", "fact_rates") \
        .option("user", "default") \
        .option("password", "clickhouse") \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .mode("append") \
        .save()

    print("Successfully written to ClickHouse!")
    spark.stop()


if __name__ == "__main__":
    main()