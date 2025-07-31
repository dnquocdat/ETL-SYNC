from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# 1. Khởi SparkSession (chỉnh lại key output.uri)
spark = SparkSession.builder \
    .appName("ETL-Sync-MySQL-to-MongoDB") \
    .config("spark.mongodb.output.uri", "mongodb://mongo:27017/etl_db.users") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Schema cho Debezium CDC
schema = StructType([
    StructField("before", StructType([
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("email", StringType()),
        StructField("updated_at", TimestampType())
    ])),
    StructField("after", StructType([
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("email", StringType()),
        StructField("updated_at", TimestampType())
    ])),
    StructField("op", StringType()),    # c, u, d, r
    StructField("ts_ms", StringType())
])

# 3. Đọc stream từ Kafka
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "mysql_server.etl_db.users") \
    .option("startingOffsets", "earliest") \
    .load()

# 4. Parse JSON
parsed_df = raw_df.selectExpr("CAST(value AS STRING) AS json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# 5. Xử lý mỗi micro-batch
def process_batch(batch_df, batch_id):
    # 5.1 INSERT / UPDATE / READ
    upsert_df = batch_df.filter(col("op").isin("c", "u", "r")) \
        .select(
            col("after.id").alias("_id"),
            col("after.name").alias("name"),
            col("after.email").alias("email"),
            col("after.updated_at").alias("updated_at")
        )

    if not upsert_df.rdd.isEmpty():
        upsert_df.write \
            .format("mongodb") \
            .option("uri", "mongodb://mongo:27017/etl_db.users") \
            .mode("append") \
            .save()

    # 5.2 DELETE
    delete_ids = [row["_id"] for row in batch_df.filter(col("op") == "d")
                                      .select(col("before.id").alias("_id"))
                                      .collect()]
    if delete_ids:
        from pymongo import MongoClient
        client = MongoClient("mongodb://mongo:27017/")
        coll = client["etl_db"]["users"]
        coll.delete_many({"_id": {"$in": delete_ids}})
        client.close()

# 6. Start streaming và giữ chạy liên tục
query = parsed_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("update") \
    .option("checkpointLocation", "/opt/spark-app/checkpoint") \
    .start()

query.awaitTermination()
