from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType

from waba import config, iceberg, schemas, silver, streaming
from waba.session import get_logger, get_spark
from waba.validation import validate

log = get_logger("stream_raw_to_silver")
DLQ_TOPIC = "dlq-financial-events"
refs_cache = streaming.TtlCache(silver.load_refs, ttl_seconds=600)


def parse(raw, ds):
    m = F.from_json("value", MapType(StringType(), StringType()))
    df = raw.where(F.col("topic") == ds.topic).withColumn("m", m)
    return (df.select(*[F.col("m")[c].alias(c) for c in ds.column_names],
                      F.col("m")["ingestion_timestamp"].alias("ingestion_timestamp"),
                      F.col("m")["source_file"].alias("source_file"),
                      F.col("m").isNotNull().alias("_json_ok"),
                      "topic", "value", "kafka_ts"))


def checked(raw, ds):
    v = validate(parse(raw, ds), ds)
    return v.withColumn("_error", F.when(~F.col("_json_ok"), F.lit("INVALID_JSON")).otherwise(F.col("_error")))


def dlq_stream(raw):
    rejected = None
    for ds in schemas.TRANSACTIONAL:
        part = (checked(raw, ds).where(F.col("_error") != "")
                .select(F.col("topic").alias("source_topic"), F.col("_error").alias("error"),
                        F.col("value").alias("raw_value"), "source_file", "kafka_ts",
                        F.current_timestamp().alias("detected_at"),
                        F.coalesce(F.col("country_code"), F.lit("UNKNOWN")).alias("country_code")))
        rejected = part if rejected is None else rejected.unionByName(part)
    return rejected


def silver_handler(ds):
    def handle(batch_df, batch_id):
        spark = batch_df.sparkSession
        df = (batch_df.drop("_error", "_raw", "_json_ok", "topic", "value", "kafka_ts", "ingestion_timestamp",
                            "source_file")
              .withColumn("source", F.lit("stream"))
              .withColumn("batch_ingested_at", F.lit(None).cast("timestamp"))
              .withColumn("processed_at", F.current_timestamp()))
        transformed = silver.TRANSFORMS[ds.name](df, refs_cache.get(spark)).persist()
        valid = silver.write(spark, ds, transformed, overwrite_existing=False)
        if ds.silver_topic:
            streaming.to_kafka(valid, ds.silver_topic)
        log.info(f"{ds.name} batch {batch_id} traité")
        transformed.unpersist()
    return handle


def main():
    spark = get_spark("stream_raw_to_silver")
    iceberg.ensure_namespace(spark, "silver")
    raw = streaming.read_topics(spark, [d.topic for d in schemas.TRANSACTIONAL])

    dlq = dlq_stream(raw).select(F.col("country_code").alias("key"),
                                 F.to_json(F.struct("*"), {"timestampFormat": "yyyy-MM-dd HH:mm:ss"}).alias("value"))
    (dlq.writeStream.queryName("dlq").format("kafka")
     .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
     .option("topic", DLQ_TOPIC)
     .option("checkpointLocation", f"{streaming.CHECKPOINT_ROOT}/dlq")
     .trigger(processingTime=streaming.TRIGGER)
     .start())

    for ds in schemas.TRANSACTIONAL:
        valid = (checked(raw, ds).where(F.col("_error") == "")
                 .withWatermark("timestamp", "10 minutes")
                 .dropDuplicatesWithinWatermark([ds.id_col]))
        streaming.start(f"silver_{ds.name}", valid, handler=silver_handler(ds))

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
