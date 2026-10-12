import time

from pyspark.sql import functions as F

from waba import config, iceberg

CHECKPOINT_ROOT = config.env("CHECKPOINT_ROOT", "/opt/checkpoints")
TRIGGER = config.env("STREAM_TRIGGER", "10 seconds")

ALERT_COLUMNS = ["alert_id", "alert_type", "rule", "country_code", "entity_ref", "amount_eur", "event_time",
                 "detected_at", "details"]


def read_topics(spark, topics):
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
            .option("subscribe", ",".join(topics))
            .option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", 50000)
            .option("failOnDataLoss", "false")
            .load()
            .select("topic", F.col("value").cast("string").alias("value"), F.col("timestamp").alias("kafka_ts")))


def to_kafka(df, topic, key_col="country_code"):
    (df.select(F.col(key_col).cast("string").alias("key"),
               F.to_json(F.struct("*"), {"timestampFormat": "yyyy-MM-dd HH:mm:ss"}).alias("value"))
     .write.format("kafka")
     .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
     .option("topic", topic)
     .save())


def start(query_name, df, handler, output_mode="append"):
    writer = (df.writeStream.queryName(query_name)
              .option("checkpointLocation", f"{CHECKPOINT_ROOT}/{query_name}")
              .outputMode(output_mode)
              .trigger(processingTime=TRIGGER))
    return writer.foreachBatch(handler).start()


class TtlCache:
    def __init__(self, loader, ttl_seconds=600):
        self.loader = loader
        self.ttl = ttl_seconds
        self.value = None
        self.loaded_at = 0

    def get(self, spark):
        if self.value is None or time.time() - self.loaded_at > self.ttl:
            old = self.value
            self.value = {k: v.cache() for k, v in self.loader(spark).items()}
            self.loaded_at = time.time()
            for df in (old or {}).values():
                df.unpersist()
        return self.value


def alert(df, alert_type, rule, entity_ref, amount_eur, event_time, details, alert_id=None):
    alert_id = alert_id if alert_id is not None else F.sha2(
        F.concat_ws("|", F.lit(rule), entity_ref, event_time.cast("string")), 256)
    return df.select(
        alert_id.alias("alert_id"),
        F.lit(alert_type).alias("alert_type"),
        F.lit(rule).alias("rule"),
        F.col("country_code"),
        entity_ref.alias("entity_ref"),
        F.round(amount_eur, 2).alias("amount_eur"),
        event_time.cast("timestamp").alias("event_time"),
        F.current_timestamp().alias("detected_at"),
        details.alias("details"),
    )


def publish_alerts(spark, alerts, topic, table_name):
    alerts = alerts.persist()
    if alerts.take(1):
        to_kafka(alerts, topic)
        target = iceberg.table("gold", table_name)
        iceberg.ensure_like(spark, alerts, target, [F.col("country_code"), F.days("event_time")])
        iceberg.merge(spark, alerts.dropDuplicates(["alert_id"]), target, "alert_id", update=True)
    alerts.unpersist()
