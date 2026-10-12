import uuid

from pyspark.sql import functions as F

from waba import config


def table(ns, name):
    return f"{config.CATALOG}.{ns}.{name}"


def ensure_namespace(spark, ns):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {config.CATALOG}.{ns}")


def exists(spark, full_name):
    return spark.catalog.tableExists(full_name)


def create_table(spark, full_name, columns, partition_by):
    cols = ", ".join(f"`{c}` {t}" for c, t in columns)
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {full_name} ({cols}) USING iceberg "
        f"PARTITIONED BY ({partition_by}) TBLPROPERTIES ('format-version'='2')"
    )


def merge(spark, df, full_name, id_col, update=False):
    view = f"src_{uuid.uuid4().hex[:8]}"
    df.createOrReplaceTempView(view)
    matched = "WHEN MATCHED THEN UPDATE SET * " if update else ""
    spark.sql(
        f"MERGE INTO {full_name} t USING {view} s "
        f"ON t.`{id_col}` = s.`{id_col}` AND t.country_code = s.country_code "
        f"{matched}WHEN NOT MATCHED THEN INSERT *"
    )
    spark.catalog.dropTempView(view)


def overwrite_partitions(spark, df, full_name, partition_cols):
    if exists(spark, full_name):
        df.writeTo(full_name).overwritePartitions()
    else:
        df.writeTo(full_name).using("iceberg").tableProperty("format-version", "2") \
            .partitionedBy(*[F.col(c) for c in partition_cols]).create()


def watermark(spark, full_name, col, countries):
    if not exists(spark, full_name):
        return None
    row = spark.table(full_name).where(F.col("country_code").isin(countries)).agg(F.max(col)).first()
    return row[0]


def ensure_like(spark, df, full_name, partitions):
    if not exists(spark, full_name):
        df.limit(0).writeTo(full_name).using("iceberg").tableProperty("format-version", "2") \
            .partitionedBy(*partitions).create()
