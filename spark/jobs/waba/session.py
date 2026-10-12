import logging

from pyspark.sql import SparkSession

from waba import config


def get_logger(name):
    logging.basicConfig(
        level=logging.INFO,
        format='{"ts":"%(asctime)s","level":"%(levelname)s","job":"%(name)s","msg":"%(message)s"}',
    )
    return logging.getLogger(name)


def get_spark(app_name, extra=None):
    c = config.CATALOG
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{c}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{c}.type", "rest")
        .config(f"spark.sql.catalog.{c}.uri", config.REST_URI)
        .config(f"spark.sql.catalog.{c}.warehouse", f"s3://{config.LAKEHOUSE_BUCKET}/")
        .config(f"spark.sql.catalog.{c}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{c}.s3.endpoint", config.S3_ENDPOINT)
        .config(f"spark.sql.catalog.{c}.s3.path-style-access", "true")
        .config(f"spark.sql.catalog.{c}.s3.access-key-id", config.access_key())
        .config(f"spark.sql.catalog.{c}.s3.secret-access-key", config.secret_key())
        .config(f"spark.sql.catalog.{c}.client.region", config.AWS_REGION)
        .config(f"spark.sql.catalog.{c}.cache-enabled", "false")
        .config("spark.sql.defaultCatalog", c)
        .config("spark.hadoop.fs.s3a.endpoint", config.S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", config.access_key())
        .config("spark.hadoop.fs.s3a.secret.key", config.secret_key())
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )
    for key, value in (extra or {}).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
