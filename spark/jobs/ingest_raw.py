import argparse

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from waba import config, iceberg, schemas, storage
from waba.session import get_logger, get_spark
from waba.validation import CORRUPT_COL, split_valid, validate

NS = "raw"
log = get_logger("ingest_raw")

REJECT_COLUMNS = [("dataset", "string"), ("source_file", "string"), ("error", "string"),
                  ("raw_record", "string"), ("country_code", "string"), ("ingested_at", "timestamp")]


def raw_columns(ds):
    return ds.columns + [("source_file", "string"), ("ingested_at", "timestamp")]


def partition_spec(ds):
    return f"country_code, days(`{ds.ts_col}`)" if ds.ts_col else "country_code"


def keys_for(ds, bucket, countries):
    if ds.referential:
        return storage.list_csv(bucket, f"referentials/{ds.name}.csv")
    return [k for cc in countries for k in storage.list_csv(bucket, f"{cc}/{ds.folder}/")]


def read_csv(spark, ds, paths):
    schema = StructType([StructField(c, StringType()) for c in ds.column_names] +
                        [StructField(CORRUPT_COL, StringType())])
    return (
        spark.read.schema(schema)
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_COL)
        .csv(paths)
        .withColumn("source_file", F.col("_metadata.file_path"))
    )


def ingest(spark, ds, countries, bucket, do_archive):
    keys = keys_for(ds, bucket, countries)
    if not keys:
        log.info(f"{ds.name}: aucun fichier")
        return 0
    validated = validate(read_csv(spark, ds, [f"s3a://{bucket}/{k}" for k in keys]), ds).cache()
    valid, rejected = split_valid(validated)
    valid = valid.dropDuplicates([ds.id_col]).withColumn("ingested_at", F.current_timestamp())

    target = iceberg.table(NS, ds.name)
    iceberg.create_table(spark, target, raw_columns(ds), partition_spec(ds))
    iceberg.merge(spark, valid, target, ds.id_col, update=ds.referential)

    n_rejected = rejected.count()
    if n_rejected:
        rejects = rejected.select(
            F.lit(ds.name).alias("dataset"), "source_file", F.col("_error").alias("error"),
            F.col("_raw").alias("raw_record"), "country_code", F.current_timestamp().alias("ingested_at"),
        )
        rejects.writeTo(iceberg.table(NS, "rejected_records")).append()
    log.info(f"{ds.name}: {len(keys)} fichier(s), {validated.count() - n_rejected} valides, {n_rejected} rejetées")
    validated.unpersist()

    if do_archive and bucket == config.RAW_BUCKET:
        storage.archive(keys, log)
    return len(keys)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--datasets", default="all")
    parser.add_argument("--countries", default="ALL")
    parser.add_argument("--bucket", default=config.RAW_BUCKET)
    parser.add_argument("--no-archive", action="store_true")
    args = parser.parse_args()

    countries = config.parse_countries(args.countries)
    names = [d.name for d in schemas.ALL] if args.datasets == "all" else args.datasets.split(",")

    spark = get_spark("ingest_raw")
    iceberg.ensure_namespace(spark, NS)
    iceberg.create_table(spark, iceberg.table(NS, "rejected_records"), REJECT_COLUMNS, "dataset")
    for name in names:
        ingest(spark, schemas.BY_NAME[name], countries, args.bucket, not args.no_archive)
    spark.stop()


if __name__ == "__main__":
    main()
