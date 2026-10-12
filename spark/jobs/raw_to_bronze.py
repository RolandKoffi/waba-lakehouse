import argparse

from pyspark.sql import functions as F

from waba import config, iceberg, schemas
from waba.session import get_logger, get_spark

log = get_logger("raw_to_bronze")


def load(spark, ds, countries, full):
    source = iceberg.table("raw", ds.name)
    target = iceberg.table("bronze", ds.name)
    if not iceberg.exists(spark, source):
        log.info(f"{ds.name}: table raw absente")
        return
    df = spark.table(source).where(F.col("country_code").isin(countries))
    last = None if full else iceberg.watermark(spark, target, "ingested_at", countries)
    if last is not None:
        df = df.where(F.col("ingested_at") > F.lit(last))
    df = df.withColumn("ingestion_date", F.to_date("ingested_at"))

    columns = ds.columns + [("source_file", "string"), ("ingested_at", "timestamp"), ("ingestion_date", "date")]
    iceberg.create_table(spark, target, columns, "country_code, ingestion_date")
    iceberg.merge(spark, df, target, ds.id_col, update=ds.referential)
    log.info(f"{ds.name}: bronze mis à jour depuis {last}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--countries", default="ALL")
    parser.add_argument("--full", action="store_true")
    args = parser.parse_args()
    countries = config.parse_countries(args.countries)

    spark = get_spark("raw_to_bronze")
    iceberg.ensure_namespace(spark, "bronze")
    for ds in schemas.ALL:
        load(spark, ds, countries, args.full)
    spark.stop()


if __name__ == "__main__":
    main()
