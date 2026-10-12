import argparse

from pyspark.sql import functions as F

from waba import config, iceberg, schemas, silver
from waba.session import get_logger, get_spark

log = get_logger("bronze_to_silver")


def build_referentials(spark):
    for name, transform in silver.REFERENTIAL_TRANSFORMS.items():
        source = iceberg.table("bronze", name)
        if not iceberg.exists(spark, source):
            raise RuntimeError(f"Référentiel {source} absent : lancer l'ingestion des référentiels d'abord")
        df = transform(spark.table(source))
        df.writeTo(iceberg.table("silver", name)).using("iceberg") \
            .partitionedBy(F.col("country_code")).createOrReplace()
        log.info(f"silver.{name} reconstruit")


def build_transactional(spark, ds, refs, countries, full):
    source = iceberg.table("bronze", ds.name)
    if not iceberg.exists(spark, source):
        log.info(f"{ds.name}: bronze absent")
        return
    target = iceberg.table("silver", ds.name)
    df = spark.table(source).where(F.col("country_code").isin(countries))
    last = None if full else iceberg.watermark(spark, target, "batch_ingested_at", countries)
    if last is not None:
        df = df.where(F.col("ingested_at") > F.lit(last))
    df = (silver.latest(df, ds.id_col)
          .withColumn("source", F.lit("batch"))
          .withColumn("batch_ingested_at", F.col("ingested_at"))
          .withColumn("processed_at", F.current_timestamp()))
    transformed = silver.TRANSFORMS[ds.name](df, refs).persist()
    silver.write(spark, ds, transformed, overwrite_existing=True)
    transformed.unpersist()
    log.info(f"{ds.name}: silver mis à jour depuis {last}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--countries", default="ALL")
    parser.add_argument("--full", action="store_true")
    args = parser.parse_args()
    countries = config.parse_countries(args.countries)

    spark = get_spark("bronze_to_silver")
    iceberg.ensure_namespace(spark, "silver")
    build_referentials(spark)
    refs = {k: v.cache() for k, v in silver.load_refs(spark).items()}
    for ds in schemas.TRANSACTIONAL:
        build_transactional(spark, ds, refs, countries, args.full)
    spark.stop()


if __name__ == "__main__":
    main()
