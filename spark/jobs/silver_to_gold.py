import argparse

from pyspark.sql import functions as F

from waba import config, gold, iceberg, schemas
from waba.session import get_logger, get_spark

log = get_logger("silver_to_gold")


def load_silver(spark, countries):
    tables = {}
    for ds in schemas.TRANSACTIONAL:
        df = spark.table(iceberg.table("silver", ds.name))
        tables[ds.name] = df.where(F.col("country_code").isin(countries) & ~F.col("is_outlier")).cache()
    return tables


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--countries", default="ALL")
    parser.add_argument("--kpis", default="all")
    args = parser.parse_args()
    countries = config.parse_countries(args.countries)
    names = list(gold.KPIS) if args.kpis == "all" else args.kpis.split(",")

    spark = get_spark("silver_to_gold")
    iceberg.ensure_namespace(spark, "gold")
    tables = load_silver(spark, countries)
    for name in names:
        df = gold.KPIS[name](tables).withColumn("computed_at", F.current_timestamp())
        iceberg.overwrite_partitions(spark, df, iceberg.table("gold", name), ["country_code"])
        log.info(f"gold.{name} recalculé pour {countries}")
    spark.stop()


if __name__ == "__main__":
    main()
