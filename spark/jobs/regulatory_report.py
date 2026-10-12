import argparse
from datetime import date, datetime, timedelta

from pyspark.sql import functions as F

from waba import config, iceberg
from waba.session import get_logger, get_spark

log = get_logger("regulatory_report")


def aml_threshold():
    return (F.when(F.col("currency") == "XOF", F.lit(config.AML_THRESHOLD_XOF))
            .when(F.col("currency") == "GHS", F.lit(config.AML_THRESHOLD_GHS)))


def bceao_report(spark, d, countries):
    bank = spark.table(iceberg.table("silver", "bank_transactions")) \
        .where((F.col("txn_date") == F.lit(d)) & F.col("country_code").isin(countries))
    mobile = spark.table(iceberg.table("silver", "mobile_money_payments")) \
        .where((F.col("txn_date") == F.lit(d)) & F.col("country_code").isin(countries))
    npl = spark.table(iceberg.table("gold", "npl_ratio_by_country")) \
        .where((F.col("loan_type") == "ALL") & (F.col("month") == F.trunc(F.lit(d), "month"))) \
        .select("country_code", "npl_ratio", "total_outstanding_eur", "defaulted_outstanding_eur")

    ok = F.col("transaction_status") == "SUCCESS"
    declarable = ok & F.col("transaction_type").isin("TRANSFER", "INTERNATIONAL_WIRE") & \
        (F.col("amount") >= aml_threshold())
    bank_agg = bank.groupBy("country_code").agg(
        F.count("*").alias("bank_txn_count"),
        F.round(F.sum(F.when(ok, F.col("amount_eur"))), 2).alias("bank_amount_eur"),
        F.sum(declarable.cast("int")).alias("aml_declarable_count"),
        F.round(F.sum(F.when(declarable, F.col("amount_eur"))), 2).alias("aml_declarable_amount_eur"),
    )
    mm_agg = mobile.groupBy("country_code").agg(
        F.count("*").alias("mobile_money_txn_count"),
        F.round(F.sum(F.when(F.col("status") == "SUCCESS", F.col("amount_eur"))), 2).alias("mobile_money_amount_eur"),
    )
    return (bank_agg.join(mm_agg, "country_code", "full").join(npl, "country_code", "left")
            .withColumn("npl_breach", F.col("npl_ratio") > config.NPL_THRESHOLD)
            .withColumn("report_date", F.lit(d)))


def cima_report(spark, d, countries):
    ins = spark.table(iceberg.table("silver", "insurance_operations")) \
        .where(F.col("country_code").isin(countries)
               & (F.col("txn_date") >= F.trunc(F.lit(d), "month")) & (F.col("txn_date") <= F.lit(d)))
    premium = F.col("operation_type").isin("PREMIUM_PAYMENT", "POLICY_RENEWAL")
    paid = (F.col("operation_type") == "CLAIM_PAYMENT") & (F.col("claim_status") == "PAID")
    return (ins.groupBy("country_code", "product_line")
            .agg(F.round(F.sum(F.when(premium, F.col("amount_eur")).otherwise(0)), 2).alias("mtd_premiums_eur"),
                 F.round(F.sum(F.when(paid, F.col("amount_eur")).otherwise(0)), 2).alias("mtd_claims_paid_eur"),
                 F.sum((F.col("claim_status") == "PENDING").cast("int")).alias("pending_claims"),
                 F.round(F.avg("processing_days"), 2).alias("avg_processing_days"))
            .withColumn("mtd_loss_ratio", F.round(F.col("mtd_claims_paid_eur") / F.col("mtd_premiums_eur"), 4))
            .withColumn("loss_ratio_alert", F.col("mtd_loss_ratio") > config.LOSS_RATIO_THRESHOLD)
            .withColumn("report_date", F.lit(d)))


def publish(spark, df, name, d, countries):
    target = iceberg.table("gold", name)
    df = df.withColumn("generated_at", F.current_timestamp())
    if iceberg.exists(spark, target):
        in_list = ", ".join(f"'{c}'" for c in countries)
        spark.sql(f"DELETE FROM {target} WHERE report_date = DATE '{d}' AND country_code IN ({in_list})")
        df.writeTo(target).append()
    else:
        iceberg.overwrite_partitions(spark, df, target, ["country_code"])
    df.coalesce(1).write.mode("overwrite").option("header", "true") \
        .csv(f"s3a://{config.LAKEHOUSE_BUCKET}/reports/{name}/report_date={d}")
    log.info(f"gold.{name} publié pour {d}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--report-date", default=(datetime.utcnow().date() - timedelta(days=1)).isoformat())
    parser.add_argument("--countries", default="ALL")
    args = parser.parse_args()
    d = date.fromisoformat(args.report_date)
    countries = config.parse_countries(args.countries)

    spark = get_spark("regulatory_report")
    iceberg.ensure_namespace(spark, "gold")
    publish(spark, bceao_report(spark, d, countries), "regulatory_bceao_daily", d, countries)
    publish(spark, cima_report(spark, d, countries), "regulatory_cima_daily", d, countries)
    spark.stop()


if __name__ == "__main__":
    main()
