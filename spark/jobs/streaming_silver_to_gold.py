from datetime import datetime, timedelta

from pyspark.sql import functions as F

from waba import config, iceberg, streaming
from waba.session import get_logger, get_spark

log = get_logger("stream_silver_to_gold")

BANK_SCHEMA = ("transaction_id STRING, event_time STRING, account_ref STRING, country_code STRING, "
               "transaction_type STRING, amount DOUBLE, currency STRING, amount_eur DOUBLE, "
               "transaction_status STRING, account_balance DOUBLE, account_credit_limit DOUBLE")
MOBILE_SCHEMA = ("payment_id STRING, event_time STRING, sender_id STRING, sender_home_country STRING, "
                 "sender_country STRING, country_code STRING, amount_eur DOUBLE, payment_type STRING, "
                 "is_unusual_country BOOLEAN")
INSURANCE_SCHEMA = ("operation_id STRING, event_time STRING, customer_id STRING, country_code STRING, "
                    "operation_type STRING, product_line STRING, amount_eur DOUBLE")

DEBIT_TYPES = ["TRANSFER", "PAYMENT", "WITHDRAWAL", "INTERNATIONAL_WIRE"]
BIG_TXN_EUR = config.FRAUD_BIG_TXN_XOF / config.FX_XOF_PER_EUR


def silver_stream(spark, topic, schema):
    return (streaming.read_topics(spark, [topic])
            .select(F.from_json("value", schema).alias("e")).select("e.*")
            .withColumn("event_time", F.to_timestamp("event_time"))
            .where(F.col("event_time").isNotNull()))


def load_reference(spark):
    accounts = spark.table(iceberg.table("silver", "accounts"))
    deposits = (accounts.where(F.col("account_type").isin("CURRENT", "SAVINGS"))
                .groupBy("country_code").agg(F.sum("balance_eur").alias("deposits_eur")))
    since = datetime.utcnow() - timedelta(days=365)
    ins = spark.table(iceberg.table("silver", "insurance_operations")) \
        .where(F.col("event_time") >= F.lit(since))
    premiums = ins.where(F.col("operation_type").isin("PREMIUM_PAYMENT", "POLICY_RENEWAL"))
    by_customer = premiums.groupBy("customer_id", "product_line") \
        .agg(F.sum("amount_eur").alias("customer_annual_premium_eur"))
    by_product = premiums.groupBy("country_code", "product_line") \
        .agg((F.avg("amount_eur") * 12).alias("product_annual_premium_eur"))
    return {"deposits": deposits, "premiums_customer": by_customer, "premiums_product": by_product}


reference = streaming.TtlCache(load_reference, ttl_seconds=600)


def big_txn_handler(batch_df, batch_id):
    hits = batch_df.where(F.col("txn_count") >= config.FRAUD_MIN_TXN_COUNT)
    alerts = streaming.alert(
        hits, "FRAUD", "MULTIPLE_BIG_TXN_5MIN", F.col("account_ref"), F.col("total_eur"), F.col("window.start"),
        F.to_json(F.struct("txn_count", "window", "transaction_ids")),
    )
    streaming.publish_alerts(batch_df.sparkSession, alerts, "gold-fraud-alerts", "fraud_alerts")


def bank_handler(batch_df, batch_id):
    spark = batch_df.sparkSession
    threshold = (F.when(F.col("currency") == "XOF", F.lit(config.AML_THRESHOLD_XOF))
                 .when(F.col("currency") == "GHS", F.lit(config.AML_THRESHOLD_GHS)))
    aml = batch_df.where(F.col("transaction_type").isin("TRANSFER", "INTERNATIONAL_WIRE")
                         & (F.col("transaction_status") == "SUCCESS") & (F.col("amount") >= threshold))
    streaming.publish_alerts(spark, streaming.alert(
        aml, "AML", "BCEAO_DECLARATION_THRESHOLD", F.col("account_ref"), F.col("amount_eur"), F.col("event_time"),
        F.to_json(F.struct("transaction_id", "transaction_type", "amount", "currency")),
        alert_id=F.col("transaction_id"),
    ), "gold-aml-events", "aml_events")

    breach = batch_df.where(F.col("transaction_type").isin(DEBIT_TYPES)
                            & (F.col("amount") > F.col("account_balance") + F.col("account_credit_limit")))
    streaming.publish_alerts(spark, streaming.alert(
        breach, "LIQUIDITY", "ACCOUNT_LIMIT_BREACH", F.col("account_ref"), F.col("amount_eur"), F.col("event_time"),
        F.to_json(F.struct("transaction_id", "amount", "account_balance", "account_credit_limit")),
    ), "gold-liquidity-alerts", "liquidity_alerts")


def country_liquidity_handler(batch_df, batch_id):
    spark = batch_df.sparkSession
    deposits = reference.get(spark)["deposits"]
    coverage = (batch_df.join(deposits, "country_code")
                .withColumn("coverage_ratio", (F.col("deposits_eur") + F.col("net_flow_eur")) / F.col("deposits_eur")))
    low = coverage.where(F.col("coverage_ratio") < config.LIQUIDITY_MIN_COVERAGE)
    hour = F.date_trunc("hour", F.current_timestamp())
    streaming.publish_alerts(spark, streaming.alert(
        low, "LIQUIDITY", "COUNTRY_COVERAGE_BELOW_MIN", F.col("country_code"), F.col("net_flow_eur"), hour,
        F.to_json(F.struct("deposits_eur", "net_flow_eur", "coverage_ratio")),
    ), "gold-liquidity-alerts", "liquidity_alerts")


def mobile_handler(batch_df, batch_id):
    odd = batch_df.where(F.col("is_unusual_country"))
    streaming.publish_alerts(batch_df.sparkSession, streaming.alert(
        odd, "FRAUD", "MOBILE_MONEY_UNUSUAL_COUNTRY", F.col("sender_id"), F.col("amount_eur"), F.col("event_time"),
        F.to_json(F.struct("payment_id", "sender_home_country", "sender_country", "payment_type")),
    ), "gold-fraud-alerts", "fraud_alerts")


def insurance_handler(batch_df, batch_id):
    spark = batch_df.sparkSession
    ref = reference.get(spark)
    claims = (batch_df.where(F.col("operation_type").isin("CLAIM_SUBMISSION", "CLAIM_PAYMENT"))
              .join(ref["premiums_customer"], ["customer_id", "product_line"], "left")
              .join(ref["premiums_product"], ["country_code", "product_line"], "left")
              .withColumn("annual_premium_eur",
                          F.greatest("customer_annual_premium_eur", "product_annual_premium_eur")))
    big = claims.where(F.col("amount_eur") > 3 * F.col("annual_premium_eur"))
    streaming.publish_alerts(spark, streaming.alert(
        big, "FRAUD", "CLAIM_ABOVE_3X_ANNUAL_PREMIUM", F.col("customer_id"), F.col("amount_eur"), F.col("event_time"),
        F.to_json(F.struct("operation_id", "product_line", "annual_premium_eur")),
    ), "gold-fraud-alerts", "fraud_alerts")


def main():
    spark = get_spark("stream_silver_to_gold")
    iceberg.ensure_namespace(spark, "gold")
    bank = silver_stream(spark, "silver-bank-transactions", BANK_SCHEMA)
    mobile = silver_stream(spark, "silver-mobile-money", MOBILE_SCHEMA)
    insurance = silver_stream(spark, "silver-insurance-operations", INSURANCE_SCHEMA)

    big_txn = (bank.where((F.col("amount_eur") > BIG_TXN_EUR) & (F.col("transaction_status") != "FAILED"))
               .withWatermark("event_time", "10 minutes")
               .groupBy(F.window("event_time", "5 minutes", "1 minute"), "account_ref", "country_code")
               .agg(F.count("*").alias("txn_count"), F.sum("amount_eur").alias("total_eur"),
                    F.collect_set("transaction_id").alias("transaction_ids")))
    streaming.start("fraud_big_txn", big_txn, big_txn_handler, output_mode="update")

    signed = F.when(F.col("transaction_type") == "DEPOSIT", F.col("amount_eur")).otherwise(-F.col("amount_eur"))
    net_flow = (bank.where(F.col("transaction_status") == "SUCCESS")
                .groupBy("country_code").agg(F.sum(signed).alias("net_flow_eur")))
    streaming.start("liquidity_country", net_flow, country_liquidity_handler, output_mode="update")

    streaming.start("bank_rules", bank, bank_handler)
    streaming.start("fraud_mobile", mobile, mobile_handler)
    streaming.start("fraud_insurance", insurance, insurance_handler)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
