from pyspark.sql import Window
from pyspark.sql import functions as F

from waba import config
from waba.config import UEMOA


def month(col="event_time"):
    return F.to_date(F.date_trunc("month", F.col(col)))


def daily_transaction_volume(bank):
    ok = F.col("transaction_status") == "SUCCESS"
    return (bank.groupBy("txn_date", "country_code", "entity_type", "transaction_type")
            .agg(F.count("*").alias("txn_count"),
                 F.sum(ok.cast("int")).alias("success_count"),
                 F.sum((F.col("transaction_status") == "FAILED").cast("int")).alias("failed_count"),
                 F.round(F.sum(F.when(ok, F.col("amount"))), 2).alias("total_amount_local"),
                 F.round(F.sum(F.when(ok, F.col("amount_eur"))), 2).alias("total_amount_eur"),
                 F.first("currency").alias("currency")))


def npl_ratio_by_country(loans):
    w = Window.partitionBy("loan_account_ref", "month").orderBy(F.col("event_time").desc())
    last = (loans.withColumn("month", month())
            .withColumn("_rn", F.row_number().over(w)).where("_rn = 1"))

    def agg(df, keys):
        return (df.groupBy(*keys)
                .agg(F.countDistinct("loan_account_ref").alias("loans_count"),
                     F.sum(F.col("is_default").cast("int")).alias("defaulted_count"),
                     F.round(F.sum("loan_outstanding_eur"), 2).alias("total_outstanding_eur"),
                     F.round(F.sum(F.when(F.col("is_default"), F.col("loan_outstanding_eur")).otherwise(0)), 2)
                     .alias("defaulted_outstanding_eur")))

    by_type = agg(last, ["month", "country_code", "loan_type"])
    all_types = agg(last, ["month", "country_code"]).withColumn("loan_type", F.lit("ALL"))
    return (by_type.unionByName(all_types)
            .withColumn("npl_ratio", F.round(F.col("defaulted_outstanding_eur") / F.col("total_outstanding_eur"), 4))
            .withColumn("above_bceao_threshold", F.col("npl_ratio") > config.NPL_THRESHOLD))


def customer_arpu_monthly(bank, mobile, loans):
    ok = F.col("transaction_status") == "SUCCESS"
    revenues = (
        bank.where(ok).select("customer_id", "country_code", "segment", month().alias("month"),
                              F.col("fee_amount_eur").alias("revenue_eur"))
        .unionByName(mobile.where(F.col("status") == "SUCCESS")
                     .select(F.col("sender_id").alias("customer_id"), "country_code",
                             F.col("sender_segment").alias("segment"), month().alias("month"),
                             F.col("fee_amount_eur").alias("revenue_eur")))
        .unionByName(loans.select("customer_id", "country_code", "segment", month().alias("month"),
                                  F.col("interest_received_eur").alias("revenue_eur")))
    )
    return (revenues.groupBy("month", "country_code", "segment")
            .agg(F.round(F.sum("revenue_eur"), 2).alias("total_revenue_eur"),
                 F.countDistinct("customer_id").alias("active_customers"))
            .withColumn("arpu_eur", F.round(F.col("total_revenue_eur") / F.col("active_customers"), 2)))


def loss_ratio_by_product(insurance):
    premium = F.col("operation_type").isin("PREMIUM_PAYMENT", "POLICY_RENEWAL")
    paid = (F.col("operation_type") == "CLAIM_PAYMENT") & (F.col("claim_status") == "PAID")
    return (insurance.groupBy(month().alias("month"), "country_code", "product_line")
            .agg(F.round(F.sum(F.when(premium, F.col("amount_eur")).otherwise(0)), 2).alias("premiums_eur"),
                 F.round(F.sum(F.when(paid, F.col("amount_eur")).otherwise(0)), 2).alias("claims_paid_eur"),
                 F.sum(paid.cast("int")).alias("claims_paid_count"))
            .withColumn("loss_ratio", F.round(F.col("claims_paid_eur") / F.col("premiums_eur"), 4))
            .withColumn("above_cima_threshold", F.col("loss_ratio") > config.LOSS_RATIO_THRESHOLD))


def claims_processing_time(insurance):
    claims = insurance.where(F.col("claim_status").isNotNull() & F.col("processing_days").isNotNull())
    return (claims.groupBy(month().alias("month"), "country_code", "line_group")
            .agg(F.count("*").alias("claims_count"),
                 F.round(F.avg("processing_days"), 2).alias("avg_processing_days"),
                 F.percentile_approx("processing_days", 0.5).alias("median_processing_days"),
                 F.max("processing_days").alias("max_processing_days")))


def mobile_money_daily_flow(mobile):
    ok = F.col("status") == "SUCCESS"
    return (mobile.groupBy("txn_date", "country_code")
            .agg(F.count("*").alias("txn_count"),
                 F.round(F.sum(F.when(ok, F.col("amount_eur"))), 2).alias("total_amount_eur"),
                 F.round(F.avg((F.col("status") == "FAILED").cast("double")), 4).alias("failure_rate"),
                 F.countDistinct("sender_id").alias("active_users")))


def cross_border_transfers(mobile):
    flows = mobile.where((F.col("payment_type") == "CROSS_BORDER_TRANSFER") & F.col("is_cross_border")
                         & F.col("sender_country").isin(UEMOA) & F.col("receiver_country").isin(UEMOA)
                         & (F.col("status") == "SUCCESS"))
    weekly = (flows.groupBy(F.to_date(F.date_trunc("week", "event_time")).alias("week_start"),
                            F.col("sender_country").alias("country_code"), "receiver_country")
              .agg(F.count("*").alias("transfers_count"),
                   F.round(F.sum("amount_eur"), 2).alias("total_amount_eur"),
                   F.round(F.avg("amount_eur"), 2).alias("avg_amount_eur"))
              .withColumn("corridor", F.concat_ws("-", "country_code", "receiver_country")))
    w = Window.partitionBy("corridor").orderBy("week_start")
    previous = F.lag("total_amount_eur").over(w)
    return weekly.withColumn("wow_growth", F.round((F.col("total_amount_eur") - previous) / previous, 4))


KPIS = {
    "daily_transaction_volume": lambda t: daily_transaction_volume(t["bank_transactions"]),
    "npl_ratio_by_country": lambda t: npl_ratio_by_country(t["loan_repayments"]),
    "customer_arpu_monthly": lambda t: customer_arpu_monthly(t["bank_transactions"], t["mobile_money_payments"],
                                                             t["loan_repayments"]),
    "loss_ratio_by_product": lambda t: loss_ratio_by_product(t["insurance_operations"]),
    "claims_processing_time": lambda t: claims_processing_time(t["insurance_operations"]),
    "mobile_money_daily_flow": lambda t: mobile_money_daily_flow(t["mobile_money_payments"]),
    "cross_border_transfers": lambda t: cross_border_transfers(t["mobile_money_payments"]),
}
