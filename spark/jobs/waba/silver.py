from pyspark.sql import Window
from pyspark.sql import functions as F

from waba import config, iceberg


def pseudo(col):
    return F.sha2(F.concat(F.lit(config.pseudo_salt()), col), 256)


def to_eur(amount, currency="currency"):
    rate = (F.when(F.col(currency) == "XOF", F.lit(config.FX_XOF_PER_EUR))
            .when(F.col(currency) == "GHS", F.lit(config.FX_GHS_PER_EUR)))
    return F.round(F.col(amount) / rate, 2)


def latest(df, id_col):
    w = Window.partitionBy(id_col).orderBy(F.col("ingested_at").desc())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


META_COLUMNS = ["source", "batch_ingested_at", "processed_at"]


def finish(out, columns):
    meta = [c for c in META_COLUMNS if c in out.columns]
    return out.select(*columns, *meta, "_orphan")


def orphan_reason(*checks):
    return F.concat_ws(";", *[F.when(cond, F.lit(label)) for cond, label in checks])


def customers(bronze):
    return (latest(bronze, "customer_id")
            .fillna({"segment": "UNKNOWN", "kyc_level": "UNKNOWN", "is_active": False})
            .select("customer_id", "country_code", "entity_type", "segment", "kyc_level", "onboarding_date",
                    "region", "is_active", "ingested_at"))


def accounts(bronze):
    return (latest(bronze, "account_id")
            .fillna({"balance": 0.0, "credit_limit": 0.0, "status": "UNKNOWN"})
            .withColumn("account_ref", pseudo(F.col("account_id")))
            .withColumn("balance_eur", to_eur("balance"))
            .withColumn("credit_limit_eur", to_eur("credit_limit"))
            .select("account_ref", "customer_id", "country_code", "account_type", "currency", "balance",
                    "credit_limit", "balance_eur", "credit_limit_eur", "opened_date", "status", "entity_type",
                    "ingested_at"))


def branches(bronze):
    return latest(bronze, "branch_id").drop("source_file", "ingestion_date")


def products(bronze):
    return latest(bronze, "product_id").fillna({"interest_rate": 0.0}).drop("source_file", "ingestion_date")


BANK_COLUMNS = ["transaction_id", "event_time", "txn_date", "account_ref", "beneficiary_ref", "customer_id",
                "segment", "branch_id", "branch_city", "branch_type", "country_code", "entity_type",
                "transaction_type", "amount", "currency", "amount_eur", "fee_amount", "fee_amount_eur",
                "channel", "transaction_status", "account_balance", "account_credit_limit", "is_outlier"]


def bank(df, refs):
    acc = refs["accounts"].select("account_ref", "customer_id", F.col("balance").alias("account_balance"),
                                  F.col("credit_limit").alias("account_credit_limit"))
    benef = refs["accounts"].select(F.col("account_ref").alias("beneficiary_ref"), F.lit(True).alias("_benef_ok"))
    br = F.broadcast(refs["branches"].select("branch_id", F.col("city").alias("branch_city"), "branch_type"))
    cust = refs["customers"].select("customer_id", "segment")
    out = (
        df.withColumnRenamed("timestamp", "event_time")
        .withColumn("account_ref", pseudo(F.col("account_id")))
        .withColumn("beneficiary_ref", pseudo(F.col("beneficiary_account")))
        .fillna({"fee_amount": 0.0, "channel": "UNKNOWN", "transaction_status": "UNKNOWN", "entity_type": "BANK"})
        .withColumn("txn_date", F.to_date("event_time"))
        .withColumn("amount_eur", to_eur("amount"))
        .withColumn("fee_amount_eur", to_eur("fee_amount"))
        .join(acc, "account_ref", "left")
        .join(benef, "beneficiary_ref", "left")
        .join(br, "branch_id", "left")
        .join(cust, "customer_id", "left")
        .withColumn("is_outlier", (F.col("amount") <= 0) | (F.col("amount_eur") > 1_000_000))
        .withColumn("_orphan", orphan_reason(
            (F.col("account_balance").isNull(), "ORPHAN_ACCOUNT"),
            (F.col("beneficiary_account").isNotNull() & F.col("_benef_ok").isNull(), "ORPHAN_BENEFICIARY"),
            (F.col("branch_city").isNull(), "ORPHAN_BRANCH"),
        ))
    )
    return finish(out, BANK_COLUMNS)


INSURANCE_COLUMNS = ["operation_id", "event_time", "txn_date", "customer_id", "segment", "policy_ref",
                     "country_code", "entity_type", "operation_type", "product_line", "line_group", "amount",
                     "currency", "amount_eur", "claim_status", "processing_days", "is_outlier"]


def insurance(df, refs):
    pol = refs["accounts"].select(F.col("account_ref").alias("policy_ref"), F.lit(True).alias("_pol_ok"))
    cust = refs["customers"].select("customer_id", "segment")
    out = (
        df.withColumnRenamed("timestamp", "event_time")
        .withColumn("policy_ref", pseudo(F.col("account_id")))
        .fillna({"entity_type": "INSURANCE"})
        .withColumn("txn_date", F.to_date("event_time"))
        .withColumn("amount_eur", to_eur("amount"))
        .withColumn("line_group", F.when(F.col("product_line").startswith("IARD"), "IARD").otherwise("VIE"))
        .join(pol, "policy_ref", "left")
        .join(cust, "customer_id", "left")
        .withColumn("is_outlier", (F.col("amount") <= 0) | (F.col("amount_eur") > 1_000_000))
        .withColumn("_orphan", orphan_reason(
            (F.col("_pol_ok").isNull(), "ORPHAN_POLICY"),
            (F.col("segment").isNull(), "ORPHAN_CUSTOMER"),
        ))
    )
    return finish(out, INSURANCE_COLUMNS)


MOBILE_COLUMNS = ["payment_id", "event_time", "txn_date", "sender_id", "receiver_id", "sender_segment",
                  "sender_home_country", "sender_country", "receiver_country", "country_code", "entity_type",
                  "payment_type", "operator", "amount", "currency", "amount_eur", "fee_amount", "fee_amount_eur",
                  "status", "is_cross_border", "is_unusual_country", "is_outlier"]


def mobile(df, refs):
    sender = refs["customers"].select(F.col("customer_id").alias("sender_id"),
                                      F.col("segment").alias("sender_segment"),
                                      F.col("country_code").alias("sender_home_country"))
    receiver = refs["customers"].select(F.col("customer_id").alias("receiver_id"), F.lit(True).alias("_recv_ok"))
    out = (
        df.withColumnRenamed("timestamp", "event_time")
        .fillna({"fee_amount": 0.0, "status": "UNKNOWN", "entity_type": "MOBILE_MONEY"})
        .withColumn("sender_country", F.coalesce("sender_country", "country_code"))
        .withColumn("receiver_country", F.coalesce("receiver_country", "country_code"))
        .withColumn("txn_date", F.to_date("event_time"))
        .withColumn("amount_eur", to_eur("amount"))
        .withColumn("fee_amount_eur", to_eur("fee_amount"))
        .withColumn("is_cross_border", F.col("sender_country") != F.col("receiver_country"))
        .join(sender, "sender_id", "left")
        .join(receiver, "receiver_id", "left")
        .withColumn("is_unusual_country", F.col("sender_country") != F.col("sender_home_country"))
        .withColumn("is_outlier", (F.col("amount") <= 0) | (F.col("amount_eur") > 1_000_000))
        .withColumn("_orphan", orphan_reason(
            (F.col("sender_home_country").isNull(), "ORPHAN_SENDER"),
            (F.col("_recv_ok").isNull(), "ORPHAN_RECEIVER"),
        ))
    )
    return finish(out, MOBILE_COLUMNS)


LOAN_COLUMNS = ["repayment_id", "event_time", "txn_date", "loan_account_ref", "customer_id", "segment",
                "country_code", "entity_type", "loan_type", "amount_due", "amount_paid", "currency",
                "amount_due_eur", "amount_paid_eur", "interest_rate", "interest_received_eur", "loan_outstanding",
                "loan_outstanding_eur", "due_date", "payment_date", "days_overdue", "repayment_status",
                "is_default", "is_outlier"]


def loans(df, refs):
    acc = refs["accounts"].select(F.col("account_ref").alias("loan_account_ref"),
                                  F.col("balance").alias("loan_outstanding"),
                                  F.col("balance_eur").alias("loan_outstanding_eur"))
    cust = refs["customers"].select("customer_id", "segment")
    rates = F.broadcast(refs["products"].where(F.col("product_family") == "LOAN")
                        .groupBy(F.col("product_code").alias("loan_type"))
                        .agg(F.avg("interest_rate").alias("interest_rate")))
    out = (
        df.withColumnRenamed("timestamp", "event_time")
        .withColumn("loan_account_ref", pseudo(F.col("loan_account_id")))
        .fillna({"amount_paid": 0.0, "days_overdue": 0, "loan_type": "UNKNOWN"})
        .withColumn("txn_date", F.to_date("event_time"))
        .withColumn("amount_due_eur", to_eur("amount_due"))
        .withColumn("amount_paid_eur", to_eur("amount_paid"))
        .join(acc, "loan_account_ref", "left")
        .join(cust, "customer_id", "left")
        .join(rates, "loan_type", "left")
        .fillna({"interest_rate": 0.0})
        .withColumn("interest_received_eur", F.round(F.col("amount_paid_eur") * F.col("interest_rate"), 2))
        .withColumn("is_default", (F.col("repayment_status") == "DEFAULT") | (F.col("days_overdue") >= 90))
        .withColumn("is_outlier", (F.col("amount_due") < 0) | (F.col("amount_due_eur") > 1_000_000))
        .withColumn("_orphan", orphan_reason(
            (F.col("loan_outstanding").isNull(), "ORPHAN_LOAN_ACCOUNT"),
            (F.col("segment").isNull(), "ORPHAN_CUSTOMER"),
        ))
    )
    return finish(out, LOAN_COLUMNS)


TRANSFORMS = {
    "bank_transactions": bank,
    "insurance_operations": insurance,
    "mobile_money_payments": mobile,
    "loan_repayments": loans,
}

REFERENTIAL_TRANSFORMS = {
    "customers": customers,
    "accounts": accounts,
    "branches": branches,
    "products": products,
}


def load_refs(spark, namespace="silver"):
    return {name: spark.table(iceberg.table(namespace, name)) for name in REFERENTIAL_TRANSFORMS}


QUARANTINE_COLUMNS = [("dataset", "string"), ("record_id", "string"), ("reason", "string"), ("payload", "string"),
                      ("country_code", "string"), ("quarantined_at", "timestamp")]


def write(spark, ds, transformed, overwrite_existing):
    valid = transformed.where(F.col("_orphan") == "").drop("_orphan")
    target = iceberg.table("silver", ds.name)
    iceberg.ensure_like(spark, valid, target, [F.col("country_code"), F.days("event_time")])
    iceberg.merge(spark, valid, target, ds.id_col, update=overwrite_existing)

    quarantine = iceberg.table("silver", "quarantine")
    iceberg.create_table(spark, quarantine, QUARANTINE_COLUMNS, "dataset")
    (transformed.where(F.col("_orphan") != "")
     .select(F.lit(ds.name).alias("dataset"), F.col(ds.id_col).alias("record_id"),
             F.col("_orphan").alias("reason"), F.to_json(F.struct("*")).alias("payload"),
             "country_code", F.current_timestamp().alias("quarantined_at"))
     .writeTo(quarantine).append())
    return valid
