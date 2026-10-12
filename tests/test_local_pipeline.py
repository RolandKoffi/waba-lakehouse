import os
import sys
import tempfile
from datetime import datetime, timedelta

import numpy as np

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path[:0] = [os.path.join(ROOT, "spark", "jobs"), os.path.join(ROOT, "generator")]
os.environ.setdefault("MINIO_ROOT_USER", "test")
os.environ.setdefault("MINIO_ROOT_PASSWORD", "test")
os.environ.setdefault("PSEUDO_SALT", "test-salt")

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

import data_gen  # noqa: E402
import streaming_silver_to_gold as job2  # noqa: E402
from ingest_raw import read_csv  # noqa: E402
from waba import config, gold, schemas, silver  # noqa: E402
from waba.validation import split_valid, validate  # noqa: E402


def write_inputs(folder):
    customers = data_gen.generate_customers(20_000)
    accounts = data_gen.generate_accounts(customers, 40_000)
    refs = data_gen.ReferentialStore(customers, accounts, data_gen.generate_branches(), data_gen.generate_products())
    rng = np.random.default_rng(7)
    end = datetime(2026, 10, 1)
    start = end - timedelta(days=90)
    paths = {}
    for name in ["customers", "accounts", "branches", "products"]:
        p = os.path.join(folder, f"{name}.csv")
        open(p, "wb").write(data_gen.to_csv_bytes(getattr(refs, name)))
        paths[name] = [p]
    sizes = {"bank_transactions": 4000, "insurance_operations": 6000, "mobile_money_payments": 3000,
             "loan_repayments": 6000}
    for ds, gen in data_gen.GENERATORS.items():
        paths[ds] = []
        for cc in data_gen.COUNTRIES:
            df = data_gen.corrupt_rows(gen(refs, cc, sizes[ds], start, end, rng, anomalies=True), rng, 0.002)
            p = os.path.join(folder, f"{ds}_{cc}.csv")
            open(p, "wb").write(data_gen.to_csv_bytes(df))
            paths[ds].append(p)
    with open(paths["bank_transactions"][0], "a") as f:
        f.write("broken,line,with,too,few,columns\n")
        f.write("dup-id,2026-08-01T10:00:00,WABA-CI-A-9999999,WABA-CI-A-0000001,WABA-CI-B-001,CI,TRANSFER,"
                "1000,XOF,BRANCH,SUCCESS,1,BANK\n")
        f.write("bad-ccy,2026-08-01T10:00:00,WABA-CI-A-0000001,WABA-CI-A-0000001,WABA-CI-B-001,CI,TRANSFER,"
                "1000,GHS,BRANCH,SUCCESS,1,BANK\n")
    return paths


def main():
    spark = (SparkSession.builder.master("local[4]").appName("test")
             .config("spark.sql.session.timeZone", "UTC").config("spark.ui.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    folder = tempfile.mkdtemp()
    paths = write_inputs(folder)

    bronze = {}
    for ds in schemas.ALL:
        v = validate(read_csv(spark, ds, paths[ds.name]), ds).cache()
        valid, rejected = split_valid(v)
        bronze[ds.name] = valid.dropDuplicates([ds.id_col]).withColumn("ingested_at", F.current_timestamp())
        print(f"[ingest] {ds.name}: valid={valid.count()} rejected={rejected.count()}")
        if ds.name == "bank_transactions":
            rejected.groupBy("_error").count().show(truncate=False)

    refs = {n: t(bronze[n]).cache() for n, t in silver.REFERENTIAL_TRANSFORMS.items()}
    silver_tables = {}
    for ds in schemas.TRANSACTIONAL:
        out = silver.TRANSFORMS[ds.name](silver.latest(bronze[ds.name], ds.id_col), refs).cache()
        orphans = out.where(F.col("_orphan") != "")
        print(f"[silver] {ds.name}: rows={out.count()} orphans={orphans.count()}")
        if orphans.count():
            orphans.select(ds.id_col, "_orphan").show(truncate=False)
        silver_tables[ds.name] = out.where(F.col("_orphan") == "").drop("_orphan").where(~F.col("is_outlier"))

    for name, fn in gold.KPIS.items():
        df = fn(silver_tables)
        print(f"[gold] {name}: {df.count()} lignes")
    gold.npl_ratio_by_country(silver_tables["loan_repayments"]).where("loan_type = 'ALL'") \
        .groupBy("country_code").agg(F.round(F.avg("npl_ratio"), 4).alias("avg_npl")).orderBy("country_code").show()
    lr = gold.loss_ratio_by_product(silver_tables["insurance_operations"]).groupBy("country_code") \
        .agg(F.round(F.sum("claims_paid_eur") / F.sum("premiums_eur"), 3).alias("loss_ratio"))
    lr.orderBy("country_code").show()
    gold.cross_border_transfers(silver_tables["mobile_money_payments"]).orderBy("corridor", "week_start").show(5)

    bank = (silver_tables["bank_transactions"]
            .select(F.from_json(F.to_json(F.struct("*"), {"timestampFormat": "yyyy-MM-dd HH:mm:ss"}),
                                job2.BANK_SCHEMA).alias("e")).select("e.*")
            .withColumn("event_time", F.to_timestamp("event_time")))
    bursts = (bank.where((F.col("amount_eur") > job2.BIG_TXN_EUR) & (F.col("transaction_status") != "FAILED"))
              .groupBy(F.window("event_time", "5 minutes", "1 minute"), "account_ref", "country_code")
              .agg(F.count("*").alias("txn_count"))
              .where(F.col("txn_count") >= config.FRAUD_MIN_TXN_COUNT))
    print("[stream] comptes en rafale:", bursts.select("account_ref").distinct().count())
    print("[stream] AML:", bank.where(F.col("transaction_type").isin("TRANSFER", "INTERNATIONAL_WIRE")
                                      & (F.col("amount") >= F.when(F.col("currency") == "XOF", 1_000_000)
                                         .otherwise(5000))).count())
    print("[stream] mobile pays inhabituel:",
          silver_tables["mobile_money_payments"].where("is_unusual_country").count())

    ins = silver_tables["insurance_operations"]
    premiums = ins.where(F.col("operation_type").isin("PREMIUM_PAYMENT", "POLICY_RENEWAL"))
    by_customer = premiums.groupBy("customer_id", "product_line").agg(F.sum("amount_eur").alias("c"))
    by_product = premiums.groupBy("country_code", "product_line").agg((F.avg("amount_eur") * 12).alias("p"))
    claims = (ins.where(F.col("operation_type").isin("CLAIM_SUBMISSION", "CLAIM_PAYMENT"))
              .join(by_customer, ["customer_id", "product_line"], "left")
              .join(by_product, ["country_code", "product_line"], "left")
              .withColumn("annual", F.greatest("c", "p")))
    print("[stream] sinistres > 3x prime:", claims.where(F.col("amount_eur") > 3 * F.col("annual")).count(),
          "/", claims.count())
    spark.stop()


if __name__ == "__main__":
    main()
