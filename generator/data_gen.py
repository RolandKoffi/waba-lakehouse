import io
import os
import uuid
import argparse
from datetime import datetime, timedelta

import boto3
import numpy as np
import pandas as pd

COUNTRIES = ["CI", "SN", "ML", "BF", "GN", "TG", "BJ", "GH"]
UEMOA = ["CI", "SN", "ML", "BF", "GN", "TG", "BJ"]
CURRENCY = {c: "XOF" for c in UEMOA} | {"GH": "GHS"}
XOF_PER_LOCAL = {c: 1.0 for c in UEMOA} | {"GH": 655.957 / 15.0}
ENTITY_TYPES = ["BANK", "INSURANCE", "MOBILE_MONEY", "MICROFINANCE"]

REGIONS = {
    "CI": ["Abidjan", "Bouake", "Yamoussoukro", "San Pedro", "Korhogo"],
    "SN": ["Dakar", "Thies", "Ziguinchor", "Saint-Louis", "Kaolack"],
    "ML": ["Bamako", "Sikasso", "Segou", "Mopti", "Tombouctou"],
    "BF": ["Ouagadougou", "Bobo-Dioulasso", "Koudougou", "Banfora"],
    "GN": ["Conakry", "Nzerekore", "Kindia", "Kankan"],
    "TG": ["Lome", "Sokode", "Kara", "Atakpame"],
    "BJ": ["Cotonou", "Porto-Novo", "Parakou", "Abomey-Calavi"],
    "GH": ["Accra", "Kumasi", "Tamale", "Cape Coast", "Sunyani"],
}

NPL_RATE = {"CI": 0.045, "SN": 0.040, "ML": 0.065, "BF": 0.060, "GN": 0.070, "TG": 0.050, "BJ": 0.045, "GH": 0.055}
LOSS_RATIO = {"CI": 0.62, "SN": 0.58, "ML": 0.74, "BF": 0.72, "GN": 0.76, "TG": 0.66, "BJ": 0.56, "GH": 0.68}

PRODUCT_CODES = [
    ("CURRENT", "ACCOUNT", "BANK", 0.0),
    ("SAVINGS", "ACCOUNT", "BANK", 0.035),
    ("CONSUMER", "LOAN", "BANK", 0.12),
    ("MORTGAGE", "LOAN", "BANK", 0.08),
    ("SME", "LOAN", "BANK", 0.10),
    ("AGRICULTURAL", "LOAN", "MICROFINANCE", 0.14),
    ("MICROCREDIT", "LOAN", "MICROFINANCE", 0.18),
    ("VIE", "INSURANCE", "INSURANCE", 0.0),
    ("IARD_AUTO", "INSURANCE", "INSURANCE", 0.0),
    ("IARD_HABITATION", "INSURANCE", "INSURANCE", 0.0),
    ("IARD_SANTE", "INSURANCE", "INSURANCE", 0.0),
    ("PREVOYANCE", "INSURANCE", "INSURANCE", 0.0),
    ("P2P", "MOBILE_MONEY", "MOBILE_MONEY", 0.0),
    ("MERCHANT_PAYMENT", "MOBILE_MONEY", "MOBILE_MONEY", 0.0),
    ("BILL_PAYMENT", "MOBILE_MONEY", "MOBILE_MONEY", 0.0),
    ("AIRTIME", "MOBILE_MONEY", "MOBILE_MONEY", 0.0),
    ("CROSS_BORDER_TRANSFER", "MOBILE_MONEY", "MOBILE_MONEY", 0.0),
]

DATASETS = {
    "bank_transactions": "bank_txn",
    "insurance_operations": "insurance_ops",
    "mobile_money_payments": "mobile_money",
    "loan_repayments": "loan_repayments",
}

REF_SEED = 42


def random_dates(rng, start, end, n):
    seconds = int((end - start).total_seconds())
    offsets = rng.integers(0, max(seconds, 1), n)
    return pd.to_datetime(start) + pd.to_timedelta(offsets, unit="s")


def city_for(rng, countries):
    return np.array([REGIONS[c][rng.integers(len(REGIONS[c]))] for c in countries])


def generate_customers(n=500_000, seed=REF_SEED):
    rng = np.random.default_rng(seed)
    cc = rng.choice(COUNTRIES, n)
    return pd.DataFrame({
        "customer_id": [f"WABA-{c}-C-{i:06d}" for i, c in enumerate(cc, start=1)],
        "country_code": cc,
        "entity_type": rng.choice(ENTITY_TYPES, n, p=[0.55, 0.20, 0.18, 0.07]),
        "segment": rng.choice(["RETAIL", "SME", "CORPORATE", "PREMIUM"], n, p=[0.65, 0.20, 0.10, 0.05]),
        "kyc_level": rng.choice(["BASIC", "STANDARD", "ENHANCED"], n, p=[0.3, 0.5, 0.2]),
        "onboarding_date": random_dates(rng, datetime(2010, 1, 1), datetime(2025, 12, 31), n).date,
        "region": city_for(rng, cc),
        "is_active": rng.random(n) > 0.08,
    })


def generate_accounts(customers, n=800_000, seed=REF_SEED + 1):
    rng = np.random.default_rng(seed)
    idx = rng.integers(0, len(customers), n)
    cust = customers.iloc[idx].reset_index(drop=True)
    cc = cust["country_code"].to_numpy()
    acc_type = rng.choice(["CURRENT", "SAVINGS", "LOAN", "MOBILE_WALLET", "INSURANCE_POLICY"], n,
                          p=[0.35, 0.20, 0.20, 0.15, 0.10])
    xpl = np.array([XOF_PER_LOCAL[c] for c in cc])
    balance = rng.lognormal(13, 1.2, n)
    balance = np.where(acc_type == "LOAN", rng.lognormal(14.5, 1.0, n), balance)
    has_limit = (acc_type == "CURRENT") & (rng.random(n) < 0.3)
    credit_limit = np.where(has_limit, rng.lognormal(12.5, 0.8, n), 0.0)
    bank_like = np.where(cust["entity_type"].to_numpy() == "MICROFINANCE", "MICROFINANCE", "BANK")
    entity = np.select([acc_type == "MOBILE_WALLET", acc_type == "INSURANCE_POLICY"],
                       ["MOBILE_MONEY", "INSURANCE"], default=bank_like)
    return pd.DataFrame({
        "account_id": [f"WABA-{c}-A-{i:07d}" for i, c in enumerate(cc, start=1)],
        "customer_id": cust["customer_id"].to_numpy(),
        "country_code": cc,
        "account_type": acc_type,
        "currency": [CURRENCY[c] for c in cc],
        "balance": np.round(balance / xpl, 2),
        "credit_limit": np.round(credit_limit / xpl, 2),
        "opened_date": random_dates(rng, datetime(2010, 1, 1), datetime(2025, 12, 31), n).date,
        "status": rng.choice(["ACTIVE", "DORMANT", "FROZEN", "CLOSED"], n, p=[0.85, 0.08, 0.03, 0.04]),
        "entity_type": entity,
    })


def generate_branches(n=200, seed=REF_SEED + 2):
    rng = np.random.default_rng(seed)
    cc = np.array([COUNTRIES[i % len(COUNTRIES)] for i in range(n)])
    counters = {c: 0 for c in COUNTRIES}
    ids = []
    for c in cc:
        counters[c] += 1
        ids.append(f"WABA-{c}-B-{counters[c]:03d}")
    city = city_for(rng, cc)
    return pd.DataFrame({
        "branch_id": ids,
        "country_code": cc,
        "entity_type": rng.choice(ENTITY_TYPES, n, p=[0.6, 0.1, 0.1, 0.2]),
        "city": city,
        "region": city,
        "branch_type": rng.choice(["FULL_SERVICE", "DIGITAL_ONLY", "AGENCY_BANKING", "ATM_POINT"], n,
                                  p=[0.5, 0.1, 0.25, 0.15]),
        "is_active": rng.random(n) > 0.05,
    })


def generate_products(n=50):
    rows = []
    for i in range(n):
        code, family, entity, rate = PRODUCT_CODES[i % len(PRODUCT_CODES)]
        country = COUNTRIES[(i // len(PRODUCT_CODES) + i) % len(COUNTRIES)]
        rows.append({
            "product_id": f"WABA-P-{i + 1:03d}",
            "product_code": code,
            "product_name": f"WABA {code.replace('_', ' ').title()} {country}",
            "product_family": family,
            "interest_rate": rate,
            "country_code": country,
            "entity_type": entity,
        })
    return pd.DataFrame(rows)


class ReferentialStore:
    def __init__(self, customers, accounts, branches, products):
        self.customers = customers
        self.accounts = accounts
        self.branches = branches
        self.products = products
        self.cust_by_country = {c: g for c, g in customers.groupby("country_code")}
        self.acc_by_country_type = {k: g for k, g in accounts.groupby(["country_code", "account_type"])}
        self.branch_by_country = {c: g for c, g in branches.groupby("country_code")}

    def accounts_of(self, cc, types):
        return pd.concat([self.acc_by_country_type[(cc, t)] for t in types if (cc, t) in self.acc_by_country_type])

    @classmethod
    def build(cls, n_customers=500_000, n_accounts=800_000, n_branches=200, n_products=50):
        customers = generate_customers(n_customers)
        accounts = generate_accounts(customers, n_accounts)
        return cls(customers, accounts, generate_branches(n_branches), generate_products(n_products))

    def save(self, folder):
        os.makedirs(folder, exist_ok=True)
        for name in ["customers", "accounts", "branches", "products"]:
            getattr(self, name).to_csv(os.path.join(folder, f"{name}.csv"), index=False)

    @classmethod
    def load(cls, folder):
        names = ["customers", "accounts", "branches", "products"]
        frames = [pd.read_csv(os.path.join(folder, f"{n}.csv")) for n in names]
        return cls(*frames)


def _pick(rng, df, n):
    return df.iloc[rng.integers(0, len(df), n)].reset_index(drop=True)


def generate_bank_transactions(refs, cc, n, start, end, rng, anomalies=False):
    pool = refs.accounts_of(cc, ["CURRENT", "SAVINGS"])
    acc = _pick(rng, pool, n)
    benef = _pick(rng, pool, n)
    branch = _pick(rng, refs.branch_by_country[cc], n)
    status = rng.choice(["SUCCESS", "FAILED", "REVERSED"], n, p=[0.92, 0.05, 0.03])
    amount = np.maximum(500, rng.lognormal(12, 1.5, n)) / XOF_PER_LOCAL[cc]
    df = pd.DataFrame({
        "transaction_id": [str(uuid.uuid4()) for _ in range(n)],
        "timestamp": random_dates(rng, start, end, n),
        "account_id": acc["account_id"],
        "beneficiary_account": benef["account_id"],
        "branch_id": branch["branch_id"],
        "country_code": cc,
        "transaction_type": rng.choice(["TRANSFER", "PAYMENT", "WITHDRAWAL", "DEPOSIT", "INTERNATIONAL_WIRE"], n,
                                       p=[0.35, 0.30, 0.15, 0.15, 0.05]),
        "amount": np.round(amount, 2),
        "currency": CURRENCY[cc],
        "channel": rng.choice(["BRANCH", "ATM", "MOBILE_APP", "INTERNET_BANKING", "USSD"], n,
                              p=[0.20, 0.15, 0.35, 0.20, 0.10]),
        "transaction_status": status,
        "fee_amount": np.where(status == "SUCCESS", np.round(amount * 0.001, 2), 0.0),
        "entity_type": acc["entity_type"],
    })
    if anomalies:
        df = pd.concat([df, _bank_bursts(refs, cc, max(1, n // 500), start, end, rng)], ignore_index=True)
    return df


def _bank_bursts(refs, cc, k, start, end, rng):
    rows = []
    pool = refs.accounts_of(cc, ["CURRENT"])
    for _ in range(k):
        acc = pool.iloc[rng.integers(len(pool))]
        base = random_dates(rng, start, end - timedelta(minutes=5), 1)[0]
        branch = refs.branch_by_country[cc].iloc[0]["branch_id"]
        for j in range(3):
            amount = round(float(rng.uniform(600_000, 2_000_000)) / XOF_PER_LOCAL[cc], 2)
            rows.append({
                "transaction_id": str(uuid.uuid4()),
                "timestamp": base + timedelta(seconds=int(rng.integers(0, 120))),
                "account_id": acc["account_id"],
                "beneficiary_account": pool.iloc[rng.integers(len(pool))]["account_id"],
                "branch_id": branch,
                "country_code": cc,
                "transaction_type": "TRANSFER",
                "amount": amount,
                "currency": CURRENCY[cc],
                "channel": "MOBILE_APP",
                "transaction_status": "SUCCESS",
                "fee_amount": round(amount * 0.001, 2),
                "entity_type": acc["entity_type"],
            })
    return pd.DataFrame(rows)


def generate_insurance_operations(refs, cc, n, start, end, rng, anomalies=False):
    pol = _pick(rng, refs.accounts_of(cc, ["INSURANCE_POLICY"]), n)
    op_types = ["PREMIUM_PAYMENT", "CLAIM_SUBMISSION", "CLAIM_PAYMENT", "POLICY_RENEWAL", "POLICY_CANCELLATION"]
    op = rng.choice(op_types, n, p=[0.55, 0.15, 0.10, 0.15, 0.05])
    line = rng.choice(["VIE", "IARD_AUTO", "IARD_HABITATION", "IARD_SANTE", "PREVOYANCE"], n,
                      p=[0.25, 0.25, 0.15, 0.20, 0.15])
    premium = rng.lognormal(10.3, 0.5, n)
    claim = rng.lognormal(10.3, 0.5, n) * 7 * LOSS_RATIO[cc]
    amount = np.select([np.isin(op, ["CLAIM_SUBMISSION", "CLAIM_PAYMENT"]), op == "POLICY_CANCELLATION"],
                       [claim, premium * 0.2], default=premium)
    if anomalies:
        fraud = (op == "CLAIM_PAYMENT") & (rng.random(n) < 0.003)
        amount = np.where(fraud, premium * rng.uniform(40, 60, n), amount)
    is_claim = np.isin(op, ["CLAIM_SUBMISSION", "CLAIM_PAYMENT"])
    claim_status = np.where(op == "CLAIM_PAYMENT", "PAID",
                            rng.choice(["PENDING", "APPROVED", "REJECTED"], n, p=[0.4, 0.4, 0.2]))
    days = np.where(line == "VIE", rng.integers(10, 61, n), rng.integers(2, 31, n))
    return pd.DataFrame({
        "operation_id": [str(uuid.uuid4()) for _ in range(n)],
        "timestamp": random_dates(rng, start, end, n),
        "customer_id": pol["customer_id"],
        "account_id": pol["account_id"],
        "country_code": cc,
        "operation_type": op,
        "product_line": line,
        "amount": np.round(amount / XOF_PER_LOCAL[cc], 2),
        "currency": CURRENCY[cc],
        "claim_status": pd.Series(np.where(is_claim, claim_status, None)),
        "processing_days": pd.Series(np.where(is_claim, days, None)).astype("Int64"),
        "entity_type": "INSURANCE",
    })


def generate_mobile_money(refs, cc, n, start, end, rng, anomalies=False):
    sender = _pick(rng, refs.cust_by_country[cc], n)
    ptype = rng.choice(["P2P", "MERCHANT_PAYMENT", "BILL_PAYMENT", "AIRTIME", "CROSS_BORDER_TRANSFER"], n,
                       p=[0.35, 0.25, 0.15, 0.15, 0.10])
    others = [c for c in (UEMOA if cc in UEMOA else COUNTRIES) if c != cc]
    recv_country = np.where(ptype == "CROSS_BORDER_TRANSFER", rng.choice(others, n), cc)
    receiver = [refs.cust_by_country[c].iloc[rng.integers(len(refs.cust_by_country[c]))]["customer_id"]
                for c in recv_country]
    sender_country = np.full(n, cc)
    if anomalies:
        odd = rng.random(n) < 0.01
        sender_country = np.where(odd, rng.choice([c for c in COUNTRIES if c != cc], n), sender_country)
    status = rng.choice(["SUCCESS", "FAILED", "PENDING"], n, p=[0.94, 0.04, 0.02])
    amount = np.round(rng.lognormal(9.5, 1.1, n) / XOF_PER_LOCAL[cc], 2)
    fee_rate = np.where(np.isin(ptype, ["P2P", "CROSS_BORDER_TRANSFER"]), 0.01, 0.0)
    return pd.DataFrame({
        "payment_id": [str(uuid.uuid4()) for _ in range(n)],
        "timestamp": random_dates(rng, start, end, n),
        "sender_id": sender["customer_id"],
        "receiver_id": receiver,
        "sender_country": sender_country,
        "receiver_country": recv_country,
        "country_code": cc,
        "amount": amount,
        "currency": CURRENCY[cc],
        "payment_type": ptype,
        "operator": rng.choice(["WABA_PAY", "ORANGE_MONEY_PARTNER", "MTN_PARTNER"], n, p=[0.5, 0.3, 0.2]),
        "status": status,
        "fee_amount": np.where(status == "SUCCESS", np.round(amount * fee_rate, 2), 0.0),
        "entity_type": "MOBILE_MONEY",
    })


def generate_loan_repayments(refs, cc, n, start, end, rng, anomalies=False):
    loans = _pick(rng, refs.accounts_of(cc, ["LOAN"]), n)
    acc_num = loans["account_id"].str[-7:].astype(int).to_numpy()
    micro = loans["entity_type"].to_numpy() == "MICROFINANCE"
    bank_types = np.array(["CONSUMER", "MORTGAGE", "SME", "AGRICULTURAL"])
    micro_types = np.array(["MICROCREDIT", "AGRICULTURAL"])
    loan_type = np.where(micro, micro_types[acc_num % 2], bank_types[acc_num % 4])
    p_default = NPL_RATE[cc]
    status = rng.choice(["ON_TIME", "LATE", "DEFAULT"], n, p=[1 - 0.12 - p_default, 0.12, p_default])
    days = np.select([status == "LATE", status == "DEFAULT"],
                     [rng.integers(1, 90, n), rng.integers(90, 366, n)], default=0)
    ts = random_dates(rng, start, end, n)
    due = (ts.normalize() - pd.to_timedelta(days, unit="D")).date
    amount_due = np.round(loans["balance"].to_numpy() * 0.03, 2)
    paid = np.select([status == "ON_TIME", status == "LATE"],
                     [amount_due, np.round(amount_due * rng.uniform(0.3, 1.0, n), 2)], default=0.0)
    return pd.DataFrame({
        "repayment_id": [str(uuid.uuid4()) for _ in range(n)],
        "timestamp": ts,
        "loan_account_id": loans["account_id"],
        "customer_id": loans["customer_id"],
        "country_code": cc,
        "amount_due": amount_due,
        "amount_paid": paid,
        "currency": CURRENCY[cc],
        "due_date": due,
        "payment_date": pd.Series(np.where(status == "DEFAULT", None, ts.date)),
        "days_overdue": days,
        "loan_type": loan_type,
        "repayment_status": status,
        "entity_type": loans["entity_type"],
    })


GENERATORS = {
    "bank_transactions": generate_bank_transactions,
    "insurance_operations": generate_insurance_operations,
    "mobile_money_payments": generate_mobile_money,
    "loan_repayments": generate_loan_repayments,
}


def corrupt_rows(df, rng, rate):
    if rate <= 0:
        return df
    df = df.astype({"amount": object} if "amount" in df else {})
    bad = rng.random(len(df)) < rate
    if "amount" in df:
        df.loc[bad, "amount"] = "N/A"
    return df


def to_csv_bytes(df):
    buf = io.StringIO()
    out = df.copy()
    if "timestamp" in out:
        out["timestamp"] = pd.to_datetime(out["timestamp"]).dt.strftime("%Y-%m-%dT%H:%M:%S")
    out.to_csv(buf, index=False)
    return buf.getvalue().encode("utf-8")


class Lake:
    def __init__(self):
        self.s3 = boto3.client(
            "s3",
            endpoint_url=os.environ.get("S3_ENDPOINT", "http://minio:9000"),
            aws_access_key_id=os.environ["MINIO_ROOT_USER"],
            aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
            region_name=os.environ.get("AWS_REGION", "us-east-1"),
        )
        self.raw = os.environ.get("RAW_BUCKET", "raw-landing")
        self.archive = os.environ.get("ARCHIVE_BUCKET", "archive")

    def put(self, key, body):
        self.s3.put_object(Bucket=self.raw, Key=key, Body=body)
        return f"s3://{self.raw}/{key}"

    def count(self, bucket, prefix):
        pages = self.s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
        return sum(len(p.get("Contents", [])) for p in pages)

    def next_key(self, cc, dataset, day):
        prefix = f"{cc}/{dataset}/{DATASETS[dataset]}_{cc}_{day:%Y%m%d}_"
        seq = self.count(self.raw, prefix) + self.count(self.archive, prefix) + 1
        return f"{prefix}{seq:02d}.csv"

    def upload_referentials(self, refs):
        keys = []
        for name in ["customers", "accounts", "branches", "products"]:
            keys.append(self.put(f"referentials/{name}.csv", to_csv_bytes(getattr(refs, name))))
        return keys

    def download_referentials(self, folder):
        os.makedirs(folder, exist_ok=True)
        for name in ["customers", "accounts", "branches", "products"]:
            self.s3.download_file(self.raw, f"referentials/{name}.csv", os.path.join(folder, f"{name}.csv"))


def generate_and_upload(lake, refs, dataset, countries, n_rows, start, end, rng, anomalies=False, bad_rate=0.0,
                        entities=None):
    uploaded = []
    per_country = max(1, n_rows // len(countries))
    for cc in countries:
        df = GENERATORS[dataset](refs, cc, per_country, start, end, rng, anomalies)
        if entities:
            df = df[df["entity_type"].isin(entities)]
        if df.empty:
            continue
        df = corrupt_rows(df, rng, bad_rate)
        key = lake.next_key(cc, dataset, datetime.utcnow())
        uploaded.append((lake.put(key, to_csv_bytes(df)), len(df)))
    return uploaded


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--referentials", action="store_true")
    parser.add_argument("--dataset", choices=list(DATASETS))
    parser.add_argument("--rows", type=int, default=10_000)
    parser.add_argument("--countries", default=",".join(COUNTRIES))
    parser.add_argument("--cache", default=os.environ.get("REF_CACHE", "/data/referentials"))
    args = parser.parse_args()

    lake = Lake()
    if args.referentials:
        refs = ReferentialStore.build()
        refs.save(args.cache)
        print(lake.upload_referentials(refs))
        return
    refs = ReferentialStore.load(args.cache)
    end = datetime.utcnow().replace(microsecond=0)
    print(generate_and_upload(lake, refs, args.dataset, args.countries.split(","), args.rows,
                              end - timedelta(days=90), end, np.random.default_rng()))


if __name__ == "__main__":
    main()
