import os

COUNTRIES = ["CI", "SN", "ML", "BF", "GN", "TG", "BJ", "GH"]
UEMOA = ["CI", "SN", "ML", "BF", "GN", "TG", "BJ"]
CURRENCY = {c: "XOF" for c in UEMOA} | {"GH": "GHS"}


def env(name, default=None):
    value = os.environ.get(name, default)
    if value is None:
        raise RuntimeError(f"Variable d'environnement manquante : {name}")
    return value


CATALOG = env("ICEBERG_CATALOG", "iceberg")
RAW_BUCKET = env("RAW_BUCKET", "raw-landing")
LAKEHOUSE_BUCKET = env("LAKEHOUSE_BUCKET", "lakehouse")
ARCHIVE_BUCKET = env("ARCHIVE_BUCKET", "archive")
S3_ENDPOINT = env("S3_ENDPOINT", "http://minio:9000")
AWS_REGION = env("AWS_REGION", "us-east-1")
REST_URI = env("ICEBERG_REST_URI", "http://iceberg-rest:8181")
KAFKA_BOOTSTRAP = env("KAFKA_BOOTSTRAP", "kafka:9092")

FX_XOF_PER_EUR = float(env("FX_XOF_PER_EUR", "655.957"))
FX_GHS_PER_EUR = float(env("FX_GHS_PER_EUR", "15.0"))

FRAUD_BIG_TXN_XOF = float(env("FRAUD_BIG_TXN_XOF", "500000"))
FRAUD_MIN_TXN_COUNT = int(env("FRAUD_MIN_TXN_COUNT", "2"))
AML_THRESHOLD_XOF = float(env("AML_THRESHOLD_XOF", "1000000"))
AML_THRESHOLD_GHS = float(env("AML_THRESHOLD_GHS", "5000"))
LIQUIDITY_MIN_COVERAGE = float(env("LIQUIDITY_MIN_COVERAGE", "0.98"))
NPL_THRESHOLD = 0.05
LOSS_RATIO_THRESHOLD = 0.70


def access_key():
    return env("MINIO_ROOT_USER")


def secret_key():
    return env("MINIO_ROOT_PASSWORD")


def pseudo_salt():
    return env("PSEUDO_SALT")


def parse_countries(value):
    if not value or value.upper() == "ALL":
        return COUNTRIES
    countries = [c.strip().upper() for c in value.split(",") if c.strip()]
    unknown = set(countries) - set(COUNTRIES)
    if unknown:
        raise ValueError(f"Pays inconnus : {unknown}")
    return countries
