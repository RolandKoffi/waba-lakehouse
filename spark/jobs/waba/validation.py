from itertools import chain

from pyspark.sql import functions as F

from waba.config import COUNTRIES, CURRENCY

CORRUPT_COL = "_corrupt_record"


def _clean(col_name):
    c = F.col(col_name).cast("string")
    return F.when(F.trim(c) == "", None).otherwise(F.trim(c))


def validate(df, ds):
    typed = {c: _clean(c).cast(t) for c, t in ds.columns}
    expected_currency = F.create_map(*[F.lit(x) for x in chain.from_iterable(CURRENCY.items())])

    errors = []
    if CORRUPT_COL in df.columns:
        errors.append(F.when(F.col(CORRUPT_COL).isNotNull(), F.lit("MALFORMED_ROW")))
    for c, t in ds.columns:
        if t != "string":
            errors.append(F.when(_clean(c).isNotNull() & typed[c].isNull(), F.lit(f"INVALID_TYPE:{c}")))
    for c in ds.required:
        errors.append(F.when(_clean(c).isNull(), F.lit(f"MISSING:{c}")))
    errors.append(F.when(~typed["country_code"].isin(COUNTRIES), F.lit("INVALID_COUNTRY")))
    if ds.currency_check:
        errors.append(F.when(typed["currency"] != expected_currency[typed["country_code"]],
                             F.lit("INVALID_CURRENCY")))

    raw_payload = F.to_json(F.struct(*[F.col(c).cast("string") for c in ds.column_names]))
    passthrough = [c for c in df.columns if c not in ds.column_names and c != CORRUPT_COL]
    return df.select(
        *[typed[c].alias(c) for c in ds.column_names],
        *passthrough,
        raw_payload.alias("_raw"),
        F.concat_ws(";", *errors).alias("_error"),
    )


def split_valid(validated):
    valid = validated.where(F.col("_error") == "").drop("_error", "_raw")
    rejected = validated.where(F.col("_error") != "")
    return valid, rejected
