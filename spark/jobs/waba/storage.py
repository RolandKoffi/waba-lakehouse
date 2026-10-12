import boto3

from waba import config


def client():
    return boto3.client(
        "s3",
        endpoint_url=config.S3_ENDPOINT,
        aws_access_key_id=config.access_key(),
        aws_secret_access_key=config.secret_key(),
        region_name=config.AWS_REGION,
    )


def list_csv(bucket, prefix):
    pages = client().get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
    return sorted(o["Key"] for p in pages for o in p.get("Contents", []) if o["Key"].endswith(".csv"))


def archive(keys, log):
    s3 = client()
    for key in keys:
        s3.copy_object(Bucket=config.ARCHIVE_BUCKET, Key=key,
                       CopySource={"Bucket": config.RAW_BUCKET, "Key": key})
        s3.head_object(Bucket=config.ARCHIVE_BUCKET, Key=key)
        s3.delete_object(Bucket=config.RAW_BUCKET, Key=key)
        log.info(f"archived s3://{config.RAW_BUCKET}/{key} -> s3://{config.ARCHIVE_BUCKET}/{key}")
