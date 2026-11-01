"""导出对象存储中的 artifact key 列表（CI 证据用）。

在集群内执行：python3 /evidence/dump_objects.py
"""
import os

import boto3


def main() -> int:
    endpoint = os.getenv("OBJSTORE_ENDPOINT", "http://127.0.0.1:5000")
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.getenv("OBJSTORE_ACCESS_KEY", "test-access-key"),
        aws_secret_access_key=os.getenv("OBJSTORE_SECRET_KEY", "test-secret-key"),
        region_name=os.getenv("OBJSTORE_REGION", "us-east-1"),
    )
    buckets = s3.list_buckets().get("Buckets", [])
    if not buckets:
        print("(no buckets)")
        return 0
    for b in buckets:
        name = b["Name"]
        listed = s3.list_objects_v2(Bucket=name)
        count = listed.get("KeyCount", 0)
        print("bucket {}: {} objects".format(name, count))
        for obj in listed.get("Contents", []):
            print("   ", obj["Key"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
