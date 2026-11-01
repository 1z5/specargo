"""对象存储 CLI：按"路径契约"在 Pod 之间传递数据。

为什么不用 Argo 的 artifact 引用：
    ``{{tasks.extract.outputs.artifacts.raw}}`` 这种引用依赖**扇出索引匹配**——
    上游 withItems 扇出 N 个、下游也必须同样扇出且顺序一致，Argo 才能按 index 配对。
    在大规模扇出、部分失败重试、或上下游分区列表不同的场景下，这种隐式耦合很容易断
    （CI 里实测过：Unable to resolve tasks.extract.outputs.artifacts.raw）。
    显式路径契约更健壮：上下游只约定 key 怎么算，不依赖编排器的配对行为。

契约：
    runs/<run_id>/<partition>/<stage>.<ext>
    - run_id     批次标识（TTS_RUN_ID / ARGO_RUN_ID）
    - partition  分区键（章节号 / 日期 / 文件名）
    - stage      阶段名（raw / clean / marker）

用法：
    python objstore.py put --key <key> --file <本地路径>
    python objstore.py get --key <key> --file <本地路径>
    python objstore.py list [--prefix <前缀>]
    python objstore.py exists --key <key>

配置（环境变量）：
    OBJSTORE_ENDPOINT   S3 兼容端点（集群内 object-store:5000；生产换 MinIO/OSS/S3）
    OBJSTORE_BUCKET     bucket 名
    OBJSTORE_ACCESS_KEY / OBJSTORE_SECRET_KEY
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


def _client():
    import boto3

    endpoint = os.getenv("OBJSTORE_ENDPOINT", "http://object-store.argo.svc.cluster.local:5000")
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.getenv("OBJSTORE_ACCESS_KEY", "test-access-key"),
        aws_secret_access_key=os.getenv("OBJSTORE_SECRET_KEY", "test-secret-key"),
        region_name=os.getenv("OBJSTORE_REGION", "us-east-1"),
    )


def _bucket() -> str:
    return os.getenv("OBJSTORE_BUCKET", "argo-artifacts")


def contract_key(run_id: str, partition: str, stage: str, ext: str = "jsonl") -> str:
    """路径契约：runs/<run_id>/<partition>/<stage>.<ext>"""
    return f"runs/{run_id}/{partition}/{stage}.{ext}"


def put(key: str, file: Path) -> str:
    src = Path(file)
    if not src.exists():
        raise FileNotFoundError(f"待上传文件不存在: {src}")
    _client().upload_file(str(src), _bucket(), key)
    print(f"[objstore] put {key} ({src.stat().st_size} bytes)")
    return key


def get(key: str, file: Path) -> Path:
    dest = Path(file)
    dest.parent.mkdir(parents=True, exist_ok=True)
    _client().download_file(_bucket(), key, str(dest))
    print(f"[objstore] get {key} -> {dest} ({dest.stat().st_size} bytes)")
    return dest


def exists(key: str) -> bool:
    try:
        _client().head_object(Bucket=_bucket(), Key=key)
        return True
    except Exception:
        return False


def list_keys(prefix: str = "") -> list[str]:
    keys: list[str] = []
    paginator = _client().get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=_bucket(), Prefix=prefix):
        keys.extend(o["Key"] for o in page.get("Contents", []))
    return sorted(keys)


def main() -> int:
    parser = argparse.ArgumentParser(description="对象存储 CLI（路径契约）")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("put")
    p.add_argument("--key", required=True)
    p.add_argument("--file", required=True)

    p = sub.add_parser("get")
    p.add_argument("--key", required=True)
    p.add_argument("--file", required=True)

    p = sub.add_parser("exists")
    p.add_argument("--key", required=True)

    p = sub.add_parser("list")
    p.add_argument("--prefix", default="")

    args = parser.parse_args()
    try:
        if args.cmd == "put":
            put(args.key, Path(args.file))
        elif args.cmd == "get":
            get(args.key, Path(args.file))
        elif args.cmd == "exists":
            print("true" if exists(args.key) else "false")
            return 0 if exists(args.key) else 1
        elif args.cmd == "list":
            for k in list_keys(args.prefix):
                print(k)
    except Exception as exc:
        print(f"[objstore] 错误: {exc!r}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
