"""对象存储抽象：artifact 的存取。

为什么需要它：
    Argo 的每个 task 是独立 Pod，**artifact 必须走 Pod 之外的存储**。
    原实现里产物只在本地目录/PVC，跨 Pod 传递没有抽象。
    本模块把"存/取/列举"抽成接口，本地用目录模拟，上集群换 S3/OSS/MinIO。

与 Argo 的对应：
    ``k8s/workflow.yaml`` 里的 ``artifactRepository`` 就是配置一个 ArtifactStore；
    runner 的 ``artifacts: {name: path}`` 声明经由它上传/下载。

    # Argo 侧配置示例（S3 兼容）
    artifactRepository:
      s3:
        bucket: tts-artifacts
        endpoint: minio.data-platform:9000
        accessKeySecret: {name: tts-secrets, key: ak}
        secretKeySecret: {name: tts-secrets, key: sk}
        keyFormat: "{{workflow.name}}/{{pod.name}}/{{artifact.name}}"

S3 实现默认不启用（依赖 boto3）；本地实现零依赖，供单机演练与测试。
"""
from __future__ import annotations

import hashlib
import shutil
from pathlib import Path
from typing import Protocol


class ArtifactStore(Protocol):
    """artifact 存储契约：put / get / exists / list。"""

    def put(self, local_path: Path, key: str) -> str:
        """上传，返回存储内的寻址（key 或 URI）。"""
        ...

    def get(self, key: str, dest: Path) -> Path:
        """下载到本地路径，返回本地路径。"""
        ...

    def exists(self, key: str) -> bool:
        ...

    def list(self, prefix: str = "") -> list[str]:
        ...

    def delete(self, key: str) -> bool:
        ...


# ---------------------------------------------------------------------------
# 本地实现：目录模拟对象存储（布局与 S3 一致：key 即"路径"）
# ---------------------------------------------------------------------------

def _normalize_key(key: str) -> str:
    """artifact key 规范化 + 目录穿越防护。

    本地目录实现与 S3 实现必须共用同一套校验，否则同一个 key 在两种存储上
    行为不一致（本地拒绝、S3 当字面 key 接受），排障时非常隐蔽。
    """
    cleaned = str(key).strip().lstrip("/")
    if not cleaned:
        raise ValueError("artifact key 不能为空")
    if any(seg in ("", ".", "..") for seg in cleaned.split("/")):
        raise ValueError(f"非法 artifact key: {key}")
    return cleaned


class LocalArtifactStore:
    """零依赖的本地对象存储模拟。

    布局：``<root>/<key>``，key 用 "/" 分隔，与 S3 的 key 语义一致，
    所以换成 S3ArtifactStore 时调用方代码不用改。
    """

    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    # -- key 规范化：防目录穿越（与 S3 共用同一套规则） --
    @staticmethod
    def _safe_key(key: str) -> str:
        return _normalize_key(key)

    def _path(self, key: str) -> Path:
        return self.root / self._safe_key(key)

    def put(self, local_path: Path, key: str) -> str:
        # 先校验 key（安全），再碰文件系统：恶意 key 不应触发任何 IO
        dst = self._path(key)
        src = Path(local_path)
        if not src.exists():
            raise FileNotFoundError(f"待上传的 artifact 不存在: {src}")
        dst.parent.mkdir(parents=True, exist_ok=True)
        if src.is_dir():
            if dst.exists():
                shutil.rmtree(dst)
            shutil.copytree(src, dst)
        else:
            shutil.copy2(src, dst)
        return key

    def get(self, key: str, dest: Path) -> Path:
        src = self._path(key)
        if not src.exists():
            raise FileNotFoundError(f"artifact 不存在: {key}")
        dest = Path(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        if src.is_dir():
            if dest.exists():
                shutil.rmtree(dest)
            shutil.copytree(src, dest)
        else:
            shutil.copy2(src, dest)
        return dest

    def exists(self, key: str) -> bool:
        return self._path(key).exists()

    def list(self, prefix: str = "") -> list[str]:
        # 前缀允许以 "/" 结尾（调用方常用 "runs/"），先去尾再规范化
        prefix = prefix.strip().strip("/")
        base = self._path(prefix) if prefix else self.root
        if not base.exists():
            return []
        if base.is_file():
            return [prefix]
        return sorted(str(p.relative_to(self.root)) for p in base.rglob("*") if p.is_file())

    def delete(self, key: str) -> bool:
        target = self._path(key)
        if not target.exists():
            return False
        if target.is_dir():
            shutil.rmtree(target)
        else:
            target.unlink()
        return True

    def digest(self, key: str) -> str:
        """artifact 内容的 sha256（用于对账与变更检测）。"""
        path = self._path(key)
        h = hashlib.sha256()
        if path.is_file():
            h.update(path.read_bytes())
        elif path.is_dir():
            for p in sorted(path.rglob("*")):
                if p.is_file():
                    h.update(p.name.encode())
                    h.update(p.read_bytes())
        return h.hexdigest()


# ---------------------------------------------------------------------------
# S3 兼容实现（集群用；boto3 未安装时给出明确报错而不是静默失败）
# ---------------------------------------------------------------------------

class S3ArtifactStore:
    """S3 / MinIO / OSS(S3 兼容) 实现。

    依赖 boto3；未安装时构造阶段就报错，避免跑到一半才发现传不出去。
    """

    def __init__(self, bucket: str, endpoint: str | None = None,
                 prefix: str = "", **client_kwargs):
        try:
            import boto3
        except ImportError as exc:
            raise RuntimeError("S3ArtifactStore 需要 boto3：pip install boto3") from exc
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self._s3 = boto3.client("s3", endpoint_url=endpoint, **client_kwargs)

    def _key(self, key: str) -> str:
        k = _normalize_key(key)          # 与本地实现同一套穿越/空段校验
        return f"{self.prefix}/{k}" if self.prefix else k

    def put(self, local_path: Path, key: str) -> str:
        src = Path(local_path)
        k = self._key(key)
        if src.is_dir():
            # 目录：逐个上传，保留相对路径
            for p in sorted(src.rglob("*")):
                if p.is_file():
                    rel = p.relative_to(src)
                    self._s3.upload_file(str(p), self.bucket, f"{k}/{rel}")
        else:
            self._s3.upload_file(str(src), self.bucket, k)
        return k

    def get(self, key: str, dest: Path) -> Path:
        k = self._key(key)
        dest = Path(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        self._s3.download_file(self.bucket, k, str(dest))
        return dest

    def exists(self, key: str) -> bool:
        try:
            self._s3.head_object(Bucket=self.bucket, Key=self._key(key))
            return True
        except Exception:
            return False

    @staticmethod
    def _list_prefix(store_prefix: str, prefix: str) -> str:
        """S3 list 前缀：保留调用方的尾斜杠语义。

        本地实现用目录语义，S3 用字符串前缀语义。若把 ``"runs/r1/"`` 简单去掉
        尾斜杠，Prefix 会变成 ``runs/r1``，从而 over-match 兄弟前缀
        ``runs/r10/...``。尾斜杠表示"目录前缀"，必须补回。
        """
        raw = str(prefix).strip()
        core = raw.strip("/")
        if not core:
            return f"{store_prefix}/" if store_prefix else ""
        normalized = _normalize_key(core)
        full = f"{store_prefix}/{normalized}" if store_prefix else normalized
        return full + "/" if raw.endswith("/") else full

    def list(self, prefix: str = "") -> list[str]:
        full_prefix = self._list_prefix(self.prefix, prefix)
        keys: list[str] = []
        paginator = self._s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=full_prefix):
            for obj in page.get("Contents", []):
                k = obj["Key"]
                keys.append(k[len(self.prefix) + 1:] if self.prefix and k.startswith(self.prefix + "/") else k)
        return sorted(keys)

    def delete(self, key: str) -> bool:
        self._s3.delete_object(Bucket=self.bucket, Key=self._key(key))
        return True


def make_store(kind: str = "local", **kwargs) -> ArtifactStore:
    """工厂：``local`` / ``s3``。"""
    if kind == "local":
        return LocalArtifactStore(kwargs.get("root", "./artifacts"))
    if kind == "s3":
        return S3ArtifactStore(**kwargs)
    raise ValueError(f"未知 artifact 存储类型: {kind}")
