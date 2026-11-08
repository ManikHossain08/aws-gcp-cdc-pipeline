"""Object-store abstraction over S3, GCS and the local filesystem.

Every component talks to storage through ``ObjectStore`` so the same code runs in AWS
Glue, an Airflow worker, or locally against ``file://`` "buckets".
"""

from __future__ import annotations

import base64
import hashlib
import json
import shutil
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlparse

_CHUNK = 8 * 1024 * 1024


@dataclass(frozen=True)
class ObjectInfo:
    """Metadata of one stored object."""

    key: str
    size: int
    last_modified: datetime


class ObjectStore(Protocol):
    """Minimal object-store interface used by the pipeline."""

    uri: str

    def list(self, prefix: str) -> list[ObjectInfo]:
        """All objects under ``prefix`` (recursive)."""

    def read_bytes(self, key: str) -> bytes:
        """Return the object's content."""

    def write_bytes(
        self, key: str, data: bytes, content_type: str = "application/octet-stream"
    ) -> None:
        """Create or overwrite an object."""

    def download_file(self, key: str, path: Path) -> None:
        """Stream an object to a local file."""

    def upload_file(self, path: Path, key: str, md5_hex: str | None = None) -> None:
        """Upload a local file; when ``md5_hex`` is given the store must verify it."""

    def md5(self, key: str) -> str | None:
        """Hex MD5 of the stored object if the store exposes it cheaply, else None."""

    def exists(self, key: str) -> bool:
        """Whether ``key`` exists."""


def md5_file(path: Path) -> str:
    """Hex MD5 of a local file, streamed in chunks."""
    digest = hashlib.md5()  # noqa: S324 - integrity check, not security
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(_CHUNK), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_json(store: ObjectStore, key: str) -> Any:
    """Read and decode a JSON object."""
    return json.loads(store.read_bytes(key).decode("utf-8"))


def write_json(store: ObjectStore, key: str, payload: Any) -> None:
    """Encode and write a JSON object (pretty-printed, stable key order)."""
    body = json.dumps(payload, indent=2, sort_keys=True, default=str).encode("utf-8")
    store.write_bytes(key, body, "application/json")


# ----------------------------------------------------------------------------- local
class LocalObjectStore:
    """Filesystem-backed store; the "bucket" is a directory."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.uri = f"file://{root}"
        root.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        path = (self.root / key).resolve()
        if self.root.resolve() not in path.parents and path != self.root.resolve():
            raise ValueError(f"Key escapes bucket root: {key!r}")
        return path

    def list(self, prefix: str) -> list[ObjectInfo]:
        """All files under ``prefix``."""
        base = self.root / prefix
        search_root = base if base.is_dir() else base.parent
        if not search_root.exists():
            return []
        out = []
        for path in search_root.rglob("*"):
            if not path.is_file():
                continue
            key = path.relative_to(self.root).as_posix()
            if key.startswith(prefix):
                stat = path.stat()
                out.append(
                    ObjectInfo(
                        key, stat.st_size, datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
                    )
                )
        return sorted(out, key=lambda o: o.key)

    def read_bytes(self, key: str) -> bytes:
        """Return the file's content."""
        return self._path(key).read_bytes()

    def write_bytes(
        self, key: str, data: bytes, content_type: str = "application/octet-stream"
    ) -> None:
        """Atomically write ``data`` (temp file + rename)."""
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(f".{path.name}.tmp")
        tmp.write_bytes(data)
        tmp.replace(path)

    def download_file(self, key: str, path: Path) -> None:
        """Copy the file to ``path``."""
        shutil.copyfile(self._path(key), path)

    def upload_file(self, path: Path, key: str, md5_hex: str | None = None) -> None:
        """Copy ``path`` into the bucket, verifying MD5 when given."""
        dest = self._path(key)
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_name(f".{dest.name}.tmp")
        shutil.copyfile(path, tmp)
        if md5_hex and md5_file(tmp) != md5_hex:
            tmp.unlink()
            raise OSError(f"MD5 mismatch writing {key}")
        tmp.replace(dest)

    def md5(self, key: str) -> str | None:
        """Hex MD5 of the file."""
        path = self._path(key)
        return md5_file(path) if path.exists() else None

    def exists(self, key: str) -> bool:
        """Whether the file exists."""
        return self._path(key).exists()


# ----------------------------------------------------------------------------- S3
class S3ObjectStore:
    """Amazon S3 implementation (boto3)."""

    def __init__(self, bucket: str, client: Any | None = None) -> None:
        import boto3

        self.bucket = bucket
        self.uri = f"s3://{bucket}"
        self.client = client or boto3.client("s3")

    def _iter(self, prefix: str) -> Iterator[dict[str, Any]]:
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            yield from page.get("Contents", [])

    def list(self, prefix: str) -> list[ObjectInfo]:
        """All objects under ``prefix``."""
        return [
            ObjectInfo(o["Key"], int(o["Size"]), o["LastModified"])
            for o in self._iter(prefix)
            if not o["Key"].endswith("/")
        ]

    def read_bytes(self, key: str) -> bytes:
        """Return the object's content."""
        body: bytes = self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()
        return body

    def write_bytes(
        self, key: str, data: bytes, content_type: str = "application/octet-stream"
    ) -> None:
        """Put the object (server-side encryption is enforced by the bucket default)."""
        md5_b64 = base64.b64encode(hashlib.md5(data).digest()).decode()  # noqa: S324
        self.client.put_object(
            Bucket=self.bucket, Key=key, Body=data, ContentType=content_type, ContentMD5=md5_b64
        )

    def download_file(self, key: str, path: Path) -> None:
        """Stream the object to ``path``."""
        self.client.download_file(self.bucket, key, str(path))

    def upload_file(self, path: Path, key: str, md5_hex: str | None = None) -> None:
        """Upload ``path``; S3 verifies Content-MD5 for single-part uploads."""
        if md5_hex and path.stat().st_size < 5 * 1024 * 1024 * 1024:
            md5_b64 = base64.b64encode(bytes.fromhex(md5_hex)).decode()
            with path.open("rb") as fh:
                self.client.put_object(Bucket=self.bucket, Key=key, Body=fh, ContentMD5=md5_b64)
        else:
            self.client.upload_file(str(path), self.bucket, key)

    def md5(self, key: str) -> str | None:
        """S3 ETag is not an MD5 under SSE-KMS / multipart, so it is never trusted."""
        return None

    def exists(self, key: str) -> bool:
        """Whether the object exists."""
        from botocore.exceptions import ClientError

        try:
            self.client.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise


# ----------------------------------------------------------------------------- GCS
class GcsObjectStore:
    """Google Cloud Storage implementation (google-cloud-storage)."""

    def __init__(self, bucket: str, client: Any | None = None) -> None:
        if client is None:
            from google.cloud import storage  # type: ignore[attr-defined]

            client = storage.Client()
        self.client = client
        self.bucket_name = bucket
        self.bucket = client.bucket(bucket)
        self.uri = f"gs://{bucket}"

    def list(self, prefix: str) -> list[ObjectInfo]:
        """All objects under ``prefix``."""
        return [
            ObjectInfo(b.name, int(b.size), b.updated)
            for b in self.client.list_blobs(self.bucket_name, prefix=prefix)
        ]

    def read_bytes(self, key: str) -> bytes:
        """Return the object's content."""
        data: bytes = self.bucket.blob(key).download_as_bytes()
        return data

    def write_bytes(
        self, key: str, data: bytes, content_type: str = "application/octet-stream"
    ) -> None:
        """Upload ``data`` with an MD5 integrity check."""
        self.bucket.blob(key).upload_from_string(data, content_type=content_type, checksum="md5")

    def download_file(self, key: str, path: Path) -> None:
        """Stream the object to ``path`` (client validates the checksum)."""
        self.bucket.blob(key).download_to_filename(str(path), checksum="md5")

    def upload_file(self, path: Path, key: str, md5_hex: str | None = None) -> None:
        """Upload ``path``; GCS rejects the write if the MD5 does not match."""
        blob = self.bucket.blob(key)
        if md5_hex:
            blob.md5_hash = base64.b64encode(bytes.fromhex(md5_hex)).decode()
        blob.upload_from_filename(str(path), checksum="md5")

    def md5(self, key: str) -> str | None:
        """Hex MD5 from GCS object metadata."""
        blob = self.bucket.get_blob(key)
        if blob is None or not blob.md5_hash:
            return None
        return base64.b64decode(blob.md5_hash).hex()

    def exists(self, key: str) -> bool:
        """Whether the object exists."""
        return bool(self.bucket.blob(key).exists())


def store_for_uri(uri: str) -> ObjectStore:
    """Build the right ``ObjectStore`` for ``s3://``, ``gs://`` or ``file://`` URIs."""
    parsed = urlparse(uri)
    if parsed.scheme == "s3":
        return S3ObjectStore(parsed.netloc)
    if parsed.scheme == "gs":
        return GcsObjectStore(parsed.netloc)
    if parsed.scheme == "file":
        return LocalObjectStore(Path(parsed.path))
    raise ValueError(f"Unsupported storage URI {uri!r}")


def spark_path(uri: str, key: str) -> str:
    """Path string Spark can read/write for ``key`` inside bucket ``uri``."""
    parsed = urlparse(uri)
    if parsed.scheme == "file":
        return str(Path(parsed.path) / key)
    return f"{uri.rstrip('/')}/{key}"
