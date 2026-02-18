"""MinIO (S3-compatible) filesystem implementation."""

from __future__ import annotations

import io

from de_platform.services.filesystem.interface import FileSystemInterface
from de_platform.services.secrets.interface import SecretsInterface


class MinioFileSystem(FileSystemInterface):
    """Filesystem backed by MinIO (S3-compatible object storage).

    Config (via secrets):
        FS_MINIO_ENDPOINT  - Host:port of MinIO server (default: localhost:9000)
        FS_MINIO_ACCESS_KEY - Access key (default: minioadmin)
        FS_MINIO_SECRET_KEY - Secret key (default: minioadmin)
        FS_MINIO_BUCKET    - Bucket name (default: de-platform)
        FS_MINIO_SECURE    - Use HTTPS (default: false)
    """

    def __init__(self, secrets: SecretsInterface) -> None:
        from minio import Minio

        endpoint = secrets.get_or_default("FS_MINIO_ENDPOINT", "localhost:9000")
        access_key = secrets.get_or_default("FS_MINIO_ACCESS_KEY", "minioadmin")
        secret_key = secrets.get_or_default("FS_MINIO_SECRET_KEY", "minioadmin")
        self._bucket = secrets.get_or_default("FS_MINIO_BUCKET", "de-platform")
        secure = secrets.get_or_default("FS_MINIO_SECURE", "false").lower() == "true"

        self._client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
        self._ensure_bucket()

    def _ensure_bucket(self) -> None:
        if not self._client.bucket_exists(self._bucket):
            self._client.make_bucket(self._bucket)

    def read(self, path: str) -> bytes:
        try:
            response = self._client.get_object(self._bucket, path)
            return response.read()
        except Exception as exc:
            # Translate S3 NoSuchKey to FileNotFoundError
            err_str = str(exc).lower()
            if "nosuchkey" in err_str or "no such key" in err_str or "not found" in err_str:
                raise FileNotFoundError(f"File not found: {path}") from exc
            raise
        finally:
            try:
                response.close()  # type: ignore[union-attr]
                response.release_conn()  # type: ignore[union-attr]
            except Exception:
                pass

    def write(self, path: str, data: bytes) -> None:
        self._client.put_object(
            self._bucket,
            path,
            io.BytesIO(data),
            length=len(data),
        )

    def list(self, prefix: str) -> list[str]:
        objects = self._client.list_objects(self._bucket, prefix=prefix, recursive=True)
        return [obj.object_name for obj in objects if obj.object_name]

    def delete(self, path: str) -> bool:
        try:
            self._client.remove_object(self._bucket, path)
            return True
        except Exception as exc:
            err_str = str(exc).lower()
            if "nosuchkey" in err_str or "no such key" in err_str or "not found" in err_str:
                return False
            raise

    def exists(self, path: str) -> bool:
        try:
            self._client.stat_object(self._bucket, path)
            return True
        except Exception:
            return False

    def health_check(self) -> bool:
        try:
            self._client.bucket_exists(self._bucket)
            return True
        except Exception:
            return False
