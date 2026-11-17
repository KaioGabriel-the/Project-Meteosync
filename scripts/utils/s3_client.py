import hashlib
import json
import logging
import os
from pathlib import Path, PurePosixPath
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger(__name__)


class S3ClientWrapper:
    def __init__(self, client: Any | None = None) -> None:
        self.endpoint_url = os.getenv("MINIO_ENDPOINT_URL", "http://localhost:9000")
        self.aws_access_key_id = os.getenv(
            "MINIO_ROOT_USER",
            os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        )
        self.aws_secret_access_key = os.getenv(
            "MINIO_ROOT_PASSWORD",
            os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        )
        self.bucket_name = os.getenv("MINIO_BUCKET_BRONZE", "bronze")
        self.use_local_fallback = os.getenv("USE_LOCAL_FALLBACK", "true").lower() == "true"
        self.local_root = Path(__file__).resolve().parents[2] / "data" / "raw"
        self.s3_client = (
            client
            if client is not None
            else boto3.client(
                "s3",
                endpoint_url=self.endpoint_url,
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                config=Config(s3={"addressing_style": "path"}),
            )
        )

    @staticmethod
    def _validate_key(s3_key: str) -> None:
        key_path = PurePosixPath(s3_key)
        if (
            not s3_key
            or not key_path.parts
            or "\\" in s3_key
            or ":" in key_path.parts[0]
            or key_path.is_absolute()
            or ".." in key_path.parts
        ):
            raise ValueError(f"Chave S3 inválida: {s3_key!r}")

    @staticmethod
    def _is_missing_object(error: ClientError) -> bool:
        return error.response.get("Error", {}).get("Code") in {
            "404",
            "NoSuchKey",
            "NotFound",
        }

    @staticmethod
    def _is_transient_error(error: BotoCoreError | ClientError) -> bool:
        if isinstance(error, BotoCoreError):
            return True
        status_code = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
        return status_code in {408, 429, 500, 502, 503, 504}

    def _local_path(self, s3_key: str) -> Path:
        self._validate_key(s3_key)
        return self.local_root.joinpath(*PurePosixPath(s3_key).parts)

    def _check_existing_object(self, s3_key: str, expected_sha256: str) -> bool:
        try:
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
        except ClientError as error:
            if self._is_missing_object(error):
                return False
            raise

        stored_sha256 = response.get("Metadata", {}).get("sha256")
        if stored_sha256 is None:
            existing = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            stored_sha256 = hashlib.sha256(existing["Body"].read()).hexdigest()
        if stored_sha256 != expected_sha256:
            raise FileExistsError(f"s3://{self.bucket_name}/{s3_key} já existe com outro conteúdo.")
        return True

    def _upload_local(
        self,
        content: bytes,
        s3_key: str,
        expected_sha256: str,
        overwrite: bool,
    ) -> str:
        local_path = self._local_path(s3_key)
        local_path.parent.mkdir(parents=True, exist_ok=True)

        if local_path.exists() and not overwrite:
            existing_sha256 = hashlib.sha256(local_path.read_bytes()).hexdigest()
            if existing_sha256 != expected_sha256:
                raise FileExistsError(f"{local_path} já existe com outro conteúdo.")
            logger.info("Arquivo local %s já existe; mantendo o conteúdo.", local_path)
            return str(local_path)

        if overwrite:
            local_path.write_bytes(content)
        else:
            try:
                with local_path.open("xb") as file:
                    file.write(content)
            except FileExistsError:
                existing_sha256 = hashlib.sha256(local_path.read_bytes()).hexdigest()
                if existing_sha256 != expected_sha256:
                    raise
        logger.info("Dados salvos via fallback local em: %s", local_path)
        return str(local_path)

    def upload_json(self, data: dict, s3_key: str, overwrite: bool = False) -> str:
        self._validate_key(s3_key)
        content = json.dumps(data, ensure_ascii=False, indent=4).encode("utf-8")
        expected_sha256 = hashlib.sha256(content).hexdigest()

        try:
            if not overwrite and self._check_existing_object(s3_key, expected_sha256):
                logger.info("Arquivo s3://%s/%s já existe; mantendo o conteúdo.", self.bucket_name, s3_key)
                return f"s3a://{self.bucket_name}/{s3_key}"

            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=content,
                ContentType="application/json",
                Metadata={"sha256": expected_sha256},
            )
            logger.info("Upload realizado com sucesso para s3a://%s/%s", self.bucket_name, s3_key)
            return f"s3a://{self.bucket_name}/{s3_key}"
        except (BotoCoreError, ClientError) as error:
            if not self.use_local_fallback or not self._is_transient_error(error):
                raise
            logger.warning("MinIO temporariamente indisponível (%s); usando fallback local.", error)
            return self._upload_local(content, s3_key, expected_sha256, overwrite)

    def list_objects(self, prefix: str = "") -> list[str]:
        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")
            return [
                item["Key"]
                for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)
                for item in page.get("Contents", [])
            ]
        except (BotoCoreError, ClientError) as error:
            if not self.use_local_fallback or not self._is_transient_error(error):
                raise
            logger.warning("MinIO temporariamente indisponível (%s); listando fallback local.", error)
            if not self.local_root.exists():
                return []
            return [
                path.relative_to(self.local_root).as_posix()
                for path in self.local_root.rglob("*")
                if path.is_file()
                and path.relative_to(self.local_root).as_posix().startswith(prefix)
            ]

    def verify_integrity(self, s3_key: str, expected_sha256: str) -> bool:
        self._validate_key(s3_key)
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            content = response["Body"].read()
        except (BotoCoreError, ClientError) as error:
            if not self.use_local_fallback or not self._is_transient_error(error):
                raise
            logger.warning("MinIO temporariamente indisponível (%s); verificando fallback local.", error)
            content = self._local_path(s3_key).read_bytes()
        return hashlib.sha256(content).hexdigest() == expected_sha256
