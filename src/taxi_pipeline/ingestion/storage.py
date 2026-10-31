"""Accès au stockage objet (MinIO / S3) via boto3."""

from __future__ import annotations

import json

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from taxi_pipeline.config import Settings


class ObjectStore:
    def __init__(self, settings: Settings) -> None:
        self.bucket = settings.bucket
        self.client = boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint,
            aws_access_key_id=settings.s3_access_key,
            aws_secret_access_key=settings.s3_secret_key,
            # MinIO exige l'adressage "path style" (http://minio:9000/bucket/key).
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
            region_name="us-east-1",
        )

    def ensure_bucket(self) -> None:
        try:
            self.client.head_bucket(Bucket=self.bucket)
        except ClientError:
            self.client.create_bucket(Bucket=self.bucket)

    def put_bytes(self, key: str, data: bytes, content_type: str = "text/csv") -> None:
        self.client.put_object(Bucket=self.bucket, Key=key, Body=data, ContentType=content_type)

    def put_json(self, key: str, payload: dict) -> None:
        body = json.dumps(payload, indent=2, default=str).encode("utf-8")
        self.put_bytes(key, body, content_type="application/json")

    def delete_prefix(self, prefix: str) -> int:
        """Supprime tous les objets sous `prefix` et renvoie leur nombre."""
        deleted = 0
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            keys = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
            if keys:
                self.client.delete_objects(Bucket=self.bucket, Delete={"Objects": keys})
                deleted += len(keys)
        return deleted
