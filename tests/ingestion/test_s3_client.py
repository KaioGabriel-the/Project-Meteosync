import hashlib
import io
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from botocore.exceptions import ClientError, EndpointConnectionError

from scripts.utils.s3_client import S3ClientWrapper


class MemoryS3Client:
    def __init__(self):
        self.objects = {}
        self.put_calls = []

    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            raise ClientError(
                {"Error": {"Code": "404"}, "ResponseMetadata": {"HTTPStatusCode": 404}},
                "HeadObject",
            )
        return {"Metadata": self.objects[Key]["Metadata"]}

    def put_object(self, **kwargs):
        self.put_calls.append(kwargs)
        self.objects[kwargs["Key"]] = {
            "Body": kwargs["Body"],
            "Metadata": kwargs["Metadata"],
        }

    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(self.objects[Key]["Body"])}

    def get_paginator(self, operation_name):
        self.operation_name = operation_name
        return self

    def paginate(self, Bucket, Prefix):
        keys = [
            {"Key": key}
            for key in self.objects
            if key.startswith(Prefix)
        ]
        return [{"Contents": keys}]


class UnavailableS3Client:
    def _raise_unavailable(self, *args, **kwargs):
        raise EndpointConnectionError(endpoint_url="http://minio:9000")

    head_object = _raise_unavailable
    put_object = _raise_unavailable
    get_object = _raise_unavailable
    get_paginator = _raise_unavailable


class S3ClientWrapperTests(unittest.TestCase):
    def test_reads_compose_minio_credentials(self):
        with (
            patch.dict(
                os.environ,
                {
                    "MINIO_ENDPOINT_URL": "http://minio:9000",
                    "MINIO_ROOT_USER": "test-user",
                    "MINIO_ROOT_PASSWORD": "test-password",
                },
                clear=True,
            ),
            patch("scripts.utils.s3_client.boto3.client") as create_client,
        ):
            S3ClientWrapper()

        client_params = create_client.call_args.kwargs
        self.assertEqual(client_params["endpoint_url"], "http://minio:9000")
        self.assertEqual(client_params["aws_access_key_id"], "test-user")
        self.assertEqual(client_params["aws_secret_access_key"], "test-password")
        self.assertEqual(client_params["config"].s3, {"addressing_style": "path"})

    def test_rejects_path_traversal_keys(self):
        wrapper = S3ClientWrapper(client=MemoryS3Client())

        for key in ("../outside.json", r"..\outside.json", "C:/outside.json"):
            with self.subTest(key=key), self.assertRaises(ValueError):
                wrapper.upload_json({"value": 1}, key)

    def test_upload_is_idempotent_and_can_verify_integrity(self):
        client = MemoryS3Client()
        wrapper = S3ClientWrapper(client=client)
        data = {"weather": {"temp": 300}}

        first_uri = wrapper.upload_json(data, "openweather/city.json")
        second_uri = wrapper.upload_json(data, "openweather/city.json")
        expected_digest = hashlib.sha256(client.put_calls[0]["Body"]).hexdigest()

        self.assertEqual(first_uri, "s3a://bronze/openweather/city.json")
        self.assertEqual(second_uri, first_uri)
        self.assertEqual(len(client.put_calls), 1)
        self.assertTrue(wrapper.verify_integrity("openweather/city.json", expected_digest))
        self.assertEqual(wrapper.list_objects("openweather/"), ["openweather/city.json"])

    def test_existing_key_with_different_content_is_not_overwritten(self):
        client = MemoryS3Client()
        wrapper = S3ClientWrapper(client=client)
        wrapper.upload_json({"value": 1}, "open_meteo/city.json")

        with self.assertRaises(FileExistsError):
            wrapper.upload_json({"value": 2}, "open_meteo/city.json")
        self.assertEqual(len(client.put_calls), 1)

    def test_transient_minio_failure_uses_local_fallback(self):
        wrapper = S3ClientWrapper(client=UnavailableS3Client())
        data = {"value": "fallback"}
        digest = hashlib.sha256(b'{\n    "value": "fallback"\n}').hexdigest()

        with tempfile.TemporaryDirectory() as temp_dir:
            wrapper.local_root = Path(temp_dir)
            path = wrapper.upload_json(data, "open_meteo/city.json")

            self.assertTrue(os.path.isfile(path))
            self.assertTrue(wrapper.verify_integrity("open_meteo/city.json", digest))
            self.assertEqual(wrapper.list_objects("open_meteo/"), ["open_meteo/city.json"])
