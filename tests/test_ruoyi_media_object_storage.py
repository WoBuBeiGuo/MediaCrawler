# -*- coding: utf-8 -*-
# Copyright (c) 2025 relakkes@gmail.com
#
# This file is part of MediaCrawler project.
# Repository: https://github.com/NanmiCoder/MediaCrawler/blob/main/tests\test_ruoyi_media_object_storage.py
# GitHub: https://github.com/NanmiCoder
# Licensed under NON-COMMERCIAL LEARNING LICENSE 1.1
#
# 声明：本代码仅供学习和研究目的使用。使用者应遵守以下原则：
# 1. 不得用于任何商业用途。
# 2. 使用时应遵守目标平台的使用条款和robots.txt规则。
# 3. 不得进行大规模爬取或对平台造成运营干扰。
# 4. 应合理控制请求频率，避免给目标平台带来不必要的负担。
# 5. 不得用于任何非法或不当的用途。
#
# 详细许可条款请参阅项目根目录下的LICENSE文件。
# 使用本代码即表示您同意遵守上述原则和LICENSE中的所有条款。

from __future__ import annotations

from pathlib import Path

import pytest

from integration.ruoyi_media.object_storage import MinioAssetStorage, MinioStorageSettings
from integration.ruoyi_media.worker import RuoyiMediaWorker


class FakeMinio:
    def __init__(self) -> None:
        self.created_bucket: str | None = None
        self.upload: tuple[str, str, bytes, str] | None = None

    def bucket_exists(self, _bucket: str) -> bool:
        return False

    def make_bucket(self, bucket: str, location: str | None = None) -> None:
        self.created_bucket = bucket

    def fput_object(self, bucket: str, key: str, path: str, content_type: str) -> None:
        self.upload = (bucket, key, Path(path).read_bytes(), content_type)


@pytest.mark.asyncio
async def test_store_asset_uploads_to_deterministic_minio_key(tmp_path: Path) -> None:
    settings = MinioStorageSettings(
        endpoint="http://minio.example.test:9000",
        access_key="access",
        secret_key="secret",
        bucket="ruoyi-media",
    )
    storage = MinioAssetStorage(settings)
    fake_minio = FakeMinio()
    storage._client = fake_minio
    downloaded = tmp_path / "asset.part"
    downloaded.write_bytes(b"media-bytes")

    async def fake_download(_source_url: str) -> tuple[str, int, str, str]:
        return str(downloaded), 11, "a" * 64, "video/mp4"

    storage._download = fake_download
    result = await storage.store(
        {
            "platformPostId": "73800001",
            "assetType": "VIDEO",
            "sequenceNo": 0,
            "sourceUrl": "https://example.test/video",
            "downloadStatus": "PENDING",
        }
    )

    assert fake_minio.created_bucket == "ruoyi-media"
    assert fake_minio.upload == (
        "ruoyi-media",
        "douyin/73800001/video-0.mp4",
        b"media-bytes",
        "video/mp4",
    )
    assert result["storageProvider"] == "MINIO"
    assert result["downloadStatus"] == "SUCCEEDED"
    assert result["objectKey"] == "douyin/73800001/video-0.mp4"
    assert not downloaded.exists()


def test_post_media_status_reflects_asset_results() -> None:
    payload = {
        "posts": [
            {"platformPostId": "1", "mediaStatus": "PENDING"},
            {"platformPostId": "2", "mediaStatus": "PENDING"},
            {"platformPostId": "3", "mediaStatus": "PENDING"},
        ],
        "assets": [
            {"platformPostId": "1", "downloadStatus": "SUCCEEDED"},
            {"platformPostId": "2", "downloadStatus": "SUCCEEDED"},
            {"platformPostId": "2", "downloadStatus": "FAILED"},
            {"platformPostId": "3", "downloadStatus": "FAILED"},
        ],
    }

    RuoyiMediaWorker._update_post_media_status(payload)

    assert [post["mediaStatus"] for post in payload["posts"]] == ["SUCCEEDED", "PARTIAL", "FAILED"]
