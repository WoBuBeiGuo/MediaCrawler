# -*- coding: utf-8 -*-
# Copyright (c) 2025 relakkes@gmail.com
#
# This file is part of MediaCrawler project.
# Repository: https://github.com/NanmiCoder/MediaCrawler/blob/main/integration\ruoyi_media\object_storage.py
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

"""Optional MinIO storage for media assets collected by the RuoYi worker."""

from __future__ import annotations

import asyncio
import hashlib
import mimetypes
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import httpx
from minio import Minio


_DOUYIN_DOWNLOAD_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.douyin.com/",
}


@dataclass(frozen=True)
class MinioStorageSettings:
    endpoint: str
    access_key: str
    secret_key: str
    bucket: str = "ruoyi-media"
    region: str | None = None
    max_asset_bytes: int = 500 * 1024 * 1024
    timeout_seconds: float = 300.0

    @classmethod
    def from_env(cls) -> "MinioStorageSettings | None":
        endpoint = os.getenv("RUOYI_MEDIA_MINIO_ENDPOINT", "").strip()
        access_key = os.getenv("RUOYI_MEDIA_MINIO_ACCESS_KEY", "").strip()
        secret_key = os.getenv("RUOYI_MEDIA_MINIO_SECRET_KEY", "").strip()
        configured = [bool(endpoint), bool(access_key), bool(secret_key)]
        if not any(configured):
            return None
        if not all(configured):
            raise ValueError("MinIO endpoint, access key and secret key must be configured together")
        return cls(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            bucket=os.getenv("RUOYI_MEDIA_MINIO_BUCKET", "ruoyi-media").strip() or "ruoyi-media",
            region=os.getenv("RUOYI_MEDIA_MINIO_REGION", "").strip() or None,
            max_asset_bytes=max(1, int(os.getenv("RUOYI_MEDIA_MAX_ASSET_BYTES", "524288000"))),
            timeout_seconds=max(10.0, float(os.getenv("RUOYI_MEDIA_ASSET_TIMEOUT_SECONDS", "300"))),
        )


class MinioAssetStorage:
    """Downloads a remote media asset and stores it in a private MinIO bucket."""

    def __init__(self, settings: MinioStorageSettings) -> None:
        self.settings = settings
        endpoint, secure = _parse_endpoint(settings.endpoint)
        self._client = Minio(
            endpoint,
            access_key=settings.access_key,
            secret_key=settings.secret_key,
            secure=secure,
            region=settings.region,
        )
        self._bucket_ready = False

    async def prepare(self) -> None:
        if self._bucket_ready:
            return
        exists = await asyncio.to_thread(self._client.bucket_exists, self.settings.bucket)
        if not exists:
            await asyncio.to_thread(
                self._client.make_bucket,
                self.settings.bucket,
                location=self.settings.region,
            )
        self._bucket_ready = True

    async def store(self, asset: dict[str, Any]) -> dict[str, Any]:
        source_url = str(asset.get("sourceUrl") or "").strip()
        if not source_url:
            raise ValueError("Asset sourceUrl is empty")
        _validate_source_url(source_url)
        await self.prepare()

        temporary_path: str | None = None
        try:
            temporary_path, size, sha256, content_type = await self._download(source_url)
            object_key, file_name = _build_object_key(asset, source_url, content_type)
            await asyncio.to_thread(
                self._client.fput_object,
                self.settings.bucket,
                object_key,
                temporary_path,
                content_type=content_type,
            )
            return {
                **asset,
                "storageProvider": "MINIO",
                "bucket": self.settings.bucket,
                "objectKey": object_key,
                "fileName": file_name,
                "mimeType": content_type,
                "fileSize": size,
                "sha256": sha256,
                "downloadStatus": "SUCCEEDED",
                "downloadedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "errorMessage": None,
            }
        finally:
            if temporary_path:
                Path(temporary_path).unlink(missing_ok=True)

    async def _download(self, source_url: str) -> tuple[str, int, str, str]:
        temp_file = tempfile.NamedTemporaryFile(prefix="ruoyi-media-", suffix=".part", delete=False)
        temp_path = temp_file.name
        digest = hashlib.sha256()
        size = 0
        timeout = httpx.Timeout(self.settings.timeout_seconds)
        try:
            async with httpx.AsyncClient(
                timeout=timeout,
                follow_redirects=True,
                headers=_DOUYIN_DOWNLOAD_HEADERS,
            ) as client:
                async with client.stream("GET", source_url) as response:
                    response.raise_for_status()
                    content_length = _positive_int(response.headers.get("content-length"))
                    if content_length and content_length > self.settings.max_asset_bytes:
                        raise ValueError(
                            f"Asset is larger than the {self.settings.max_asset_bytes}-byte limit"
                        )
                    content_type = response.headers.get("content-type", "").split(";", 1)[0].strip()
                    for_chunk = response.aiter_bytes(chunk_size=1024 * 1024)
                    async for chunk in for_chunk:
                        size += len(chunk)
                        if size > self.settings.max_asset_bytes:
                            raise ValueError(
                                f"Asset is larger than the {self.settings.max_asset_bytes}-byte limit"
                            )
                        temp_file.write(chunk)
                        digest.update(chunk)
            temp_file.close()
            content_type = content_type or mimetypes.guess_type(urlsplit(source_url).path)[0]
            return temp_path, size, digest.hexdigest(), content_type or "application/octet-stream"
        except Exception:
            temp_file.close()
            Path(temp_path).unlink(missing_ok=True)
            raise


def _parse_endpoint(value: str) -> tuple[str, bool]:
    normalized = value if "://" in value else f"http://{value}"
    parsed = urlsplit(normalized)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("RUOYI_MEDIA_MINIO_ENDPOINT must be an HTTP(S) host and optional port")
    if parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
        raise ValueError("RUOYI_MEDIA_MINIO_ENDPOINT must not contain a path, query or fragment")
    return parsed.netloc, parsed.scheme == "https"


def _validate_source_url(value: str) -> None:
    parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("Asset sourceUrl must be an HTTP(S) URL")


def _build_object_key(asset: dict[str, Any], source_url: str, content_type: str) -> tuple[str, str]:
    post_id = _safe_segment(asset.get("platformPostId") or "unknown")
    asset_type = _safe_segment(str(asset.get("assetType") or "asset").lower())
    sequence_no = max(0, int(asset.get("sequenceNo") or 0))
    extension = _extension_for(content_type, source_url)
    file_name = f"{asset_type}-{sequence_no}{extension}"
    return f"douyin/{post_id}/{file_name}", file_name


def _extension_for(content_type: str, source_url: str) -> str:
    known = {
        "video/mp4": ".mp4",
        "image/jpeg": ".jpg",
        "image/png": ".png",
        "image/webp": ".webp",
        "image/gif": ".gif",
    }
    if content_type in known:
        return known[content_type]
    extension = Path(urlsplit(source_url).path).suffix.lower()
    if extension and len(extension) <= 10 and extension[1:].isalnum():
        return extension
    guessed = mimetypes.guess_extension(content_type or "")
    return guessed if guessed and len(guessed) <= 10 else ".bin"


def _safe_segment(value: Any) -> str:
    text = str(value)
    safe = "".join(character for character in text if character.isalnum() or character in {"-", "_"})
    return safe[:128] or "unknown"


def _positive_int(value: Any) -> int | None:
    try:
        number = int(value)
        return number if number > 0 else None
    except (TypeError, ValueError):
        return None
