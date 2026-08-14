# -*- coding: utf-8 -*-
# Copyright (c) 2025 relakkes@gmail.com
#
# This file is part of MediaCrawler project.
# Repository: https://github.com/NanmiCoder/MediaCrawler/blob/main/integration\ruoyi_media\worker.py
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

import argparse
import asyncio
import os
import socket
import traceback
from contextlib import suppress
from typing import Any

import config
from media_platform.douyin import DouYinCrawler
from tools import utils

from .client import RuoyiMediaClient
from .collector import DouyinJobCollector
from .object_storage import MinioAssetStorage, MinioStorageSettings


SUPPORTED_JOB_TYPES = [
    "CREATOR_FULL_SYNC",
    "CREATOR_INCREMENTAL_SYNC",
    "POST_IMPORT",
    "POST_COMMENT_SYNC",
    "POST_MEDIA_DOWNLOAD",
]


class RuoyiMediaWorker:
    def __init__(
        self,
        client: RuoyiMediaClient,
        worker_id: str,
        *,
        lease_seconds: int = 120,
        poll_seconds: float = 5.0,
        storage: MinioAssetStorage | None = None,
    ) -> None:
        self.client = client
        self.worker_id = worker_id
        self.lease_seconds = max(30, min(1800, lease_seconds))
        self.poll_seconds = max(1.0, poll_seconds)
        self.storage = storage

    async def run_once(self) -> bool:
        job = await self.client.claim(
            self.worker_id,
            SUPPORTED_JOB_TYPES,
            self.lease_seconds,
        )
        if not job:
            return False
        await self._execute(job)
        return True

    async def run_forever(self) -> None:
        utils.logger.info(f"[RuoyiMediaWorker] worker started: {self.worker_id}")
        while True:
            handled = await self.run_once()
            if not handled:
                await asyncio.sleep(self.poll_seconds)

    async def _execute(self, job: dict[str, Any]) -> None:
        job_id = str(job["jobId"])
        attempt_no = int(job["attemptNo"])
        request_payload = job.get("requestPayload") or {}
        collector = DouyinJobCollector(request_payload)
        crawler: DouYinCrawler | None = None
        heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(job_id, attempt_no),
            name=f"ruoyi-heartbeat-{job_id}",
        )
        try:
            self._configure_crawler(job)
            await self.client.heartbeat(
                job_id,
                self.worker_id,
                attempt_no,
                progress=10,
                stage="STARTING_BROWSER",
                lease_seconds=self.lease_seconds,
            )
            collector.activate()
            crawler = DouYinCrawler()
            await crawler.start()
            collector.deactivate()

            await self.client.heartbeat(
                job_id,
                self.worker_id,
                attempt_no,
                progress=75,
                stage="INGESTING",
                lease_seconds=self.lease_seconds,
            )
            ingest_payload = collector.build_ingest_payload()
            if not ingest_payload["posts"]:
                raise RuntimeError("Douyin crawl returned no post data")
            download_requested = bool(request_payload.get("downloadMedia")) or job["jobType"] == "POST_MEDIA_DOWNLOAD"
            download_failures = 0
            if download_requested and self.storage is not None:
                await self.client.heartbeat(
                    job_id,
                    self.worker_id,
                    attempt_no,
                    progress=78,
                    stage="DOWNLOADING_MEDIA",
                    lease_seconds=self.lease_seconds,
                )
                download_failures = await self._store_assets(ingest_payload)
                self._update_post_media_status(ingest_payload)
            summary = await self._ingest_in_batches(job_id, attempt_no, ingest_payload)
            summary["mediaStorage"] = "MINIO" if self.storage is not None and download_requested else "REMOTE_ONLY"
            summary["downloadFailures"] = download_failures
            storage_skipped = download_requested and self.storage is None
            summary["mediaStorageSkipped"] = storage_skipped
            await self.client.complete(
                job_id,
                self.worker_id,
                attempt_no,
                status="PARTIAL" if storage_skipped or download_failures else "SUCCEEDED",
                result_summary=summary,
            )
            utils.logger.info(f"[RuoyiMediaWorker] job completed: {job_id}, summary={summary}")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            utils.logger.error(f"[RuoyiMediaWorker] job failed: {job_id}: {exc}\n{traceback.format_exc()}")
            with suppress(Exception):
                await self.client.complete(
                    job_id,
                    self.worker_id,
                    attempt_no,
                    status="FAILED",
                    result_summary={
                        "capturedPosts": len(collector.awemes),
                        "capturedComments": sum(len(items) for items in collector.comments.values()),
                    },
                    error_code=type(exc).__name__.upper(),
                    error_message=str(exc)[:900],
                )
        finally:
            collector.deactivate()
            heartbeat_task.cancel()
            with suppress(asyncio.CancelledError):
                await heartbeat_task
            if crawler is not None:
                with suppress(Exception):
                    await crawler.close()

    async def _heartbeat_loop(self, job_id: str, attempt_no: int) -> None:
        interval = max(10, self.lease_seconds // 3)
        while True:
            await asyncio.sleep(interval)
            await self.client.heartbeat(
                job_id,
                self.worker_id,
                attempt_no,
                stage="CRAWLING",
                lease_seconds=self.lease_seconds,
            )

    async def _ingest_in_batches(
        self,
        job_id: str,
        attempt_no: int,
        payload: dict[str, Any],
        batch_size: int = 20,
    ) -> dict[str, Any]:
        posts = payload["posts"]
        comments = payload["comments"]
        assets = payload["assets"]
        summary = {
            "creators": 1 if payload.get("creator") else 0,
            "posts": 0,
            "comments": 0,
            "assets": 0,
        }
        for offset in range(0, len(posts), batch_size):
            post_batch = posts[offset : offset + batch_size]
            post_ids = {item["platformPostId"] for item in post_batch}
            comment_batch = [item for item in comments if item["platformPostId"] in post_ids]
            asset_batch = [item for item in assets if item["platformPostId"] in post_ids]
            await self.client.ingest(
                job_id,
                self.worker_id,
                attempt_no,
                {
                    "creator": payload.get("creator"),
                    "posts": post_batch,
                    "comments": comment_batch,
                    "assets": asset_batch,
                    "rawPayloads": [],
                },
            )
            summary["posts"] += len(post_batch)
            summary["comments"] += len(comment_batch)
            summary["assets"] += len(asset_batch)
        return summary

    async def _store_assets(self, payload: dict[str, Any]) -> int:
        stored_assets: list[dict[str, Any]] = []
        failures = 0
        for asset in payload["assets"]:
            try:
                stored_assets.append(await self.storage.store(asset))
            except Exception as exc:
                failures += 1
                stored_assets.append(
                    {
                        **asset,
                        "storageProvider": "REMOTE",
                        "downloadStatus": "FAILED",
                        "errorMessage": str(exc)[:900],
                    }
                )
                utils.logger.warning(
                    f"[RuoyiMediaWorker] asset download failed: "
                    f"{asset.get('platformPostId')}/{asset.get('assetType')}: {exc}"
                )
        payload["assets"] = stored_assets
        return failures

    @staticmethod
    def _update_post_media_status(payload: dict[str, Any]) -> None:
        assets_by_post: dict[str, list[dict[str, Any]]] = {}
        for asset in payload["assets"]:
            assets_by_post.setdefault(str(asset["platformPostId"]), []).append(asset)
        for post in payload["posts"]:
            assets = assets_by_post.get(str(post["platformPostId"]), [])
            succeeded = sum(asset.get("downloadStatus") == "SUCCEEDED" for asset in assets)
            if assets and succeeded == len(assets):
                post["mediaStatus"] = "SUCCEEDED"
            elif succeeded:
                post["mediaStatus"] = "PARTIAL"
            elif assets:
                post["mediaStatus"] = "FAILED"

    def _configure_crawler(self, job: dict[str, Any]) -> None:
        job_type = str(job["jobType"])
        payload = job.get("requestPayload") or {}
        config.PLATFORM = "dy"
        config.SAVE_DATA_OPTION = "ruoyi"
        config.ENABLE_GET_MEIDAS = False
        config.MAX_CONCURRENCY_NUM = 1
        config.CRAWLER_MAX_SLEEP_SEC = max(1, config.CRAWLER_MAX_SLEEP_SEC)

        comment_policy = payload.get("commentPolicy") or {}
        config.CRAWLER_MAX_COMMENTS_COUNT_SINGLENOTES = int(comment_policy.get("sampleLimit") or 100)
        config.ENABLE_GET_SUB_COMMENTS = bool(comment_policy.get("fetchHotReplies", True))

        if job_type.startswith("CREATOR_"):
            creator_source = payload.get("profileUrl") or payload.get("platformCreatorId")
            if not creator_source:
                raise ValueError("Creator task is missing profileUrl/platformCreatorId")
            config.CRAWLER_TYPE = "creator"
            config.DY_CREATOR_ID_LIST = [creator_source]
            config.ENABLE_GET_COMMENTS = True
            return

        source_url = payload.get("sourceUrl") or payload.get("canonicalUrl")
        if not source_url:
            raise ValueError("Post task is missing sourceUrl/canonicalUrl")
        config.CRAWLER_TYPE = "detail"
        config.DY_SPECIFIED_ID_LIST = [source_url]
        config.ENABLE_GET_COMMENTS = job_type != "POST_MEDIA_DOWNLOAD" and bool(payload.get("fetchComments", True))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="MediaCrawler worker for RuoYi Media")
    parser.add_argument("--once", action="store_true", help="claim at most one job and exit")
    parser.add_argument("--base-url", default=os.getenv("RUOYI_MEDIA_BASE_URL", "http://127.0.0.1:8080"))
    parser.add_argument("--token", default=os.getenv("RUOYI_MEDIA_WORKER_TOKEN", "ruoyi-media-dev-token"))
    parser.add_argument(
        "--worker-id",
        default=os.getenv("RUOYI_MEDIA_WORKER_ID", f"{socket.gethostname()}-{os.getpid()}"),
    )
    parser.add_argument("--lease-seconds", type=int, default=int(os.getenv("RUOYI_MEDIA_LEASE_SECONDS", "120")))
    parser.add_argument("--poll-seconds", type=float, default=float(os.getenv("RUOYI_MEDIA_POLL_SECONDS", "5")))
    return parser.parse_args()


async def main() -> None:
    args = _parse_args()
    storage_settings = MinioStorageSettings.from_env()
    storage = MinioAssetStorage(storage_settings) if storage_settings else None
    async with RuoyiMediaClient(args.base_url, args.token) as client:
        worker = RuoyiMediaWorker(
            client,
            args.worker_id,
            lease_seconds=args.lease_seconds,
            poll_seconds=args.poll_seconds,
            storage=storage,
        )
        if args.once:
            handled = await worker.run_once()
            if not handled:
                utils.logger.info("[RuoyiMediaWorker] no pending job")
        else:
            await worker.run_forever()


if __name__ == "__main__":
    asyncio.run(main())
