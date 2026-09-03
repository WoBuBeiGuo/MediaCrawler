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

from .browser_session import PersistentDouyinBrowser
from .client import (
    RuoyiMediaClient,
    RuoyiMediaClientRejectedError,
    RuoyiMediaClientTransportError,
)
from .collector import DouyinJobCollector
from .object_storage import MinioAssetStorage, MinioStorageSettings


SUPPORTED_JOB_TYPES = [
    "CREATOR_FULL_SYNC",
    "CREATOR_INCREMENTAL_SYNC",
    "POST_IMPORT",
    "POST_COMMENT_SYNC",
    "POST_ASSET_REFRESH",
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
        self.browser_session = PersistentDouyinBrowser()

    async def start(self) -> None:
        await self.browser_session.get_context()

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
            try:
                handled = await self.run_once()
            except asyncio.CancelledError:
                raise
            except RuoyiMediaClientTransportError as exc:
                utils.logger.warning(
                    f"[RuoyiMediaWorker] Java backend temporarily unavailable while polling; "
                    f"retrying in {self.poll_seconds}s: {exc}"
                )
                await asyncio.sleep(self.poll_seconds)
                continue
            except RuoyiMediaClientRejectedError as exc:
                utils.logger.error(
                    f"[RuoyiMediaWorker] Java backend rejected the job claim; "
                    f"check Worker Token and backend configuration: {exc}"
                )
                await asyncio.sleep(self.poll_seconds)
                continue
            except Exception as exc:
                utils.logger.exception(
                    f"[RuoyiMediaWorker] unexpected polling failure; "
                    f"retrying in {self.poll_seconds}s: {exc}"
                )
                await asyncio.sleep(self.poll_seconds)
                continue
            if not handled:
                await asyncio.sleep(self.poll_seconds)

    async def close(self) -> None:
        await self.browser_session.close()

    async def _execute(self, job: dict[str, Any]) -> None:
        job_id = str(job["jobId"])
        attempt_no = int(job["attemptNo"])
        request_payload = job.get("requestPayload") or {}
        collector = DouyinJobCollector(request_payload)
        crawler: DouYinCrawler | None = None
        anonymous_context = None
        heartbeat_stop = asyncio.Event()
        heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(job_id, attempt_no, heartbeat_stop),
            name=f"ruoyi-heartbeat-{job_id}",
        )
        try:
            if job["jobType"] == "POST_MEDIA_DOWNLOAD":
                await self._download_stored_remote_assets(
                    job_id,
                    attempt_no,
                    request_payload,
                    heartbeat_stop=heartbeat_stop,
                    heartbeat_task=heartbeat_task,
                )
                return

            self._configure_crawler(job)
            await self.client.heartbeat(
                job_id,
                self.worker_id,
                attempt_no,
                progress=10,
                stage="STARTING_ANONYMOUS_REFRESH"
                if job["jobType"] == "POST_ASSET_REFRESH"
                else "STARTING_BROWSER",
                lease_seconds=self.lease_seconds,
            )
            collector.activate()
            if job["jobType"] == "POST_ASSET_REFRESH":
                anonymous_context = await self.browser_session.new_anonymous_context()
                crawler = DouYinCrawler(browser_context=anonymous_context, allow_login=False)
            else:
                browser_context = await self.browser_session.get_context()
                crawler = DouYinCrawler(browser_context=browser_context)
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
                if job["jobType"] == "POST_ASSET_REFRESH":
                    raise RuntimeError("Douyin public work page returned no anonymous post data")
                raise RuntimeError("Douyin crawl returned no post data")
            if job["jobType"] == "POST_ASSET_REFRESH":
                actual_asset_types = {
                    str(asset.get("assetType") or "")
                    for asset in ingest_payload["assets"]
                    if asset.get("sourceUrl")
                }
                expected_asset_types = {
                    str(asset_type)
                    for asset_type in request_payload.get("expectedAssetTypes") or []
                    if asset_type
                }
                if not actual_asset_types:
                    raise RuntimeError("Douyin detail crawl returned no refreshable asset URLs")
                missing_asset_types = expected_asset_types - actual_asset_types
                if missing_asset_types:
                    missing = ", ".join(sorted(missing_asset_types))
                    raise RuntimeError(f"Douyin detail crawl did not refresh expected asset types: {missing}")
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
            if job["jobType"] == "POST_ASSET_REFRESH":
                summary["refreshMode"] = "ANONYMOUS_PUBLIC_PAGE"
                summary["authenticationMode"] = "ANONYMOUS"
                summary["refreshedAssetTypes"] = sorted(
                    {
                        str(asset.get("assetType"))
                        for asset in ingest_payload["assets"]
                        if asset.get("assetType") and asset.get("sourceUrl")
                    }
                )
            if bool(request_payload.get("fetchComments")) or job["jobType"] == "POST_COMMENT_SYNC":
                comment_policy = request_payload.get("commentPolicy") or {}
                scan_limit = max(1, min(1000, int(comment_policy.get("sampleLimit") or 1000)))
                first_level_counts: dict[str, int] = {}
                hot_first_level = 0
                hot_replies = 0
                for comment in ingest_payload["comments"]:
                    if comment.get("platformParentCommentId"):
                        hot_replies += int(bool(comment.get("isHot")))
                        continue
                    post_id = str(comment.get("platformPostId") or "")
                    first_level_counts[post_id] = first_level_counts.get(post_id, 0) + 1
                    hot_first_level += int(bool(comment.get("isHot")))
                summary["firstLevelComments"] = sum(first_level_counts.values())
                summary["hotFirstLevelComments"] = hot_first_level
                summary["hotReplies"] = hot_replies
                summary["commentScanLimit"] = scan_limit
                summary["reachedCommentScanLimit"] = any(
                    count >= scan_limit for count in first_level_counts.values()
                )
            summary["mediaStorage"] = "MINIO" if self.storage is not None and download_requested else "REMOTE_ONLY"
            summary["downloadFailures"] = download_failures
            storage_skipped = download_requested and self.storage is None
            summary["mediaStorageSkipped"] = storage_skipped
            await self._stop_heartbeat(heartbeat_stop, heartbeat_task)
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
            await self._stop_heartbeat(heartbeat_stop, heartbeat_task)
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
            await self._stop_heartbeat(heartbeat_stop, heartbeat_task)
            if crawler is not None:
                with suppress(Exception):
                    await crawler.close()
            if anonymous_context is not None:
                with suppress(Exception):
                    await anonymous_context.close()

    async def _download_stored_remote_assets(
        self,
        job_id: str,
        attempt_no: int,
        request_payload: dict[str, Any],
        *,
        heartbeat_stop: asyncio.Event,
        heartbeat_task: asyncio.Task[None],
    ) -> None:
        """Download DB-provided remote URLs without opening Douyin or using cookies."""
        assets = request_payload.get("assets") or []
        if not isinstance(assets, list) or not assets:
            raise RuntimeError("Media download job has no stored remote asset URLs")

        await self.client.heartbeat(
            job_id,
            self.worker_id,
            attempt_no,
            progress=15,
            stage="DOWNLOADING_REMOTE_MEDIA",
            lease_seconds=self.lease_seconds,
        )
        if self.storage is None:
            await self._stop_heartbeat(heartbeat_stop, heartbeat_task)
            await self.client.complete(
                job_id,
                self.worker_id,
                attempt_no,
                status="PARTIAL",
                result_summary={
                    "assets": 0,
                    "requestedAssets": len(assets),
                    "downloadFailures": 0,
                    "mediaStorage": "REMOTE_ONLY",
                    "mediaStorageSkipped": True,
                    "downloadMode": "ANONYMOUS_HTTP",
                },
                error_code="MINIO_NOT_CONFIGURED",
                error_message="MinIO is not configured; remote URLs were not downloaded",
            )
            return

        ingest_payload = {"assets": [dict(asset) for asset in assets]}
        download_failures = await self._store_assets(ingest_payload)
        await self.client.heartbeat(
            job_id,
            self.worker_id,
            attempt_no,
            progress=90,
            stage="UPDATING_MEDIA_ASSETS",
            lease_seconds=self.lease_seconds,
        )
        await self.client.ingest(
            job_id,
            self.worker_id,
            attempt_no,
            {
                "creator": None,
                "posts": [],
                "comments": [],
                "assets": ingest_payload["assets"],
                "rawPayloads": [],
            },
        )

        succeeded = len(assets) - download_failures
        status = "SUCCEEDED"
        if download_failures == len(assets):
            status = "FAILED"
        elif download_failures:
            status = "PARTIAL"
        summary = {
            "assets": len(assets),
            "downloadedAssets": succeeded,
            "downloadFailures": download_failures,
            "mediaStorage": "MINIO",
            "mediaStorageSkipped": False,
            "downloadMode": "ANONYMOUS_HTTP",
        }
        await self._stop_heartbeat(heartbeat_stop, heartbeat_task)
        await self.client.complete(
            job_id,
            self.worker_id,
            attempt_no,
            status=status,
            result_summary=summary,
            error_code="REMOTE_DOWNLOAD_FAILED" if download_failures else None,
            error_message=(
                f"{download_failures} of {len(assets)} remote assets failed to download"
                if download_failures
                else None
            ),
        )
        utils.logger.info(
            f"[RuoyiMediaWorker] anonymous remote download completed: {job_id}, summary={summary}"
        )

    async def _heartbeat_loop(
        self,
        job_id: str,
        attempt_no: int,
        stop_event: asyncio.Event,
    ) -> None:
        interval = max(10, self.lease_seconds // 3)
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval)
                return
            except asyncio.TimeoutError:
                await self.client.heartbeat(
                    job_id,
                    self.worker_id,
                    attempt_no,
                    stage="CRAWLING",
                    lease_seconds=self.lease_seconds,
                )

    @staticmethod
    async def _stop_heartbeat(
        stop_event: asyncio.Event,
        heartbeat_task: asyncio.Task[None],
    ) -> None:
        stop_event.set()
        if heartbeat_task.done():
            with suppress(asyncio.CancelledError, Exception):
                await heartbeat_task
            return
        try:
            await heartbeat_task
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            utils.logger.warning(
                f"[RuoyiMediaWorker] heartbeat stopped with an error before completion: {exc}"
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
        config.CRAWLER_MIN_SLEEP_SEC = max(0, config.CRAWLER_MIN_SLEEP_SEC)
        config.CRAWLER_MAX_SLEEP_SEC = max(config.CRAWLER_MIN_SLEEP_SEC, config.CRAWLER_MAX_SLEEP_SEC)
        config.DY_EXCLUDED_ID_LIST = [
            str(post_id)
            for post_id in payload.get("excludedPostIds") or []
            if post_id
        ]

        comment_policy = payload.get("commentPolicy") or {}
        scan_limit = int(comment_policy.get("sampleLimit") or 1000)
        config.CRAWLER_MAX_COMMENTS_COUNT_SINGLENOTES = max(1, min(1000, scan_limit))
        config.ENABLE_GET_SUB_COMMENTS = bool(comment_policy.get("fetchHotReplies", True))
        config.ENABLE_GET_HOT_SUB_COMMENTS_ONLY = True
        config.HOT_COMMENT_TOP_N = max(1, int(comment_policy.get("hotTopN") or 20))
        config.HOT_COMMENT_LIKE_THRESHOLD = max(0, int(comment_policy.get("likeThreshold") or 50))

        if job_type.startswith("CREATOR_"):
            creator_source = payload.get("profileUrl") or payload.get("platformCreatorId")
            if not creator_source:
                raise ValueError("Creator task is missing profileUrl/platformCreatorId")
            config.CRAWLER_TYPE = "creator"
            config.DY_CREATOR_ID_LIST = [creator_source]
            config.ENABLE_GET_COMMENTS = bool(payload.get("fetchComments", False))
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
    parser.add_argument("--base-url", default=os.getenv("RUOYI_MEDIA_BASE_URL", "http://10.200.77.58:43300"))
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

    await run_worker(
        once=args.once,
        base_url=args.base_url,
        token=args.token,
        worker_id=args.worker_id,
        lease_seconds=args.lease_seconds,
        poll_seconds=args.poll_seconds,
    )


async def run_worker(
    *,
    once: bool = False,
    base_url: str | None = None,
    token: str | None = None,
    worker_id: str | None = None,
    lease_seconds: int | None = None,
    poll_seconds: float | None = None,
) -> None:
    """Run the RuoYi worker standalone or inside the WebUI API lifespan."""
    resolved_base_url = base_url if base_url is not None else os.getenv(
        "RUOYI_MEDIA_BASE_URL", "http://10.200.77.58:43300"
    )
    resolved_token = token if token is not None else os.getenv(
        "RUOYI_MEDIA_WORKER_TOKEN", "ruoyi-media-dev-token"
    )
    resolved_worker_id = worker_id or os.getenv(
        "RUOYI_MEDIA_WORKER_ID", f"{socket.gethostname()}-{os.getpid()}"
    )
    resolved_lease_seconds = lease_seconds if lease_seconds is not None else int(
        os.getenv("RUOYI_MEDIA_LEASE_SECONDS", "120")
    )
    resolved_poll_seconds = poll_seconds if poll_seconds is not None else float(
        os.getenv("RUOYI_MEDIA_POLL_SECONDS", "5")
    )

    storage_settings = MinioStorageSettings.from_env()
    storage = MinioAssetStorage(storage_settings) if storage_settings else None
    async with RuoyiMediaClient(resolved_base_url, resolved_token) as client:
        worker = RuoyiMediaWorker(
            client,
            resolved_worker_id,
            lease_seconds=resolved_lease_seconds,
            poll_seconds=resolved_poll_seconds,
            storage=storage,
        )
        try:
            await worker.start()
            if once:
                handled = await worker.run_once()
                if not handled:
                    utils.logger.info("[RuoyiMediaWorker] no pending job")
            else:
                await worker.run_forever()
        finally:
            await worker.close()


if __name__ == "__main__":
    asyncio.run(main())
