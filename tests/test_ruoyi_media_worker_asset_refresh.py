# -*- coding: utf-8 -*-
# Copyright (c) 2025 relakkes@gmail.com
#
# This file is part of MediaCrawler project.
# Repository: https://github.com/NanmiCoder/MediaCrawler
# Licensed under NON-COMMERCIAL LEARNING LICENSE 1.1

from __future__ import annotations

import asyncio
from typing import Any, cast

import config
import pytest

import media_platform.douyin.core as douyin_core
from media_platform.douyin.core import DouYinCrawler

from integration.ruoyi_media.worker import RuoyiMediaWorker, SUPPORTED_JOB_TYPES


def test_asset_refresh_job_uses_detail_mode_without_comments(monkeypatch: Any) -> None:
    for name in (
        "PLATFORM",
        "SAVE_DATA_OPTION",
        "ENABLE_GET_MEIDAS",
        "MAX_CONCURRENCY_NUM",
        "CRAWLER_TYPE",
        "DY_SPECIFIED_ID_LIST",
        "DY_EXCLUDED_ID_LIST",
        "ENABLE_GET_COMMENTS",
    ):
        monkeypatch.setattr(config, name, getattr(config, name))

    worker = RuoyiMediaWorker(cast(Any, object()), "test-worker")
    worker._configure_crawler(
        {
            "jobType": "POST_ASSET_REFRESH",
            "requestPayload": {
                "canonicalUrl": "https://www.douyin.com/video/7442341759184653577",
                "fetchComments": False,
            },
        }
    )

    assert "POST_ASSET_REFRESH" in SUPPORTED_JOB_TYPES
    assert config.CRAWLER_TYPE == "detail"
    assert config.DY_SPECIFIED_ID_LIST == ["https://www.douyin.com/video/7442341759184653577"]
    assert config.ENABLE_GET_COMMENTS is False


def test_creator_job_configures_excluded_member_posts(monkeypatch: Any) -> None:
    monkeypatch.setattr(config, "DY_EXCLUDED_ID_LIST", [])
    worker = RuoyiMediaWorker(cast(Any, object()), "test-worker")

    worker._configure_crawler(
        {
            "jobType": "CREATOR_INCREMENTAL_SYNC",
            "requestPayload": {
                "profileUrl": "https://www.douyin.com/user/example",
                "excludedPostIds": ["7442341759184653577"],
                "fetchComments": False,
            },
        }
    )

    assert config.DY_EXCLUDED_ID_LIST == ["7442341759184653577"]


class _AnonymousPage:
    async def goto(self, _url: str) -> None:
        return None


class _AnonymousBrowserContext:
    def __init__(self) -> None:
        self.page = _AnonymousPage()

    async def new_page(self) -> _AnonymousPage:
        return self.page


class _AnonymousDouyinClient:
    def __init__(self) -> None:
        self.cookie_updates = 0

    async def pong(self, *, browser_context: Any) -> bool:
        return False

    async def update_cookies(self, *, browser_context: Any, urls: list[str]) -> None:
        self.cookie_updates += 1


class _ForbiddenLogin:
    def __init__(self, *_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("anonymous refresh must not construct a login flow")


class _AnonymousDetailResponse:
    url = "https://www.douyin.com/aweme/v1/web/aweme/detail/?aweme_id=7671641873538239744"

    async def json(self) -> dict[str, Any]:
        return {
            "aweme_detail": {
                "aweme_id": "7671641873538239744",
                "video": {"play_addr": {"url_list": ["https://example.test/video.mp4"]}},
            }
        }


class _AnonymousPublicPage:
    def __init__(self) -> None:
        self._response_handler: Any = None

    def on(self, event: str, handler: Any) -> None:
        assert event == "response"
        self._response_handler = handler

    def remove_listener(self, event: str, handler: Any) -> None:
        assert event == "response"
        assert handler is self._response_handler
        self._response_handler = None

    async def goto(self, _url: str, **_kwargs: Any) -> None:
        assert self._response_handler is not None
        self._response_handler(_AnonymousDetailResponse())

    async def evaluate(self, _script: str) -> list[Any]:
        return []


class _ForbiddenDetailClient:
    async def get_video_by_id(self, _aweme_id: str) -> Any:
        raise AssertionError("anonymous refresh must not use the direct HTTP detail API")


@pytest.mark.asyncio
async def test_anonymous_detail_never_falls_back_to_login(monkeypatch: Any) -> None:
    monkeypatch.setattr(config, "CRAWLER_TYPE", "detail")
    monkeypatch.setattr(douyin_core, "DouYinLogin", _ForbiddenLogin)
    context = _AnonymousBrowserContext()
    client = _AnonymousDouyinClient()
    detail_called = False
    crawler = DouYinCrawler(cast(Any, context), allow_login=False)

    async def create_client(_proxy: str | None) -> _AnonymousDouyinClient:
        return client

    async def fetch_detail() -> None:
        nonlocal detail_called
        detail_called = True

    monkeypatch.setattr(crawler, "create_douyin_client", create_client)
    monkeypatch.setattr(crawler, "get_specified_awemes", fetch_detail)

    await crawler._run_with_browser_context(None)

    assert detail_called is True
    assert client.cookie_updates == 1
    assert crawler._allow_login is False

@pytest.mark.asyncio
async def test_anonymous_detail_uses_public_browser_page(monkeypatch: Any) -> None:
    monkeypatch.setattr(config, "CRAWLER_MIN_SLEEP_SEC", 0)
    monkeypatch.setattr(config, "CRAWLER_MAX_SLEEP_SEC", 0)
    crawler = DouYinCrawler(cast(Any, object()), allow_login=False)
    crawler.context_page = cast(Any, _AnonymousPublicPage())
    crawler.dy_client = cast(Any, _ForbiddenDetailClient())

    detail = await crawler.get_aweme_detail(
        "7671641873538239744",
        asyncio.Semaphore(1),
    )

    assert detail is not None
    assert detail["aweme_id"] == "7671641873538239744"
