# -*- coding: utf-8 -*-
# Copyright (c) 2025 relakkes@gmail.com
#
# This file is part of MediaCrawler project.
# Repository: https://github.com/NanmiCoder/MediaCrawler
# Licensed under NON-COMMERCIAL LEARNING LICENSE 1.1

from __future__ import annotations

from typing import Any

import pytest

import config
from media_platform.douyin.core import DouYinCrawler


@pytest.mark.asyncio
async def test_creator_detail_skips_excluded_posts(monkeypatch: Any) -> None:
    monkeypatch.setattr(config, "DY_EXCLUDED_ID_LIST", ["member-only"])
    crawler = DouYinCrawler()
    fetched: list[str] = []

    async def fake_get_aweme_detail(aweme_id: str, _semaphore: Any) -> None:
        fetched.append(aweme_id)
        return None

    crawler.get_aweme_detail = fake_get_aweme_detail  # type: ignore[method-assign]
    await crawler.fetch_creator_video_detail(
        [
            {"aweme_id": "member-only"},
            {"aweme_id": "public-video"},
        ]
    )

    assert fetched == ["public-video"]
