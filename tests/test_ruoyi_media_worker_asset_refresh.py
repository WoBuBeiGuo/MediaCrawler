# -*- coding: utf-8 -*-
# Copyright (c) 2025 relakkes@gmail.com
#
# This file is part of MediaCrawler project.
# Repository: https://github.com/NanmiCoder/MediaCrawler
# Licensed under NON-COMMERCIAL LEARNING LICENSE 1.1

from __future__ import annotations

from typing import Any, cast

import config

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
