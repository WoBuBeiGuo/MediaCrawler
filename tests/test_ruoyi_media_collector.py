# -*- coding: utf-8 -*-
# Copyright (c) 2025 relakkes@gmail.com
#
# This file is part of MediaCrawler project.
# Repository: https://github.com/NanmiCoder/MediaCrawler/blob/main/tests\test_ruoyi_media_collector.py
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

from integration.ruoyi_media.collector import DouyinJobCollector, get_active_collector


def _aweme() -> dict:
    return {
        "aweme_id": "7000000000000000001",
        "desc": "财经方法论示例",
        "create_time": 1700000000,
        "author": {
            "uid": "author-uid",
            "sec_uid": "author-sec-uid",
            "unique_id": "finance-teacher",
            "nickname": "财经老师",
            "avatar_thumb": {"url_list": ["https://example/avatar.jpg"]},
            "signature": "价值投资",
            "follower_count": 10000,
            "following_count": 10,
            "aweme_count": 1,
            "total_favorited": 50000,
        },
        "statistics": {"digg_count": 999, "comment_count": 3, "collect_count": 88, "share_count": 66},
        "video": {
            "duration": 61000,
            "width": 1080,
            "height": 1920,
            "play_addr": {"url_list": ["https://example/video.mp4"]},
            "cover": {"url_list": ["https://example/cover.jpg"]},
        },
    }


def _comment(comment_id: str, likes: int, reply_id: str = "0") -> dict:
    return {
        "aweme_id": "7000000000000000001",
        "cid": comment_id,
        "reply_id": reply_id,
        "text": f"评论-{comment_id}",
        "digg_count": likes,
        "reply_comment_total": 0,
        "create_time": 1700000001,
        "user": {"uid": f"user-{comment_id}", "nickname": f"用户-{comment_id}"},
    }


def test_collector_builds_creator_post_hot_comments_and_assets() -> None:
    collector = DouyinJobCollector(
        {
            "platformCreatorId": "author-sec-uid",
            "profileUrl": "https://www.douyin.com/user/author-sec-uid",
            "commentPolicy": {"hotTopN": 1, "likeThreshold": 50, "fetchHotReplies": True},
        }
    )
    collector.capture_aweme(_aweme())
    collector.capture_comment("7000000000000000001", _comment("hot", 100))
    collector.capture_comment("7000000000000000001", _comment("cold", 1))
    collector.capture_comment("7000000000000000001", _comment("reply", 0, "hot"))

    payload = collector.build_ingest_payload()

    assert payload["creator"]["platformCreatorId"] == "author-sec-uid"
    assert payload["creator"]["followerCount"] == 10000
    assert payload["posts"][0]["caption"] == "财经方法论示例"
    comments = {item["platformCommentId"]: item for item in payload["comments"]}
    assert set(comments) == {"hot", "cold", "reply"}
    assert comments["hot"]["isHot"] is True
    assert comments["cold"]["isHot"] is False
    assert comments["cold"]["hotReason"] is None
    assert comments["reply"]["isHot"] is True
    assert comments["reply"]["hotReason"] == "HOT_REPLY"
    assert {item["assetType"] for item in payload["assets"]} == {"VIDEO", "COVER"}


def test_collector_context_is_task_scoped() -> None:
    collector = DouyinJobCollector({})
    assert get_active_collector() is None
    collector.activate()
    assert get_active_collector() is collector
    collector.deactivate()
    assert get_active_collector() is None
