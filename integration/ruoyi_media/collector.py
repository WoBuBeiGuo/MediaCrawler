# -*- coding: utf-8 -*-
# Copyright (c) 2025 relakkes@gmail.com
#
# This file is part of MediaCrawler project.
# Repository: https://github.com/NanmiCoder/MediaCrawler/blob/main/integration\ruoyi_media\collector.py
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

from contextvars import ContextVar, Token
from datetime import datetime
from typing import Any


_active_collector: ContextVar["DouyinJobCollector | None"] = ContextVar(
    "ruoyi_media_douyin_collector",
    default=None,
)


def get_active_collector() -> "DouyinJobCollector | None":
    return _active_collector.get()


class DouyinJobCollector:
    """In-memory collection buffer for one Java crawl job."""

    def __init__(self, request_payload: dict[str, Any]) -> None:
        self.request_payload = request_payload
        self.creators: dict[str, dict[str, Any]] = {}
        self.awemes: dict[str, dict[str, Any]] = {}
        self.comments: dict[str, dict[str, dict[str, Any]]] = {}
        self._context_token: Token[DouyinJobCollector | None] | None = None

    def activate(self) -> None:
        if self._context_token is not None:
            raise RuntimeError("Collector is already active")
        self._context_token = _active_collector.set(self)

    def deactivate(self) -> None:
        if self._context_token is not None:
            _active_collector.reset(self._context_token)
            self._context_token = None

    def capture_creator(self, user_id: str, creator: dict[str, Any]) -> None:
        user = creator.get("user") or creator
        stable_id = str(user.get("sec_uid") or user_id or "")
        if stable_id:
            self.creators[stable_id] = user

    def capture_aweme(self, aweme: dict[str, Any]) -> None:
        aweme_id = str(aweme.get("aweme_id") or "")
        if not aweme_id:
            return
        self.awemes[aweme_id] = aweme
        author = aweme.get("author") or {}
        stable_id = str(author.get("sec_uid") or "")
        if stable_id and stable_id not in self.creators:
            self.creators[stable_id] = author

    def capture_comment(self, aweme_id: str, comment: dict[str, Any]) -> None:
        comment_id = str(comment.get("cid") or "")
        if not aweme_id or not comment_id:
            return
        self.comments.setdefault(str(aweme_id), {})[comment_id] = comment

    def build_ingest_payload(self) -> dict[str, Any]:
        creator = self._build_creator()
        posts = [self._build_post(item) for item in self.awemes.values()]
        comments: list[dict[str, Any]] = []
        assets: list[dict[str, Any]] = []
        for aweme_id, aweme in self.awemes.items():
            comments.extend(self._build_hot_comments(aweme_id, aweme))
            assets.extend(self._build_assets(aweme))
        return {
            "creator": creator,
            "posts": posts,
            "comments": comments,
            "assets": assets,
            "rawPayloads": [],
        }

    def _build_creator(self) -> dict[str, Any] | None:
        if not self.creators:
            return None
        preferred_id = str(self.request_payload.get("platformCreatorId") or "")
        user = self.creators.get(preferred_id) or next(iter(self.creators.values()))
        stable_id = str(user.get("sec_uid") or preferred_id)
        if not stable_id:
            return None
        unique_account = user.get("unique_id") or user.get("short_id")
        verification = user.get("enterprise_verify_reason") or user.get("custom_verify")
        return {
            "platform": "DOUYIN",
            "platformCreatorId": stable_id,
            "platformUid": _string_or_none(user.get("uid")),
            "uniqueAccount": _string_or_none(unique_account),
            "nickname": user.get("nickname"),
            "avatarUrl": _first_url(user.get("avatar_larger") or user.get("avatar_medium") or user.get("avatar_thumb")),
            "signature": user.get("signature"),
            "profileUrl": self.request_payload.get("profileUrl") or f"https://www.douyin.com/user/{stable_id}",
            "verification": verification,
            "followerCount": _integer(user.get("follower_count")),
            "followingCount": _integer(user.get("following_count")),
            "postCount": _integer(user.get("aweme_count")),
            "totalFavorited": _integer(user.get("total_favorited")),
            "collectedAt": _now_text(),
        }

    def _build_post(self, aweme: dict[str, Any]) -> dict[str, Any]:
        aweme_id = str(aweme.get("aweme_id"))
        video = aweme.get("video") or {}
        statistics = aweme.get("statistics") or {}
        images = aweme.get("images") or []
        caption = aweme.get("desc") or ""
        return {
            "platform": "DOUYIN",
            "platformPostId": aweme_id,
            "postType": "IMAGE" if images else "VIDEO",
            "title": caption[:500],
            "caption": caption,
            "canonicalUrl": f"https://www.douyin.com/video/{aweme_id}",
            "sourceUrl": self.request_payload.get("sourceUrl"),
            "sourceShareText": self.request_payload.get("shareText"),
            "remoteVideoUrl": _video_url(video),
            "remoteCoverUrl": _cover_url(video),
            "durationMs": _integer(video.get("duration")),
            "width": _integer(video.get("width")),
            "height": _integer(video.get("height")),
            "publishedAt": _timestamp_text(aweme.get("create_time")),
            "likeCount": _integer(statistics.get("digg_count")),
            "commentCount": _integer(statistics.get("comment_count")),
            "collectCount": _integer(statistics.get("collect_count")),
            "shareCount": _integer(statistics.get("share_count")),
            "mediaStatus": "PENDING",
            "commentStatus": "SUCCEEDED" if self.comments.get(aweme_id) is not None else "PENDING",
            "visibilityStatus": "VISIBLE",
            "collectedAt": _now_text(),
        }

    def _build_hot_comments(self, aweme_id: str, aweme: dict[str, Any]) -> list[dict[str, Any]]:
        source = list(self.comments.get(aweme_id, {}).values())
        if not source:
            return []
        policy = self.request_payload.get("commentPolicy") or {}
        top_n = max(1, int(policy.get("hotTopN") or 20))
        like_threshold = max(0, int(policy.get("likeThreshold") or 50))
        include_replies = bool(policy.get("fetchHotReplies", True))
        ranked = sorted(source, key=lambda item: _integer(item.get("digg_count")) or 0, reverse=True)
        hot_ids = {
            str(item.get("cid"))
            for rank, item in enumerate(ranked, start=1)
            if rank <= top_n or (_integer(item.get("digg_count")) or 0) >= like_threshold or _is_pinned(item)
        }
        if include_replies:
            for item in source:
                parent_id = _parent_comment_id(item)
                if parent_id in hot_ids:
                    hot_ids.add(str(item.get("cid")))

        author = aweme.get("author") or {}
        author_ids = {str(author.get(key)) for key in ("uid", "sec_uid") if author.get(key)}
        result: list[dict[str, Any]] = []
        for rank, item in enumerate(ranked, start=1):
            comment_id = str(item.get("cid") or "")
            if comment_id not in hot_ids:
                continue
            user = item.get("user") or {}
            user_id = user.get("sec_uid") or user.get("uid")
            like_count = _integer(item.get("digg_count")) or 0
            result.append(
                {
                    "platform": "DOUYIN",
                    "platformPostId": aweme_id,
                    "platformCommentId": comment_id,
                    "platformParentCommentId": _parent_comment_id(item) or None,
                    "commenterId": _string_or_none(user_id),
                    "commenterNickname": user.get("nickname"),
                    "commenterAvatarUrl": _first_url(user.get("avatar_medium") or user.get("avatar_thumb")),
                    "content": item.get("text") or "",
                    "likeCount": like_count,
                    "replyCount": _integer(item.get("reply_comment_total")) or 0,
                    "publishedAt": _timestamp_text(item.get("create_time")),
                    "isCreator": str(user_id) in author_ids if user_id else False,
                    "isPinned": _is_pinned(item),
                    "isHot": True,
                    "hotReason": "PINNED" if _is_pinned(item) else ("LIKE_THRESHOLD" if like_count >= like_threshold else "TOP_LIKED"),
                    "sampleRank": rank,
                    "imageUrls": _comment_image_urls(item),
                }
            )
        return result

    def _build_assets(self, aweme: dict[str, Any]) -> list[dict[str, Any]]:
        aweme_id = str(aweme.get("aweme_id"))
        video = aweme.get("video") or {}
        assets: list[dict[str, Any]] = []
        video_url = _video_url(video)
        if video_url:
            assets.append(
                {
                    "platformPostId": aweme_id,
                    "assetType": "VIDEO",
                    "sequenceNo": 0,
                    "sourceUrl": video_url,
                    "storageProvider": "REMOTE",
                    "durationMs": _integer(video.get("duration")),
                    "width": _integer(video.get("width")),
                    "height": _integer(video.get("height")),
                    "downloadStatus": "PENDING",
                }
            )
        cover_url = _cover_url(video)
        if cover_url:
            assets.append(
                {
                    "platformPostId": aweme_id,
                    "assetType": "COVER",
                    "sequenceNo": 0,
                    "sourceUrl": cover_url,
                    "storageProvider": "REMOTE",
                    "downloadStatus": "PENDING",
                }
            )
        for sequence_no, image in enumerate(aweme.get("images") or []):
            image_url = _first_url(image)
            if image_url:
                assets.append(
                    {
                        "platformPostId": aweme_id,
                        "assetType": "IMAGE",
                        "sequenceNo": sequence_no,
                        "sourceUrl": image_url,
                        "storageProvider": "REMOTE",
                        "downloadStatus": "PENDING",
                    }
                )
        return assets


def _first_url(value: Any) -> str | None:
    if not isinstance(value, dict):
        return None
    urls = value.get("url_list") or value.get("download_url_list") or []
    return str(urls[-1]) if urls else None


def _video_url(video: dict[str, Any]) -> str | None:
    for key in ("play_addr_h264", "play_addr_256", "play_addr"):
        value = _first_url(video.get(key))
        if value:
            return value
    return None


def _cover_url(video: dict[str, Any]) -> str | None:
    return _first_url(video.get("raw_cover") or video.get("origin_cover") or video.get("cover"))


def _comment_image_urls(comment: dict[str, Any]) -> list[str]:
    result: list[str] = []
    for image in comment.get("image_list") or []:
        url = _first_url(image.get("origin_url") or image)
        if url:
            result.append(url)
    return result


def _parent_comment_id(comment: dict[str, Any]) -> str:
    value = comment.get("reply_id") or comment.get("reply_to_reply_id") or ""
    return "" if str(value) == "0" else str(value)


def _is_pinned(comment: dict[str, Any]) -> bool:
    return bool(comment.get("is_pinned") or comment.get("stick_position") not in (None, 0, "0"))


def _integer(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _string_or_none(value: Any) -> str | None:
    return None if value in (None, "") else str(value)


def _timestamp_text(value: Any) -> str | None:
    timestamp = _integer(value)
    if timestamp is None:
        return None
    return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
