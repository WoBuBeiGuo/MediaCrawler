# -*- coding: utf-8 -*-
from unittest.mock import AsyncMock

import pytest

from media_platform.douyin.client import DouYinClient


def _comment(comment_id: str, likes: int, replies: int = 0) -> dict:
    return {
        "cid": comment_id,
        "digg_count": likes,
        "reply_comment_total": replies,
        "reply_id": "0",
    }


@pytest.mark.asyncio
async def test_comment_scan_limits_first_level_then_fetches_only_selected_replies() -> None:
    client = object.__new__(DouYinClient)
    first_page = [_comment("hot", 100, 1), _comment("cold-1", 2, 5)]
    second_page = [_comment("cold-2", 1, 3), _comment("outside-limit", 0, 4)]
    client.get_aweme_comments = AsyncMock(
        side_effect=[
            {"has_more": 1, "cursor": 20, "comments": first_page},
            {"has_more": 1, "cursor": 40, "comments": second_page},
        ]
    )
    reply = {"cid": "reply-hot", "reply_id": "hot", "digg_count": 0}
    client.get_sub_comments = AsyncMock(
        return_value={"has_more": 0, "cursor": 0, "comments": [reply]}
    )
    captured: list[dict] = []

    async def capture(_aweme_id: str, comments: list[dict]) -> None:
        captured.extend(comments)

    selector_inputs: list[list[dict]] = []

    def select_hot(comments: list[dict]) -> list[dict]:
        selector_inputs.append(comments)
        return [comments[0]]

    result = await client.get_aweme_all_comments(
        "aweme-1",
        is_fetch_sub_comments=True,
        callback=capture,
        max_count=3,
        sub_comments_selector=select_hot,
    )

    assert [item["cid"] for item in selector_inputs[0]] == ["hot", "cold-1", "cold-2"]
    assert [item["cid"] for item in result] == ["hot", "cold-1", "cold-2", "reply-hot"]
    assert [item["cid"] for item in captured] == ["hot", "cold-1", "cold-2", "reply-hot"]
    client.get_sub_comments.assert_awaited_once_with("aweme-1", "hot", 0)
