# -*- coding: utf-8 -*-
# Copyright (c) 2025 relakkes@gmail.com
#
# This file is part of MediaCrawler project.
# Repository: https://github.com/NanmiCoder/MediaCrawler/blob/main/integration\ruoyi_media\client.py
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

from typing import Any

import httpx


class RuoyiMediaClientError(RuntimeError):
    """Raised when the Java media service rejects a worker request."""


class RuoyiMediaClientTransportError(RuoyiMediaClientError):
    """Raised when the Java media service is temporarily unreachable."""


class RuoyiMediaClientRejectedError(RuoyiMediaClientError):
    """Raised when Java received and rejected a Worker request."""


class RuoyiMediaClient:
    def __init__(
        self,
        base_url: str,
        token: str,
        *,
        timeout_seconds: float = 60.0,
    ) -> None:
        if not base_url.strip():
            raise ValueError("RuoYi Media base URL is required")
        if not token.strip():
            raise ValueError("RuoYi Media worker token is required")
        self._client = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            headers={"X-Media-Worker-Token": token},
            timeout=timeout_seconds,
        )

    async def __aenter__(self) -> "RuoyiMediaClient":
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.close()

    async def close(self) -> None:
        await self._client.aclose()

    async def claim(
        self,
        worker_id: str,
        supported_job_types: list[str],
        lease_seconds: int,
    ) -> dict[str, Any] | None:
        return await self._post(
            "/internal/media/worker/jobs/claim",
            {
                "workerId": worker_id,
                "supportedJobTypes": supported_job_types,
                "leaseSeconds": lease_seconds,
            },
        )

    async def heartbeat(
        self,
        job_id: str,
        worker_id: str,
        attempt_no: int,
        *,
        progress: int | None = None,
        stage: str | None = None,
        lease_seconds: int | None = None,
    ) -> None:
        payload: dict[str, Any] = {
            "workerId": worker_id,
            "attemptNo": attempt_no,
        }
        if progress is not None:
            payload["progress"] = progress
        if stage:
            payload["stage"] = stage
        if lease_seconds is not None:
            payload["leaseSeconds"] = lease_seconds
        await self._post(f"/internal/media/worker/jobs/{job_id}/heartbeat", payload)

    async def ingest(
        self,
        job_id: str,
        worker_id: str,
        attempt_no: int,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        body = {
            "workerId": worker_id,
            "attemptNo": attempt_no,
            **payload,
        }
        result = await self._post(f"/internal/media/worker/jobs/{job_id}/ingest", body)
        return result or {}

    async def complete(
        self,
        job_id: str,
        worker_id: str,
        attempt_no: int,
        *,
        status: str,
        result_summary: dict[str, Any] | None = None,
        error_code: str | None = None,
        error_message: str | None = None,
        log_object_key: str | None = None,
    ) -> None:
        await self._post(
            f"/internal/media/worker/jobs/{job_id}/complete",
            {
                "workerId": worker_id,
                "attemptNo": attempt_no,
                "status": status,
                "resultSummary": result_summary or {},
                "errorCode": error_code,
                "errorMessage": error_message,
                "logObjectKey": log_object_key,
            },
        )

    async def _post(self, path: str, payload: dict[str, Any]) -> Any:
        try:
            response = await self._client.post(path, json=payload)
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            if 400 <= exc.response.status_code < 500:
                raise RuoyiMediaClientRejectedError(
                    f"Worker API rejected HTTP request: {path}: {exc.response.status_code}"
                ) from exc
            raise RuoyiMediaClientTransportError(
                f"Worker API request failed: {path}: {exc}"
            ) from exc
        except httpx.HTTPError as exc:
            raise RuoyiMediaClientTransportError(
                f"Worker API request failed: {path}: {exc}"
            ) from exc
        try:
            body = response.json()
        except ValueError as exc:
            raise RuoyiMediaClientTransportError(
                f"Worker API returned non-JSON response: {path}"
            ) from exc
        if body.get("code") != 200:
            raise RuoyiMediaClientRejectedError(
                f"Worker API rejected request: {path}: {body.get('msg')}"
            )
        return body.get("data")
