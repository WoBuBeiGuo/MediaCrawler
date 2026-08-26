# -*- coding: utf-8 -*-
# Copyright (c) 2025 relakkes@gmail.com
#
# This file is part of MediaCrawler project.
# Repository: https://github.com/NanmiCoder/MediaCrawler/blob/main/api/main.py
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

"""
MediaCrawler WebUI API Server
Start command: uvicorn api.main:app --port 8080 --reload
Or: python -m api.main
"""
import asyncio
import os
import sys
import subprocess
from contextlib import asynccontextmanager, suppress
from pathlib import Path
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from integration.ruoyi_media.worker import run_worker
from tools import utils

from .routers import crawler_router, data_router, websocket_router

# Project root directory (used for running main.py subprocesses)
PROJECT_ROOT = Path(__file__).parent.parent


def _embedded_worker_enabled() -> bool:
    value = os.getenv("RUOYI_MEDIA_EMBEDDED_WORKER", "true").strip().lower()
    return value not in {"0", "false", "no", "off"}


def _log_worker_completion(task: asyncio.Task[None]) -> None:
    if task.cancelled():
        return
    error = task.exception()
    if error is not None:
        utils.logger.error(f"[MediaCrawlerService] Embedded Worker stopped unexpectedly: {error}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Run the Java ingestion Worker and WebUI API as one Python service."""
    worker_task: asyncio.Task[None] | None = None
    if _embedded_worker_enabled():
        utils.logger.info("[MediaCrawlerService] Starting embedded RuoYi Media Worker")
        worker_task = asyncio.create_task(run_worker(), name="ruoyi-media-worker")
        worker_task.add_done_callback(_log_worker_completion)
    else:
        utils.logger.info("[MediaCrawlerService] Embedded Worker is disabled")
    app.state.media_worker_task = worker_task

    try:
        yield
    finally:
        if worker_task is not None:
            utils.logger.info("[MediaCrawlerService] Stopping embedded Worker and Chrome")
            worker_task.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await worker_task


app = FastAPI(
    title="RuoYi MediaCrawler Service",
    description="MediaCrawler WebUI API with embedded RuoYi ingestion Worker",
    version="1.1.0",
    lifespan=lifespan,
)

# Get webui static files directory
WEBUI_DIR = os.path.join(os.path.dirname(__file__), "webui")

# CORS configuration - allow frontend dev server access
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",  # Vite dev server
        "http://localhost:3000",  # Backup port
        "http://127.0.0.1:5173",
        "http://127.0.0.1:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register routers
app.include_router(crawler_router, prefix="/api")
app.include_router(data_router, prefix="/api")
app.include_router(websocket_router, prefix="/api")


@app.get("/")
async def serve_frontend():
    """Return frontend page"""
    index_path = os.path.join(WEBUI_DIR, "index.html")
    if os.path.exists(index_path):
        return FileResponse(index_path)
    return {
        "message": "MediaCrawler WebUI API",
        "version": "1.1.0",
        "docs": "/docs",
        "note": "WebUI not found, please build it first: cd webui && npm run build"
    }


@app.get("/api/health")
async def health_check():
    worker_task: asyncio.Task[None] | None = getattr(app.state, "media_worker_task", None)
    if worker_task is None:
        worker_status = "disabled"
        worker_error = None
    elif not worker_task.done():
        worker_status = "running"
        worker_error = None
    elif worker_task.cancelled():
        worker_status = "stopped"
        worker_error = None
    else:
        worker_status = "failed"
        error = worker_task.exception()
        worker_error = str(error)[:500] if error else None
    return {
        "status": "ok" if worker_status in {"running", "disabled"} else "degraded",
        "workerStatus": worker_status,
        "workerError": worker_error,
    }


@app.get("/api/env/check")
async def check_environment():
    """Check if MediaCrawler environment is configured correctly"""
    try:
        # Use the same Python interpreter as the WebUI API process. This keeps the
        # native WebUI usable with the project's .venv and avoids a global uv dependency.
        if sys.platform == "win32":
            loop = asyncio.get_running_loop()
            process = await loop.run_in_executor(
                None,
                lambda: subprocess.run(
                    [sys.executable, "main.py", "--help"],
                    capture_output=True,
                    timeout=30.0,
                    cwd=str(PROJECT_ROOT)
                )
            )
            stdout, stderr = process.stdout, process.stderr  # bytes
        else:
            process = await asyncio.create_subprocess_exec(
                sys.executable, "main.py", "--help",
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=str(PROJECT_ROOT)  # Project root directory
            )
            stdout, stderr = await asyncio.wait_for(
                process.communicate(),
                timeout=30.0  # 30 seconds timeout
            )
        if process.returncode == 0:
            return {
                "success": True,
                "message": "MediaCrawler environment configured correctly",
                "output": stdout.decode("utf-8", errors="ignore")[:500]  # Truncate to first 500 characters
            }
        else:
            error_msg = stderr.decode("utf-8", errors="ignore") or stdout.decode("utf-8", errors="ignore")
            return {
                "success": False,
                "message": "Environment check failed",
                "error": error_msg[:500]
            }
    except asyncio.TimeoutError:
        return {
            "success": False,
            "message": "Environment check timeout",
            "error": "Command execution exceeded 30 seconds"
        }
    except FileNotFoundError:
        return {
            "success": False,
            "message": "Python command not found",
            "error": "Please start the WebUI API with the MediaCrawler virtual environment"
        }
    except Exception as e:
        return {
            "success": False,
            "message": "Environment check error",
            "error": f"{type(e).__name__}: {str(e) or 'Unknown'}"
        }


@app.get("/api/config/platforms")
async def get_platforms():
    """Get list of supported platforms"""
    return {
        "platforms": [
            {"value": "xhs", "label": "Xiaohongshu", "icon": "book-open"},
            {"value": "dy", "label": "Douyin", "icon": "music"},
            {"value": "ks", "label": "Kuaishou", "icon": "video"},
            {"value": "bili", "label": "Bilibili", "icon": "tv"},
            {"value": "wb", "label": "Weibo", "icon": "message-circle"},
            {"value": "tieba", "label": "Baidu Tieba", "icon": "messages-square"},
            {"value": "zhihu", "label": "Zhihu", "icon": "help-circle"},
        ]
    }


@app.get("/api/config/options")
async def get_config_options():
    """Get all configuration options"""
    return {
        "login_types": [
            {"value": "qrcode", "label": "QR Code Login"},
            {"value": "cookie", "label": "Cookie Login"},
        ],
        "crawler_types": [
            {"value": "search", "label": "Search Mode"},
            {"value": "detail", "label": "Detail Mode"},
            {"value": "creator", "label": "Creator Mode"},
        ],
        "save_options": [
            {"value": "jsonl", "label": "JSONL File"},
            {"value": "json", "label": "JSON File"},
            {"value": "csv", "label": "CSV File"},
            {"value": "excel", "label": "Excel File"},
            {"value": "sqlite", "label": "SQLite Database"},
            {"value": "db", "label": "MySQL Database"},
            {"value": "mongodb", "label": "MongoDB Database"},
        ],
    }


# Mount static resources - must be placed after all routes
if os.path.exists(WEBUI_DIR):
    assets_dir = os.path.join(WEBUI_DIR, "assets")
    if os.path.exists(assets_dir):
        app.mount("/assets", StaticFiles(directory=assets_dir), name="assets")
    # Mount logos directory
    logos_dir = os.path.join(WEBUI_DIR, "logos")
    if os.path.exists(logos_dir):
        app.mount("/logos", StaticFiles(directory=logos_dir), name="logos")
    # Mount other static files (e.g., vite.svg)
    app.mount("/static", StaticFiles(directory=WEBUI_DIR), name="webui-static")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
