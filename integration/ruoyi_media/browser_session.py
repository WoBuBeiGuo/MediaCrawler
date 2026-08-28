# -*- coding: utf-8 -*-
# Copyright (c) 2025 relakkes@gmail.com
#
# This file is part of MediaCrawler project.
# Repository: https://github.com/NanmiCoder/MediaCrawler
# Licensed under NON-COMMERCIAL LEARNING LICENSE 1.1

from __future__ import annotations

import asyncio
from typing import Optional

from playwright.async_api import BrowserContext, Playwright, async_playwright

import config
from tools import utils
from tools.cdp_browser import CDPBrowserManager


class PersistentDouyinBrowser:
    """Own one CDP Chrome session for the complete Worker lifetime."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._playwright: Optional[Playwright] = None
        self._manager: Optional[CDPBrowserManager] = None
        self._context: Optional[BrowserContext] = None

    async def get_context(self) -> Optional[BrowserContext]:
        """Return a reusable context, or None when CDP mode is disabled."""
        if not config.ENABLE_CDP_MODE:
            return None

        async with self._lock:
            if self._is_usable():
                return self._context

            await self._close_unlocked()
            utils.logger.info("[PersistentDouyinBrowser] Starting dedicated Worker Chrome session")
            self._playwright = await async_playwright().start()
            self._manager = CDPBrowserManager()
            try:
                self._context = await self._manager.launch_and_connect(
                    playwright=self._playwright,
                    playwright_proxy=None,
                    user_agent=None,
                    headless=config.CDP_HEADLESS,
                )
                await self._manager.add_stealth_script()
                utils.logger.info("[PersistentDouyinBrowser] Worker Chrome session is ready")
                return self._context
            except Exception:
                await self._close_unlocked()
                raise

    async def close(self) -> None:
        """Close the persistent browser when the Worker exits."""
        async with self._lock:
            await self._close_unlocked()

    async def new_anonymous_context(self) -> BrowserContext:
        """Create an isolated context that cannot see the account login state."""
        persistent_context = await self.get_context()
        if persistent_context is None:
            raise RuntimeError("Anonymous asset refresh requires CDP mode")
        browser = persistent_context.browser
        if browser is None or not browser.is_connected():
            raise RuntimeError("Worker Chrome does not support isolated anonymous contexts")
        context = await browser.new_context()
        await context.add_init_script(path="libs/stealth.min.js")
        return context

    def _is_usable(self) -> bool:
        if self._context is None or self._manager is None or not self._manager.is_connected():
            return False
        try:
            self._context.pages
            return True
        except Exception:
            return False

    async def _close_unlocked(self) -> None:
        manager, playwright = self._manager, self._playwright
        self._manager = None
        self._context = None
        self._playwright = None

        if manager is not None:
            await manager.cleanup(force=True)
        if playwright is not None:
            await playwright.stop()
