#!/usr/bin/env python3
import asyncio
import logging
import os
import socket
import threading
import time
from urllib.parse import urlparse

import websocket

try:
    import websockets
except Exception:
    websockets = None

try:
    from playwright.async_api import async_playwright
except Exception:
    async_playwright = None


class WSBridgeService:
    def __init__(
        self,
        listen_host: str,
        listen_port: int,
        upstream_base: str,
        proxy_url: str = "",
        nav_timeout_ms: int = 90000,
        skip_page_nav: bool = False,
    ):
        self.listen_host = listen_host
        self.listen_port = listen_port
        self.upstream_base = upstream_base.rstrip("/")
        self.proxy_url = proxy_url or ""
        self.nav_timeout_ms = int(nav_timeout_ms or 90000)
        self.skip_page_nav = bool(skip_page_nav)
        self._server = None
        self._loop = None
        self._thread = None
        self._started = False
        self._cookie_header = ""
        self._cookie_ts = 0.0
        self._has_cf_clearance = False
        self.log = logging.getLogger("WSBridge")

    def start(self):
        if self._started:
            return
        if websockets is None:
            self.log.error("websockets package missing; bridge cannot start")
            return
        self._started = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True, name="ws-bridge")
        self._thread.start()
        # TCP readiness check only (avoid websocket invalid-upgrade probes).
        if not _wait_tcp_port(self.listen_host, self.listen_port, timeout_sec=5.0):
            self.log.error("bridge readiness check failed on %s:%s", self.listen_host, self.listen_port)
        self.log.info("Playwright WS bridge started: ws://%s:%s", self.listen_host, self.listen_port)

    def _run_loop(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._start_server())
        self._loop.run_forever()

    async def _start_server(self):
        self._server = await websockets.serve(self._handle_client, self.listen_host, self.listen_port, max_size=None)

    async def _ensure_cookie(self) -> str:
        # Refresh every 10 minutes or when empty.
        if self._cookie_header and (time.time() - self._cookie_ts) < 600:
            return self._cookie_header
        if async_playwright is None:
            return self._cookie_header
        nav_ok = False
        has_cf_clearance = False
        try:
            proxy_cfg = _proxy_for_playwright(self.proxy_url)
            launch_kwargs = {"headless": True}
            if os.name != "nt":
                launch_kwargs["args"] = ["--no-sandbox"]
            async with async_playwright() as p:
                browser = await p.chromium.launch(**launch_kwargs)
                context_kwargs = {}
                if proxy_cfg:
                    context_kwargs["proxy"] = proxy_cfg
                    self.log.info("Playwright context proxy: %s", proxy_cfg.get("server", ""))
                context = await browser.new_context(**context_kwargs)
                page = await context.new_page()
                if not self.skip_page_nav:
                    try:
                        nav_timeout = max(self.nav_timeout_ms, 120000)
                        await page.goto(
                            "https://qxbroker.com/en/sign-in",
                            wait_until="domcontentloaded",
                            timeout=nav_timeout,
                        )
                        try:
                            await page.wait_for_load_state("networkidle", timeout=30000)
                        except Exception:
                            # networkidle قد لا يتحقق دائمًا؛ لا نعتبرها فشلًا قاطعًا.
                            pass
                        nav_ok = True
                    except Exception as e:
                        self.log.warning("page navigation failed: %s", e)
                        await page.goto("about:blank", wait_until="commit", timeout=30000)
                        self.log.info("page navigation/fallback: about:blank")
                else:
                    await page.goto("about:blank", wait_until="commit", timeout=30000)
                    self.log.info("page navigation/fallback: skip-page-nav=1 -> about:blank")
                cookies = await context.cookies(["https://qxbroker.com", "https://ws2.qxbroker.com"])
                ck = []
                for c in cookies:
                    n = c.get("name")
                    v = c.get("value")
                    if n and v:
                        ck.append(f"{n}={v}")
                        if n == "cf_clearance":
                            has_cf_clearance = True
                self._cookie_header = "; ".join(ck)
                self._cookie_ts = time.time()
                self._has_cf_clearance = has_cf_clearance
                await context.close()
                await browser.close()
        except Exception as e:
            self.log.warning("cookie refresh failed: %s", e)
        if nav_ok:
            self.log.info("page navigation/fallback: domcontentloaded/networkidle")
        if not self._has_cf_clearance:
            self.log.warning("Cloudflare clearance cookie missing (cf_clearance); proxy/session likely blocked")
        return self._cookie_header

    async def _handle_client(self, client_ws):
        path = getattr(getattr(client_ws, "request", None), "path", "/")
        if not isinstance(path, str):
            path = "/"
        upstream_url = f"{self.upstream_base}{path}"
        upstream = None
        try:
            cookie = await self._ensure_cookie()
            if not self._has_cf_clearance:
                raise RuntimeError("Cloudflare challenge not solved (cf_clearance missing)")
            headers = [
                "Origin: https://qxbroker.com",
                "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0 Safari/537.36",
            ]
            if cookie:
                headers.append(f"Cookie: {cookie}")
            p = _parse_proxy(self.proxy_url)
            upstream = await asyncio.to_thread(
                websocket.create_connection,
                upstream_url,
                header=headers,
                enable_multithread=True,
                timeout=45,
                http_proxy_host=p.get("host"),
                http_proxy_port=p.get("port"),
                proxy_type=p.get("type"),
                http_proxy_auth=p.get("auth"),
            )
            self.log.info("upstream Quotex WebSocket OPEN %s", upstream_url)

            async def c2u():
                async for message in client_ws:
                    await asyncio.to_thread(upstream.send, message)

            async def u2c():
                while True:
                    msg = await asyncio.to_thread(upstream.recv)
                    if msg is None:
                        break
                    await client_ws.send(msg)

            done, pending = await asyncio.wait(
                [asyncio.create_task(c2u()), asyncio.create_task(u2c())],
                return_when=asyncio.FIRST_EXCEPTION,
            )
            for t in pending:
                t.cancel()
            for t in done:
                exc = t.exception()
                if exc:
                    raise exc
        except Exception as e:
            self.log.error("ERROR %s", e)
        finally:
            try:
                if upstream:
                    upstream.close()
            except Exception:
                pass
            try:
                await client_ws.close()
            except Exception:
                pass
            self.log.info("upstream Quotex WebSocket CLOSE")


def _parse_proxy(proxy_url: str) -> dict:
    if not proxy_url:
        return {}
    try:
        u = urlparse(proxy_url.strip())
        if not u.hostname or not u.port:
            return {}
        ptype = "http"
        if (u.scheme or "").lower().startswith("socks5"):
            ptype = "socks5"
        auth = None
        if u.username and u.password:
            auth = (u.username, u.password)
        return {"host": u.hostname, "port": int(u.port), "type": ptype, "auth": auth}
    except Exception:
        return {}


def _proxy_for_playwright(proxy_url: str):
    if not proxy_url:
        return None
    try:
        u = urlparse(proxy_url.strip())
        if not u.hostname or not u.port:
            return None
        server = f"{u.scheme or 'http'}://{u.hostname}:{u.port}"
        cfg = {"server": server}
        if u.username:
            cfg["username"] = u.username
        if u.password:
            cfg["password"] = u.password
        return cfg
    except Exception:
        return None


def _wait_tcp_port(host: str, port: int, timeout_sec: float = 5.0) -> bool:
    end = time.time() + timeout_sec
    while time.time() < end:
        try:
            with socket.create_connection((host, int(port)), timeout=0.8):
                return True
        except Exception:
            time.sleep(0.2)
    return False
