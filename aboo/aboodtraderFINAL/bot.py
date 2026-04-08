#!/usr/bin/env python3
"""
abood trader Bot — النسخة النهائية الكاملة
✅ صفقة 60 ثانية ثابتة بدون marginal
✅ تحليل شمعات حقيقية من أسعار Quotex الحية
✅ حساب ربح/خسارة من رصيد المنصة الفعلي
✅ إعادة تشغيل البوت بعد الإيقاف
✅ متعدد المشتركين كل بحسابه المستقل
✅ إشعار عند تحقق الهدف
"""
import asyncio, json, logging, os, queue, random, re
import secrets, threading, time, traceback
from datetime import datetime, timezone
from urllib.parse import urlparse

import pydantic, uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

try:
    from pyquotex.stable_api import Quotex
    QX = True
except ImportError:
    QX = False

try:
    from playwright.async_api import async_playwright, TimeoutError as PwTimeoutError
    PLAYWRIGHT_OK = True
except ImportError:
    PLAYWRIGHT_OK = False
    async_playwright = None
    PwTimeoutError = Exception

# يسمح بإجبار وضع المحاكاة حتى لو pyquotex مثبت (مفيد عند حظر Cloudflare 403)
FORCE_SIM_MODE = os.getenv("FORCE_SIM_MODE", "0").strip().lower() in ("1", "true", "yes", "on")
if FORCE_SIM_MODE:
    QX = False
BROWSER_QX_MODE = os.getenv("BROWSER_QX_MODE", "0").strip().lower() in ("1", "true", "yes", "on")
QUOTEX_USE_PLAYWRIGHT_BRIDGE = os.getenv("QUOTEX_USE_PLAYWRIGHT_BRIDGE", "0").strip().lower() in ("1", "true", "yes", "on")
QUOTEX_PROXY_URL = os.getenv("QUOTEX_PROXY_URL", "").strip()
QUOTEX_WS_BRIDGE_PORT = int(os.getenv("QUOTEX_WS_BRIDGE_PORT", "18765"))
QUOTEX_WS_UPSTREAM = os.getenv("QUOTEX_WS_UPSTREAM", "wss://ws2.qxbroker.com")

# تشخيص آخر جلب شموع / WebSocket (للوج)
_HUSAAM_WS_LAST: dict = {}
_BROWSER_TICKETS: dict = {}
_WS_BRIDGE = None


def _ws_history_asset_matches(api, msg_asset, current_asset) -> bool:
    """السيرفر قد يرسل asset كرقم والجلسة EURUSD_otc — المكتبة تقارن بـ == فتفشل."""
    if msg_asset is None or current_asset is None:
        return False
    if msg_asset == current_asset:
        return True
    codes = getattr(api, "codes_asset", None) or {}
    if isinstance(current_asset, str) and codes.get(current_asset) == msg_asset:
        return True
    if isinstance(msg_asset, int):
        for sym, aid in codes.items():
            if aid == msg_asset and sym == current_asset:
                return True
    return str(msg_asset) == str(current_asset)


def _install_pyquotex_ws_candle_asset_fix():
    """يصل رد history/list/v2 لكن pyquotex لا يخزّن candles_data إذا اختلف نوع مفتاح asset."""
    if not QX:
        return
    try:
        from pyquotex.ws.client import WebsocketClient
    except ImportError:
        return
    if getattr(WebsocketClient.on_message, "_husaam_ws_patch", False):
        return
    _orig = WebsocketClient.on_message

    def _on_message(self, wss, msg):
        _orig(self, wss, msg)
        try:
            if not isinstance(msg, (bytes, bytearray)) or len(msg) < 2:
                return
            raw = msg[1:].decode("utf-8", errors="ignore")
            if "history" not in raw and "candles" not in raw:
                return
            message = json.loads(raw)
        except Exception:
            return
        if not isinstance(message, dict) or "history" not in message:
            return
        ma = message.get("asset")
        ca = getattr(self.api, "current_asset", None)
        hist = message.get("history")
        try:
            hl = len(hist) if hist is not None else 0
        except Exception:
            hl = 0
        _HUSAAM_WS_LAST["last_msg_asset"] = ma
        _HUSAAM_WS_LAST["last_current_asset"] = ca
        _HUSAAM_WS_LAST["last_history_len"] = hl
        _HUSAAM_WS_LAST["last_ts"] = time.time()
        if not _ws_history_asset_matches(self.api, ma, ca):
            log.debug(
                "WS history: asset غير متطابق (msg=%s current=%s) — قد يمنع pyquotex التخزين",
                ma,
                ca,
            )
            return
        patched = False
        if self.api.candles.candles_data is None and hist is not None:
            self.api.candles.candles_data = hist
            patched = True
        if message.get("candles") and ca is not None:
            self.api.candle_v2_data[ca] = message
            if ma is not None and ma != ca:
                self.api.candle_v2_data[ma] = message
            patched = True
        if patched:
            _HUSAAM_WS_LAST["patched_fill"] = True
            log.info(
                "🔧 WS: عُدّل تخزين شموع history (msg_asset=%s current=%s hist_len=%s v2=%s)",
                ma,
                ca,
                hl,
                bool(message.get("candles")),
            )
        else:
            _HUSAAM_WS_LAST["patched_fill"] = False

    _on_message._husaam_ws_patch = True
    WebsocketClient.on_message = _on_message


_install_pyquotex_ws_candle_asset_fix()

os.makedirs("logs", exist_ok=True)
os.makedirs("data", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/bot.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("AboodTrader")

_ws_log = logging.getLogger("WSBridge")
_ws_log.setLevel(logging.INFO)
if not any(isinstance(h, logging.FileHandler) and getattr(h, "baseFilename", "").endswith("ws_bridge.log") for h in _ws_log.handlers):
    _h = logging.FileHandler("logs/ws_bridge.log", encoding="utf-8")
    _h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    _ws_log.addHandler(_h)


def _configure_quiet_loggers():
    """يخفي سجل وصول Uvicorn وضجيج websocket (Sending ping) عند أي طريقة تشغيل."""
    logging.getLogger("uvicorn.access").disabled = True
    logging.getLogger("websocket").setLevel(logging.WARNING)


_configure_quiet_loggers()


def _proxy_parts(proxy_url: str) -> dict:
    if not proxy_url:
        return {}
    try:
        u = urlparse(proxy_url)
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


def _start_ws_bridge_if_needed():
    global _WS_BRIDGE
    if not (QX and QUOTEX_USE_PLAYWRIGHT_BRIDGE):
        return
    try:
        from ws_bridge import WSBridgeService
    except Exception as e:
        log.error("WS bridge import failed: %s", e)
        return
    if _WS_BRIDGE is None:
        _WS_BRIDGE = WSBridgeService(
            listen_host="127.0.0.1",
            listen_port=QUOTEX_WS_BRIDGE_PORT,
            upstream_base=QUOTEX_WS_UPSTREAM,
            proxy_url=QUOTEX_PROXY_URL,
        )
        _WS_BRIDGE.start()
        log.info("WS proxy patch enabled (bridge port=%s, upstream=%s)", QUOTEX_WS_BRIDGE_PORT, QUOTEX_WS_UPSTREAM)


def _install_ws_redirect_patch():
    if not (QX and (QUOTEX_USE_PLAYWRIGHT_BRIDGE or QUOTEX_PROXY_URL)):
        return
    try:
        import websocket
    except Exception as e:
        log.error("websocket-client import failed for bridge patch: %s", e)
        return
    if getattr(websocket.WebSocketApp.__init__, "_abood_bridge_patch", False):
        return
    _orig_init = websocket.WebSocketApp.__init__

    def _patched_init(self, url, *args, **kwargs):
        try:
            target = str(url or "")
            if "ws2.qxbroker.com" in target:
                u = urlparse(target)
                path_q = u.path or "/"
                if u.query:
                    path_q += f"?{u.query}"
                kwargs["header"] = kwargs.get("header", [])
                p = _proxy_parts(QUOTEX_PROXY_URL)
                if p:
                    kwargs["http_proxy_host"] = p.get("host")
                    kwargs["http_proxy_port"] = p.get("port")
                    kwargs["proxy_type"] = p.get("type")
                    if p.get("auth"):
                        kwargs["http_proxy_auth"] = p.get("auth")
                    log.info("WS proxy patch enabled host=%s port=%s type=%s",
                             p.get("host"), p.get("port"), p.get("type"))
                if QUOTEX_USE_PLAYWRIGHT_BRIDGE:
                    new_url = f"ws://127.0.0.1:{QUOTEX_WS_BRIDGE_PORT}{path_q}"
                    log.info("WebSocket redirected via Playwright bridge: %s -> %s", target, new_url)
                    url = new_url
        except Exception as e:
            log.warning("WS redirect patch failed, fallback direct WS: %s", e)
        return _orig_init(self, url, *args, **kwargs)

    _patched_init._abood_bridge_patch = True
    websocket.WebSocketApp.__init__ = _patched_init


_start_ws_bridge_if_needed()
_install_ws_redirect_patch()

USERS_F  = "data/users.json"
ADMIN_PW = "Admin@2024"

def load(f, d):
    try:
        if os.path.exists(f): return json.load(open(f, encoding="utf-8"))
    except: pass
    return d

def save(f, d):
    json.dump(d, open(f,"w",encoding="utf-8"), ensure_ascii=False, indent=2)

USERS        = load(USERS_F, {})
ADMIN_TOKENS = set()
_ADMIN_TOKEN_F = os.path.join("data", "admin_token.txt")
_ADMIN_TOKENS_JSON = os.path.join("data", "admin_tokens.json")


def _load_admin_tokens_into_set():
    try:
        merged = []
        if os.path.exists(_ADMIN_TOKENS_JSON):
            data = json.load(open(_ADMIN_TOKENS_JSON, encoding="utf-8"))
            if isinstance(data, list):
                merged.extend(t for t in data if isinstance(t, str) and len(t) >= 8)
        if os.path.exists(_ADMIN_TOKEN_F):
            t = open(_ADMIN_TOKEN_F, encoding="utf-8").read().strip()
            if len(t) >= 8 and t not in merged:
                merged.append(t)
        for t in merged:
            ADMIN_TOKENS.add(t)
        if merged and not os.path.exists(_ADMIN_TOKENS_JSON):
            json.dump(merged[-20:], open(_ADMIN_TOKENS_JSON, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    except Exception:
        pass


def _persist_admin_token(t: str):
    ADMIN_TOKENS.add(t)
    try:
        arr = []
        if os.path.exists(_ADMIN_TOKENS_JSON):
            arr = json.load(open(_ADMIN_TOKENS_JSON, encoding="utf-8"))
            if not isinstance(arr, list):
                arr = []
        if t not in arr:
            arr.append(t)
        arr = arr[-20:]
        json.dump(arr, open(_ADMIN_TOKENS_JSON, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
        open(_ADMIN_TOKEN_F, "w", encoding="utf-8").write(t)
    except Exception:
        pass


_load_admin_tokens_into_set()

def save_users(): save(USERS_F, USERS)

# ── PIN ────────────────────────────────────────────────────────────────────────
_pin_queues: dict = {}

def get_pin_q(email):
    if email not in _pin_queues:
        _pin_queues[email] = queue.Queue()
    return _pin_queues[email]

def make_pin_input(email, S):
    def _f(prompt=""):
        log.info(f"🔐 PIN مطلوب: {email}")
        S["needs_pin"] = True
        try:
            pin = get_pin_q(email).get(timeout=120)
            S["needs_pin"] = False
            return pin
        except queue.Empty:
            S["needs_pin"] = False
            return ""
    return _f

# ── Sessions — كل مشترك مستقل ─────────────────────────────────────────────────
SESSIONS: dict = {}

def new_session(email=""):
    sim_demo_balance = 10_000.0 if not QX else 0.0
    return {
        "logged_in":      False,
        "needs_pin":      False,
        "email":          email,
        "currency":       "USD",
        "real_balance":   0.0,
        "demo_balance":   sim_demo_balance,
        "start_balance":  0.0,
        "running":        False,
        "current_trade":  None,
        "trades":         [],
        "wins":           0,
        "losses":         0,
        "session_profit": 0.0,
        "client":         None,
        "browser_state_file": "",
        "stop_event":     threading.Event(),
        "sim_mode":       not QX,
        "last_signal":    "",
        "candles_ok":     False,
        "candle_source":  "",   # HUSAAM_EMA10: مصدر الشموع للعرض
        "status_msg":     "",   # رسالة للمشترك
    }

def get_session(token) -> dict:
    if not token: return None
    if token not in SESSIONS:
        for email, u in USERS.items():
            if u.get("session_token") == token:
                SESSIONS[token] = new_session(email)
                break
    return SESSIONS.get(token)

def get_session_by_email(email) -> dict:
    for S in SESSIONS.values():
        if S["email"] == email and S["logged_in"]:
            return S
    return None


def _is_sim_mode_session(S: dict | None) -> bool:
    # محاكاة فقط عندما لا يوجد pyquotex ولا browser-state فعّال.
    if not S:
        return not QX
    if not QX:
        return True
    if BROWSER_QX_MODE and S.get("browser_state_file"):
        return False
    if S.get("client"):
        return False
    return True

# ── Models ────────────────────────────────────────────────────────────────────
class RegisterReq(BaseModel):
    email: str
    name: str = ""

class LoginReq(BaseModel):
    email: str
    password: str
    token: str

class PinReq(BaseModel):
    pin: str
    token: str

class BotReq(BaseModel):
    asset: str           # الزوج الأول (للتوافق مع الكود القديم)
    assets: list = []    # قائمة الأزواج المختارة (جديد)
    amount: float
    strategy: str
    account_type: str
    profit_limit: float
    stop_loss: float
    double_on_loss: bool = False
    token: str

class TokenReq(BaseModel):
    token: str

class AdminLoginReq(BaseModel):
    password: str

class ApproveReq(BaseModel):
    admin_token: str
    email: str
    action: str
    note: str = ""

# ── Async Loop ────────────────────────────────────────────────────────────────
_loop = asyncio.new_event_loop()
threading.Thread(target=_loop.run_forever, daemon=True).start()

def run_async(coro, timeout=30):
    return asyncio.run_coroutine_threadsafe(coro, _loop).result(timeout=timeout)

# ── loop مستقل لكل مشترك — يمنع تعليق الكل بسبب مشترك واحد ──────────────────
_session_loops: dict = {}
_session_loop_lock = threading.Lock()

def get_session_loop(email: str):
    with _session_loop_lock:
        if email not in _session_loops:
            loop = asyncio.new_event_loop()
            threading.Thread(target=loop.run_forever, daemon=True, name=f"loop-{email}").start()
            _session_loops[email] = loop
        return _session_loops[email]

def run_async_for(email: str, coro, timeout=30):
    loop = get_session_loop(email)
    try:
        return asyncio.run_coroutine_threadsafe(coro, loop).result(timeout=timeout)
    except TimeoutError:
        log.warning("run_async_for(%s): انتهت المهلة بعد %.0f ث", email, timeout)
        raise
    except Exception as e:
        log.error("run_async_for(%s): %s", email, e or type(e).__name__, exc_info=True)
        raise

# ══════════════════════════════════════════════════════════════════════════════
# المؤشرات التقنية الحقيقية
# ══════════════════════════════════════════════════════════════════════════════
def calc_rsi(closes, period=14):
    if len(closes) < period + 1: return 50.0
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i-1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    ag = sum(gains[:period]) / period
    al = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        ag = (ag*(period-1) + gains[i]) / period
        al = (al*(period-1) + losses[i]) / period
    if al == 0: return 100.0
    return round(100 - (100/(1+ag/al)), 2)

def calc_ema(closes, period):
    if not closes or len(closes) < period:
        return closes[-1] if closes else 0
    k = 2/(period+1)
    ema = sum(closes[:period])/period
    for p in closes[period:]: ema = p*k + ema*(1-k)
    return ema

def calc_macd(closes):
    if len(closes) < 35: return 0, 0, 0
    ema12 = calc_ema(closes, 12)
    ema26 = calc_ema(closes, 26)
    macd  = ema12 - ema26
    mv = []
    for i in range(9, 0, -1):
        if len(closes) > i:
            mv.append(calc_ema(closes[:-i],12) - calc_ema(closes[:-i],26))
    mv.append(macd)
    sig  = calc_ema(mv, min(9,len(mv)))
    return round(macd,8), round(sig,8), round(macd-sig,8)


def calc_macd_series(closes):
    """
    MACD(12,26) وخط الإشارة EMA(9) على سلسلة الماكد، والهستوغرام = ماكد − إشارة.
    يُملأ من الفهرس 25 فصاعداً (0-based) ليتوافق مع الشارت القياسي.
    """
    n = len(closes)
    if n < 35:
        return None, None, None
    macd_line = [0.0] * n
    signal_line = [0.0] * n
    hist = [0.0] * n
    for i in range(25, n):
        macd_line[i] = calc_ema(closes[: i + 1], 12) - calc_ema(closes[: i + 1], 26)
    for i in range(25, n):
        mslice = macd_line[25 : i + 1]
        if mslice:
            signal_line[i] = calc_ema(mslice, 9)
            hist[i] = macd_line[i] - signal_line[i]
    return macd_line, signal_line, hist


def calc_bb(closes, period=20):
    if len(closes) < period: return 50.0
    w   = closes[-period:]
    mid = sum(w)/period
    std = (sum((x-mid)**2 for x in w)/period)**0.5
    if std == 0: return 50.0
    return round((closes[-1]-(mid-2*std))/(4*std)*100, 2)

def calc_stoch(closes, k=14):
    """Stochastic Oscillator"""
    if len(closes) < k: return 50.0
    low_k  = min(closes[-k:])
    high_k = max(closes[-k:])
    if high_k == low_k: return 50.0
    return round((closes[-1]-low_k)/(high_k-low_k)*100, 2)

def calc_williams(closes, period=14):
    """Williams %R"""
    if len(closes) < period: return -50.0
    high = max(closes[-period:])
    low  = min(closes[-period:])
    if high == low: return -50.0
    return round((high-closes[-1])/(high-low)*-100, 2)

# ── استراتيجية حسام EMA10 (شموع مغلقة 1m فقط، EMA10):
#    CALL (ترند صاعد): سياق فوق EMA → حمراء إشارية على EMA → خضراء تأكيد → CALL
#    PUT  (ترند هابط): سياق تحت EMA → خضراء إشارية على EMA → حمراء تأكيد → PUT
_HUSAAM_EMA10_PERIOD = 10
_HUSAAM_EMA10_MIN_CANDLES = 40
# مصدر التحليل: آخر N شمعة 1m من شارت Quotex (بعد التنظيف) — لا يُبنى على التيكات
_HUSAAM_EMA10_ANALYSIS_BARS = 40
_HUSAAM_EMA10_CONTEXT_BEFORE_SIGNAL = 4
_HUSAAM_EMA10_CONTEXT_ABOVE_MIN = 2
_HUSAAM_EMA10_CONTEXT_BELOW_MIN = 2
_HUSAAM_EMA10_CLOSE_TOL_PCT = 0.0018
_HUSAAM_EMA10_LOW_TOUCH_PCT = 0.0026
_HUSAAM_EMA10_BREAKDOWN_EXTRA_PCT = 0.0014
_HUSAAM_EMA10_MIN_GREEN_BODY_RATIO = 0.18
_HUSAAM_EMA10_MIN_RED_BODY_RATIO = 0.18
_HUSAAM_EMA10_MAX_SIGN_FLIPS = 8
_HUSAAM_EMA10_FLAT_LOOKBACK = 10
_HUSAAM_EMA10_MAX_FLAT_SLOPE_PCT = 0.00006
_HUSAAM_EMA10_SCORE = 10
# شارت Quotex دقيقة — الإشارة بعد إغلاق الشمعة على EMA وليس أثناء تكوّنها
_HUSAAM_EMA10_CANDLE_SECS = 60
# تخزين نتيجة get_candles ناجحة (ثوانٍ) — يقلل الضغط ويحافظ على مصدر واحد مع الشارت
_HUSAAM_QUOTEX_CACHE_SEC = 90.0
# مهلة run_async_for لجلب الشموع (بعد v2 قصير + get_candles قصير لكل دورة)
_HUSAAM_QUOTEX_FETCH_TIMEOUT_SEC = 55.0

# ── استراتيجية حسام الخاصة (MACD + هستوغرام، شموع 1m مغلقة) ──
_HUSAAM_PRIVATE_MIN_BARS = 55
_HUSAAM_PRIVATE_TREND_EMA = 21
_HUSAAM_PRIVATE_SCORE = 10

# ── HUSAAM: RSI + MACD (تآزر) + مدى شمعة vs وسيط + نافذة UTC — توازن بين جودة وعدد الإشارات ──
_HUSAAM_STRICT_SCORE = 10
_HUSAAM_STRICT_RSI_OS = 36
_HUSAAM_STRICT_RSI_OB = 64
_HUSAAM_STRICT_RANGE_LOOKBACK = 20
# آخر شمعة: المدى ≥ هذا النسبة من وسيط مدى الشموع السابقة (أقل حساسية من المتوسط)
_HUSAAM_STRICT_RANGE_VS_MEDIAN = 0.78
# OTC — الساعة UTC (توسيعة خفيفة عن 7–22)
_HUSAAM_TRADE_UTC_START = 6
_HUSAAM_TRADE_UTC_END = 23


def _husaam_trade_window_ok_utc() -> bool:
    """True إذا الساعة الحالية UTC ضمن [start, end)."""
    h = datetime.now(timezone.utc).hour
    if _HUSAAM_TRADE_UTC_START <= _HUSAAM_TRADE_UTC_END:
        return _HUSAAM_TRADE_UTC_START <= h < _HUSAAM_TRADE_UTC_END
    return h >= _HUSAAM_TRADE_UTC_START or h < _HUSAAM_TRADE_UTC_END


def _husaam_macd_cross_up(macd_line, signal_line, i: int) -> bool:
    """تقاطع صاعد: خط MACD يقطع خط الإشارة من أسفل إلى فوق."""
    if i < 1:
        return False
    return macd_line[i] > signal_line[i] and macd_line[i - 1] <= signal_line[i - 1]


def _husaam_macd_cross_down(macd_line, signal_line, i: int) -> bool:
    """تقاطع هابط: خط MACD يقطع خط الإشارة من فوق إلى أسفل."""
    if i < 1:
        return False
    return macd_line[i] < signal_line[i] and macd_line[i - 1] >= signal_line[i - 1]


def _husaam_median(vals: list) -> float:
    if not vals:
        return 0.0
    s = sorted(vals)
    m = len(s) // 2
    if len(s) % 2:
        return float(s[m])
    return (float(s[m - 1]) + float(s[m])) / 2.0


def _husaam_macd_bullish_confirm(macd_line, signal_line, hist, n: int) -> bool:
    """تأكيد صعود: تقاطع أو زخم هستو يتحسّن مع ماكد فوق الإشارة، أو عبور هستو فوق الصفر."""
    if n < 26:
        return False
    if _husaam_macd_cross_up(macd_line, signal_line, n):
        return True
    if macd_line[n] > signal_line[n] and hist[n] > hist[n - 1]:
        return True
    if hist[n] > 0 and hist[n - 1] <= 0:
        return True
    return False


def _husaam_macd_bearish_confirm(macd_line, signal_line, hist, n: int) -> bool:
    """تأكيد هبوط: تقاطع أو هستو يضعف مع ماكد تحت الإشارة، أو عبور هستو تحت الصفر."""
    if n < 26:
        return False
    if _husaam_macd_cross_down(macd_line, signal_line, n):
        return True
    if macd_line[n] < signal_line[n] and hist[n] < hist[n - 1]:
        return True
    if hist[n] < 0 and hist[n - 1] >= 0:
        return True
    return False


def _analyze_husaam_strict(candles) -> str:
    """
    CALL: RSI < منطقة oversold + تأكيد MACD (تقاطع أو زخم/عبور هستو) + مدى آخر شمعة ≥ نسبة من وسيط المدى السابق.
    PUT: RSI > منطقة overbought + تأكيد MACD الهابط + نفس فلتر المدى.
    نافذة UTC لتقليل التذبذب خارج أوقات السيولة.
    """
    if not _husaam_trade_window_ok_utc():
        return "wait"
    kit = _candles_ohlc_kit(_ema10_closed_only(candles))
    lb = _HUSAAM_STRICT_RANGE_LOOKBACK
    if len(kit) < max(35, lb + 1):
        return "wait"
    closes = [float(c["close"]) for c in kit]
    ranges = [max(float(c["high"]) - float(c["low"]), 1e-12) for c in kit]
    last_r = ranges[-1]
    prev = ranges[-(lb + 1) : -1]
    med_r = _husaam_median(prev)
    if med_r <= 0 or last_r < med_r * _HUSAAM_STRICT_RANGE_VS_MEDIAN:
        return "wait"

    rsi = calc_rsi(closes)
    macd_line, signal_line, hist = calc_macd_series(closes)
    if macd_line is None:
        return "wait"
    n = len(closes) - 1
    if n < 26:
        return "wait"

    if rsi < _HUSAAM_STRICT_RSI_OS and _husaam_macd_bullish_confirm(
        macd_line, signal_line, hist, n
    ):
        log.info(
            "🏆 HUSAAM CALL | RSI=%s MACD confirm | range=%.6f med=%.6f (min=%.6f)",
            rsi,
            last_r,
            med_r,
            med_r * _HUSAAM_STRICT_RANGE_VS_MEDIAN,
        )
        return "call"
    if rsi > _HUSAAM_STRICT_RSI_OB and _husaam_macd_bearish_confirm(
        macd_line, signal_line, hist, n
    ):
        log.info(
            "🏆 HUSAAM PUT | RSI=%s MACD confirm | range=%.6f med=%.6f (min=%.6f)",
            rsi,
            last_r,
            med_r,
            med_r * _HUSAAM_STRICT_RANGE_VS_MEDIAN,
        )
        return "put"
    return "wait"


def _candles_ohlc_kit(candles) -> list:
    out = []
    for c in candles:
        if isinstance(c, dict):
            cl = float(c.get("close", c.get("c", 0)) or 0)
            if cl <= 0:
                continue
            o = float(c.get("open", cl) or cl)
            h = float(c.get("high", cl) or cl)
            l = float(c.get("low", cl) or cl)
            hi = max(h, cl, o)
            lo = min(l, cl, o)
            out.append({"open": o, "high": hi, "low": lo, "close": cl})
        elif hasattr(c, "close"):
            cl = float(getattr(c, "close", 0) or 0)
            if cl > 0:
                out.append({"open": cl, "high": cl, "low": cl, "close": cl})
    return out


def _ema10_closed_only(candles) -> list:
    """شموع مكتملة فقط: تيكات محلية = إسقاط آخر شمعة؛ من API = إسقاط الدقيقة الجارية إن وُجدت."""
    if not candles:
        return []
    last = candles[-1]
    if isinstance(last, dict) and last.get("time") is not None:
        t0 = float(last["time"])
        if time.time() < t0 + _HUSAAM_EMA10_CANDLE_SECS:
            return candles[:-1]
        return candles[:]
    if len(candles) < 2:
        return []
    return candles[:-1]


def _husaam_ema10_ema_at(closes, idx: int, p: int) -> float:
    """EMA(p) عند إغلاق الشمعة idx (باستخدام closes[0..idx])."""
    if idx < p - 1 or idx >= len(closes):
        return 0.0
    return calc_ema(closes[: idx + 1], p)


def _husaam_ema10_sign_flips(closes, p: int, last_idx: int, lookback: int) -> int:
    """تقلّبات جهة (close−EMA) — رفض السوق المتذبذب بدون اتجاه."""
    if last_idx < p - 1:
        return 99
    start = max(p - 1, last_idx - lookback + 1)
    flips = 0
    prev_d = None
    for i in range(start, last_idx + 1):
        ema_i = _husaam_ema10_ema_at(closes, i, p)
        d = closes[i] - ema_i
        if prev_d is not None and prev_d * d < 0:
            flips += 1
        prev_d = d
    return flips


def _husaam_ema10_red_near_ema(
    close_sig: float,
    low_sig: float,
    ema_sig: float,
    close_tol: float,
    touch_tol: float,
) -> bool:
    """إغلاق/لمس قريب من EMA دون كسر واضح للأسفل (يُكمّل مع فحص الكسر منفصلاً)."""
    if close_sig < ema_sig - 1.5 * close_tol:
        return False
    if abs(close_sig - ema_sig) <= 1.5 * close_tol:
        return True
    if low_sig <= ema_sig + touch_tol and low_sig >= ema_sig - 2.0 * touch_tol:
        return True
    return False


def _husaam_ema10_green_near_ema(
    close_sig: float,
    high_sig: float,
    ema_sig: float,
    close_tol: float,
    touch_tol: float,
) -> bool:
    """إغلاق/لمس قريب من EMA دون كسر واضح للأعلى (ترند هابط — رفض صعود)."""
    if close_sig > ema_sig + 1.5 * close_tol:
        return False
    if abs(close_sig - ema_sig) <= 1.5 * close_tol:
        return True
    if high_sig >= ema_sig - touch_tol and high_sig <= ema_sig + 2.0 * touch_tol:
        return True
    return False


def _husaam_ema10_reject_flat_or_choppy(closes, p: int, ent: int) -> bool:
    if len(closes) >= p + _HUSAAM_EMA10_FLAT_LOOKBACK:
        ema_now = calc_ema(closes, p)
        ema_old = calc_ema(closes[: len(closes) - _HUSAAM_EMA10_FLAT_LOOKBACK], p)
        if ema_now > 0 and abs(ema_now - ema_old) / ema_now < _HUSAAM_EMA10_MAX_FLAT_SLOPE_PCT:
            return True
    if _husaam_ema10_sign_flips(closes, p, ent, lookback=15) > _HUSAAM_EMA10_MAX_SIGN_FLIPS:
        return True
    return False


def _analyze_husaam_ema10_signal(candles) -> tuple:
    """
    CALL: ترند صاعد — حمراء إشارية على EMA ثم خضراء تأكيد (شموع مغلقة).
    PUT:  ترند هابط — خضراء إشارية على EMA ثم حمراء تأكيد (لا دخول قبل إغلاق الخضراء).
    آخر 40 شمعة 1m؛ EMA10 على نفس السلسلة؛ لا تيكات.
    """
    closed = _ema10_closed_only(candles)
    kit = _candles_ohlc_kit(closed)
    p = _HUSAAM_EMA10_PERIOD
    if len(kit) < _HUSAAM_EMA10_ANALYSIS_BARS:
        return "wait", 0
    kit = kit[-_HUSAAM_EMA10_ANALYSIS_BARS :]
    closes = [float(c["close"]) for c in kit]
    if len(closes) < _HUSAAM_EMA10_ANALYSIS_BARS:
        return "wait", 0

    sig = len(kit) - 2
    ent = len(kit) - 1
    if sig < _HUSAAM_EMA10_CONTEXT_BEFORE_SIGNAL:
        return "wait", 0

    o_s = float(kit[sig]["open"])
    h_s = float(kit[sig]["high"])
    low_s = float(kit[sig]["low"])
    cl_s = float(kit[sig]["close"])
    o_e = float(kit[ent]["open"])
    h_e = float(kit[ent]["high"])
    low_e = float(kit[ent]["low"])
    cl_e = float(kit[ent]["close"])
    if min(cl_s, cl_e) <= 0 or h_s < low_s or h_e < low_e:
        return "wait", 0

    call_pattern = cl_s < o_s and cl_e > o_e
    put_pattern = cl_s > o_s and cl_e < o_e
    if not call_pattern and not put_pattern:
        return "wait", 0

    ema_sig = _husaam_ema10_ema_at(closes, sig, p)
    ema_ent = _husaam_ema10_ema_at(closes, ent, p)
    if ema_sig <= 0 or ema_ent <= 0:
        return "wait", 0

    close_tol = max(ema_sig * _HUSAAM_EMA10_CLOSE_TOL_PCT, 1e-12)
    touch_tol = max(ema_sig * _HUSAAM_EMA10_LOW_TOUCH_PCT, 1e-12)
    break_tol = max(ema_sig * (_HUSAAM_EMA10_BREAKDOWN_EXTRA_PCT + _HUSAAM_EMA10_CLOSE_TOL_PCT * 0.5), 1e-12)

    if call_pattern:
        # كسر واضح أسفل EMA على الشمعة الحمراء → رفض
        if cl_s < ema_sig - break_tol:
            return "wait", 0
        if not _husaam_ema10_red_near_ema(cl_s, low_s, ema_sig, close_tol, touch_tol):
            return "wait", 0
        above = 0
        for k in range(sig - _HUSAAM_EMA10_CONTEXT_BEFORE_SIGNAL, sig):
            ema_k = _husaam_ema10_ema_at(closes, k, p)
            if ema_k <= 0:
                return "wait", 0
            ct = max(ema_k * _HUSAAM_EMA10_CLOSE_TOL_PCT, 1e-12)
            if closes[k] >= ema_k - 0.5 * ct:
                above += 1
        if above < _HUSAAM_EMA10_CONTEXT_ABOVE_MIN:
            return "wait", 0
        rng_e = h_e - low_e
        if rng_e <= 1e-12:
            return "wait", 0
        body_e = cl_e - o_e
        if body_e / rng_e < _HUSAAM_EMA10_MIN_GREEN_BODY_RATIO:
            return "wait", 0
        touch_tol_ent = max(ema_ent * _HUSAAM_EMA10_LOW_TOUCH_PCT, 1e-12)
        if cl_e < ema_ent - 0.35 * touch_tol_ent:
            return "wait", 0
        if o_e < ema_ent - 3.5 * touch_tol_ent:
            return "wait", 0
        if _husaam_ema10_reject_flat_or_choppy(closes, p, ent):
            return "wait", 0
        log.debug(
            "📌 HUSAAM_EMA10 CALL red@sig=%s green@ent=%s ema_sig=%.5f ema_ent=%.5f",
            sig,
            ent,
            ema_sig,
            ema_ent,
        )
        return "call", _HUSAAM_EMA10_SCORE

    # PUT: كسر واضح للأعلى على الشمعة الخضراء الإشارية → رفض (ليس رفضاً للصعود)
    if cl_s > ema_sig + break_tol:
        return "wait", 0
    if not _husaam_ema10_green_near_ema(cl_s, h_s, ema_sig, close_tol, touch_tol):
        return "wait", 0
    below = 0
    for k in range(sig - _HUSAAM_EMA10_CONTEXT_BEFORE_SIGNAL, sig):
        ema_k = _husaam_ema10_ema_at(closes, k, p)
        if ema_k <= 0:
            return "wait", 0
        ct = max(ema_k * _HUSAAM_EMA10_CLOSE_TOL_PCT, 1e-12)
        if closes[k] <= ema_k + 0.5 * ct:
            below += 1
    if below < _HUSAAM_EMA10_CONTEXT_BELOW_MIN:
        return "wait", 0
    rng_e = h_e - low_e
    if rng_e <= 1e-12:
        return "wait", 0
    body_down = o_e - cl_e
    if body_down / rng_e < _HUSAAM_EMA10_MIN_RED_BODY_RATIO:
        return "wait", 0
    touch_tol_ent = max(ema_ent * _HUSAAM_EMA10_LOW_TOUCH_PCT, 1e-12)
    if cl_e > ema_ent + 0.35 * touch_tol_ent:
        return "wait", 0
    if o_e > ema_ent + 3.5 * touch_tol_ent:
        return "wait", 0
    if _husaam_ema10_reject_flat_or_choppy(closes, p, ent):
        return "wait", 0
    log.debug(
        "📌 HUSAAM_EMA10 PUT green@sig=%s red@ent=%s ema_sig=%.5f ema_ent=%.5f",
        sig,
        ent,
        ema_sig,
        ema_ent,
    )
    return "put", _HUSAAM_EMA10_SCORE


def _analyze_husaam_private_signal(candles) -> tuple:
    """
    استراتيجية حسام الخاصة — MACD + هستوغرام على شموع 1m مغلقة فقط.
    هابط: تحت EMA + هستوغرام < 0 + أول شمعة خضراء بالهستوغرام + شمعة سعر خضراء + الخطان فوق الهستو → PUT.
    صاعد: فوق EMA + هستوغرام > 0 + 6 شموع خضراء صاعدة + شمعة سعر حمراء + الخطان تحت الهستو → CALL.
    """
    kit = _candles_ohlc_kit(_ema10_closed_only(candles))
    if len(kit) < _HUSAAM_PRIVATE_MIN_BARS:
        return "wait", 0
    closes = [float(c["close"]) for c in kit]
    macd_line, signal_line, hist = calc_macd_series(closes)
    if macd_line is None:
        return "wait", 0
    i = len(kit) - 1
    o_i = float(kit[i]["open"])
    c_i = float(kit[i]["close"])
    ema_t = _husaam_ema10_ema_at(closes, i, _HUSAAM_PRIVATE_TREND_EMA)
    if ema_t <= 0:
        return "wait", 0

    # CALL: ترند صاعد، هستو فوق الصفر، 6 هستوغرامات خضراء متتالية صاعدة، شمعة حمراء، خطا MACD تحت الهستو
    if i >= 32 and c_i < o_i and c_i > ema_t:
        ok = True
        for k in range(i - 6, i):
            if k < 26:
                ok = False
                break
            if hist[k] <= 0 or hist[k] <= hist[k - 1]:
                ok = False
                break
        if ok and macd_line[i] < hist[i] and signal_line[i] < hist[i]:
            log.debug(
                "📌 HUSAAM_PRIVATE CALL i=%s hist=%.8f macd=%.8f sig=%.8f",
                i,
                hist[i],
                macd_line[i],
                signal_line[i],
            )
            return "call", _HUSAAM_PRIVATE_SCORE

    # PUT: ترند هابط، هستو تحت الصفر، أول شمعة خضراء بالهستوغرام، شمعة سعر خضراء، خطا MACD فوق الهستو
    if i >= 27 and c_i > o_i and c_i < ema_t:
        if (
            hist[i] < 0
            and hist[i] > hist[i - 1]
            and hist[i - 1] <= hist[i - 2]
            and macd_line[i] > hist[i]
            and signal_line[i] > hist[i]
        ):
            log.debug(
                "📌 HUSAAM_PRIVATE PUT i=%s hist=%.8f macd=%.8f sig=%.8f",
                i,
                hist[i],
                macd_line[i],
                signal_line[i],
            )
            return "put", _HUSAAM_PRIVATE_SCORE

    return "wait", 0


def analyze(candles, strategy) -> str:
    if strategy == "HUSAAM_EMA10":
        d, _ = _analyze_husaam_ema10_signal(candles)
        return d
    if strategy == "HUSAAM_PRIVATE":
        d, _ = _analyze_husaam_private_signal(candles)
        return d
    if strategy == "HUSAAM":
        return _analyze_husaam_strict(candles)
    if len(candles) < 35: return "wait"
    closes = []
    for c in candles:
        p = 0
        if isinstance(c, dict):
            p = float(c.get("close", c.get("c", 0)) or 0)
        elif hasattr(c, "close"):
            p = float(c.close or 0)
        if p > 0: closes.append(p)
    if len(closes) < 35: return "wait"

    rsi         = calc_rsi(closes)
    _, _, hist  = calc_macd(closes)
    bb          = calc_bb(closes)
    ema9        = calc_ema(closes, 9)
    ema21       = calc_ema(closes, 21)
    ema5        = calc_ema(closes, 5)
    stoch       = calc_stoch(closes)
    williams    = calc_williams(closes)
    # اتجاه الشمعات الأخيرة
    trend_up    = sum(1 for i in range(-5,-1) if closes[i] < closes[i+1])
    trend_dn    = sum(1 for i in range(-5,-1) if closes[i] > closes[i+1])

    log.info(f"📊 RSI={rsi} MACD={hist:.6f} BB%={bb} Stoch={stoch} W%R={williams} EMA5/9/21={ema5:.5f}/{ema9:.5f}/{ema21:.5f}")

    cp = pp = 0

    # ── استراتيجية النخبة — تجمع كل المؤشرات بأوزان (HUSAAM لها منطق منفصل أعلاه) ──
    if strategy == "ELITE":
        # ── RSI — مؤشر الزخم (وزن عالي) ──
        if rsi < 20:     cp += 5
        elif rsi < 28:   cp += 4
        elif rsi < 35:   cp += 3
        elif rsi < 42:   cp += 1
        if rsi > 80:     pp += 5
        elif rsi > 72:   pp += 4
        elif rsi > 65:   pp += 3
        elif rsi > 58:   pp += 1

        # ── MACD — اتجاه الزخم (وزن عالي) ──
        if hist > 0.0001:   cp += 4
        elif hist > 0:      cp += 2
        if hist < -0.0001:  pp += 4
        elif hist < 0:      pp += 2

        # ── Bollinger Bands — حدود السعر ──
        if bb < 5:     cp += 5
        elif bb < 15:  cp += 4
        elif bb < 25:  cp += 2
        if bb > 95:    pp += 5
        elif bb > 85:  pp += 4
        elif bb > 75:  pp += 2

        # ── Stochastic — تأكيد الزخم ──
        if stoch < 15:   cp += 4
        elif stoch < 25: cp += 3
        elif stoch < 35: cp += 1
        if stoch > 85:   pp += 4
        elif stoch > 75: pp += 3
        elif stoch > 65: pp += 1

        # ── Williams %R — نقاط الانعكاس ──
        if williams < -85:   cp += 4
        elif williams < -70: cp += 3
        elif williams < -60: cp += 1
        if williams > -15:   pp += 4
        elif williams > -30: pp += 3
        elif williams > -40: pp += 1

        # ── EMA Cross — اتجاه السوق ──
        if ema5 > ema9 > ema21:   cp += 4
        elif ema5 < ema9 < ema21: pp += 4
        elif ema5 > ema9:  cp += 2
        elif ema5 < ema9:  pp += 2
        elif ema9 > ema21: cp += 1
        elif ema9 < ema21: pp += 1

        # ── اتجاه الشمعات الأخيرة ──
        if trend_up >= 4:   cp += 3
        elif trend_up >= 3: cp += 2
        elif trend_up >= 2: cp += 1
        if trend_dn >= 4:   pp += 3
        elif trend_dn >= 3: pp += 2
        elif trend_dn >= 2: pp += 1

        # ── تأكيد إضافي: RSI + MACD + EMA في نفس الاتجاه ──
        if (rsi < 40) and (hist > 0) and (ema9 > ema21): cp += 3
        if (rsi > 60) and (hist < 0) and (ema9 < ema21): pp += 3

        # ── Stoch + Williams في نفس الاتجاه ──
        if stoch < 30 and williams < -70: cp += 2
        if stoch > 70 and williams > -30: pp += 2

        log.info(f"🏆 HUSAAM: CALL={cp} PUT={pp} diff={abs(cp-pp)}")
        # عتبة 6 نقاط للدخول — توازن بين الدقة والكمية
        if cp >= pp + 6: return "call"
        if pp >= cp + 6: return "put"
        return "wait"

    # ── الاستراتيجيات الأخرى ──────────────────────────────────────────────
    if strategy in ("RSI","RSI_MACD","ALL"):
        if rsi < 25:   cp += 3
        elif rsi < 35: cp += 2
        elif rsi < 45: cp += 1
        if rsi > 75:   pp += 3
        elif rsi > 65: pp += 2
        elif rsi > 55: pp += 1
    if strategy in ("MACD","RSI_MACD","ALL"):
        if hist > 0:   cp += 2
        elif hist < 0: pp += 2
    if strategy in ("BB","ALL"):
        if bb < 15:    cp += 3
        elif bb < 30:  cp += 2
        elif bb < 40:  cp += 1
        if bb > 85:    pp += 3
        elif bb > 70:  pp += 2
        elif bb > 60:  pp += 1
    if strategy == "ALL":
        if ema9 > ema21: cp += 1
        else:            pp += 1

    log.info(f"🗳️ CALL={cp} PUT={pp}")
    if cp >= pp+2: return "call"
    if pp >= cp+2: return "put"
    return "wait"

def analyze_score(candles, strategy) -> tuple:
    """ترجع (direction, score) — score كلما كبر كلما كانت الإشارة أقوى"""
    if strategy == "HUSAAM_EMA10":
        return _analyze_husaam_ema10_signal(candles)
    if strategy == "HUSAAM_PRIVATE":
        return _analyze_husaam_private_signal(candles)
    if strategy == "HUSAAM":
        d = _analyze_husaam_strict(candles)
        return (d, _HUSAAM_STRICT_SCORE) if d != "wait" else ("wait", 0)
    if len(candles) < 35: return "wait", 0
    closes = []
    for c in candles:
        p = 0
        if isinstance(c, dict):
            p = float(c.get("close", c.get("c", 0)) or 0)
        elif hasattr(c, "close"):
            p = float(c.close or 0)
        if p > 0: closes.append(p)
    if len(closes) < 35: return "wait", 0
    rsi        = calc_rsi(closes)
    _, _, hist = calc_macd(closes)
    bb         = calc_bb(closes)
    ema9       = calc_ema(closes, 9)
    ema21      = calc_ema(closes, 21)
    ema5       = calc_ema(closes, 5)
    stoch      = calc_stoch(closes)
    williams   = calc_williams(closes)
    trend_up   = sum(1 for i in range(-5,-1) if closes[i] < closes[i+1])
    trend_dn   = sum(1 for i in range(-5,-1) if closes[i] > closes[i+1])
    cp = pp = 0
    if strategy == "ELITE":
        if rsi < 20:     cp += 5
        elif rsi < 28:   cp += 4
        elif rsi < 35:   cp += 3
        elif rsi < 42:   cp += 1
        if rsi > 80:     pp += 5
        elif rsi > 72:   pp += 4
        elif rsi > 65:   pp += 3
        elif rsi > 58:   pp += 1
        if hist > 0.0001:   cp += 4
        elif hist > 0:      cp += 2
        if hist < -0.0001:  pp += 4
        elif hist < 0:      pp += 2
        if bb < 5:     cp += 5
        elif bb < 15:  cp += 4
        elif bb < 25:  cp += 2
        if bb > 95:    pp += 5
        elif bb > 85:  pp += 4
        elif bb > 75:  pp += 2
        if stoch < 15:   cp += 4
        elif stoch < 25: cp += 3
        elif stoch < 35: cp += 1
        if stoch > 85:   pp += 4
        elif stoch > 75: pp += 3
        elif stoch > 65: pp += 1
        if williams < -85:   cp += 4
        elif williams < -70: cp += 3
        elif williams < -60: cp += 1
        if williams > -15:   pp += 4
        elif williams > -30: pp += 3
        elif williams > -40: pp += 1
        if ema5 > ema9 > ema21:   cp += 4
        elif ema5 < ema9 < ema21: pp += 4
        elif ema5 > ema9:  cp += 2
        elif ema5 < ema9:  pp += 2
        elif ema9 > ema21: cp += 1
        elif ema9 < ema21: pp += 1
        if trend_up >= 4:   cp += 3
        elif trend_up >= 3: cp += 2
        elif trend_up >= 2: cp += 1
        if trend_dn >= 4:   pp += 3
        elif trend_dn >= 3: pp += 2
        elif trend_dn >= 2: pp += 1
        if (rsi < 40) and (hist > 0) and (ema9 > ema21): cp += 3
        if (rsi > 60) and (hist < 0) and (ema9 < ema21): pp += 3
        if stoch < 30 and williams < -70: cp += 2
        if stoch > 70 and williams > -30: pp += 2
        diff = cp - pp
        if diff >= 6:  return "call", diff
        if diff <= -6: return "put",  abs(diff)
        return "wait", 0
    if strategy in ("RSI","RSI_MACD","ALL"):
        if rsi < 25: cp += 3
        elif rsi < 35: cp += 2
        elif rsi < 45: cp += 1
        if rsi > 75: pp += 3
        elif rsi > 65: pp += 2
        elif rsi > 55: pp += 1
    if strategy in ("MACD","RSI_MACD","ALL"):
        if hist > 0: cp += 2
        elif hist < 0: pp += 2
    if strategy in ("BB","ALL"):
        if bb < 15: cp += 3
        elif bb < 30: cp += 2
        elif bb < 40: cp += 1
        if bb > 85: pp += 3
        elif bb > 70: pp += 2
        elif bb > 60: pp += 1
    if strategy == "ALL":
        if ema9 > ema21: cp += 1
        else:            pp += 1
    diff = cp - pp
    if diff >= 2:  return "call", diff
    if diff <= -2: return "put",  abs(diff)
    return "wait", 0

def _fallback_direction_from_candles(candles) -> tuple:
    """إشارة بديلة محسّنة: تدخل فقط مع توافق اتجاه واضح."""
    closes = []
    for c in candles or []:
        p = 0.0
        if isinstance(c, dict):
            p = float(c.get("close", c.get("c", 0)) or 0)
        elif hasattr(c, "close"):
            p = float(c.close or 0)
        if p > 0:
            closes.append(p)
    if len(closes) < 20:
        return "wait", 0

    ema9 = calc_ema(closes, 9)
    ema21 = calc_ema(closes, 21)
    mom = closes[-1] - closes[-4]
    up = sum(1 for i in range(-6, -1) if closes[i] > closes[i - 1])
    dn = sum(1 for i in range(-6, -1) if closes[i] < closes[i - 1])
    ema_gap_pct = abs(ema9 - ema21) / max(abs(ema21), 1e-12)
    last_range = max(closes[-6:]) - min(closes[-6:])
    if last_range <= 0:
        return "wait", 0
    last_body_strength = abs(closes[-1] - closes[-2]) / last_range

    # شروط fallback الصارمة لتقليل العشوائية:
    # 1) اتجاه EMA واضح
    # 2) زخم آخر 4 شموع بنفس الاتجاه
    # 3) أغلب آخر 5 انتقالات بنفس الاتجاه
    # 4) حركة آخر شمعة ليست ضعيفة جداً
    if ema9 > ema21 and ema_gap_pct >= 0.00040 and mom > 0 and up >= 4 and last_body_strength >= 0.26:
        return "call", 2
    if ema9 < ema21 and ema_gap_pct >= 0.00040 and mom < 0 and dn >= 4 and last_body_strength >= 0.26:
        return "put", 2
    return "wait", 0

# ── مجمع الأسعار الحية ────────────────────────────────────────────────────────
# - جميع الاستراتيجيات (ما عدا الحاجة الخاصة): سحب شموع 1m من شارت Quotex مثل EMA10 — لا تيكات للإشارة.
# - HUSAAM_PRIVATE: نفس السحب لكن آخر 55 شمعة بعد التنظيف.
# - التيكات / realtime: مراقبة وواجهة فقط.
_price_buffers: dict = {}
_sim_market_state: dict = {}
# احتفاظ أطول لبناء شموع 5ث (35 شمعة ≈ 175ث جدارياً على الأقل)
_PRICE_BUFFER_RETENTION_SEC = 1800
# تدفئة: تيكات للمراقبة/الواجهة فقط — لـ EMA10 لا تُستخدم في الإشارة
_WARMUP_COLLECT_SEC = 5
_WARMUP_COLLECT_SEC_EMA10 = 25
_SIM_PAYOUT = 0.82


def _quotex_tick_keys(client, asset: str):
    """مفاتيح تيك السعر: الرمز أولاً (غالباً يطابق message[0][0]) ثم رقم الأصل."""
    keys = [asset]
    if getattr(client, "codes_asset", None):
        aid = client.codes_asset.get(asset)
        if aid is not None and aid not in keys:
            keys.append(aid)
    seen = set()
    out = []
    for k in keys:
        if k not in seen:
            seen.add(k)
            out.append(k)
    return out


def _parse_rt_price(price) -> float:
    if isinstance(price, dict):
        return float(price.get("price", price.get("close", 0)) or 0)
    if isinstance(price, list) and len(price) > 0:
        last = price[-1]
        if isinstance(last, dict):
            return float(last.get("price", 0) or 0)
        try:
            return float(last or 0)
        except (TypeError, ValueError):
            return 0.0
    try:
        return float(price or 0)
    except (TypeError, ValueError):
        return 0.0


async def _ensure_quotex_assets(client):
    try:
        if hasattr(client, "get_all_assets") and len(getattr(client, "codes_asset", {}) or {}) == 0:
            await client.get_all_assets()
    except Exception as e:
        log.debug("get_all_assets: %s", e)


async def _start_price_stream(client, asset):
    """يشترك في التيكات بدون انتظار start_realtime_price (حلقة لا نهائية في المكتبة + مهلة قصيرة كانت تُلغي الاشتراك)."""
    try:
        if not hasattr(client, "start_candles_stream"):
            return
        keys = _quotex_tick_keys(client, asset)
        k0 = keys[0]
        client.start_candles_stream(k0, 0)
        # المكتبة تُلحق التيك بـ message[0][0]؛ إن اختلف المفتاح عن المشترك يحدث KeyError ويُبتلع — نُهيئ كل المفاتيح
        api = getattr(client, "api", None)
        if api is not None:
            for k in keys:
                api.realtime_price.setdefault(k, [])
        await asyncio.sleep(0.35)
        log.info("📡 stream: %s (مفتاح التيك=%s)", asset, k0)
    except Exception as e:
        log.warning("start_candles_stream(%s): %s", asset, e)


def _append_price_tick(asset: str, p: float) -> None:
    if p <= 0:
        return
    if asset not in _price_buffers:
        _price_buffers[asset] = []
    _price_buffers[asset].append({"price": p, "time": time.time()})
    cutoff = time.time() - _PRICE_BUFFER_RETENTION_SEC
    _price_buffers[asset] = [x for x in _price_buffers[asset] if x["time"] > cutoff]


def _price_from_realtime_candles_tuple(client) -> float:
    """آخر تيك من realtime_candles (رسالة طول 4 في pyquotex)."""
    try:
        ca = getattr(client.api, "current_asset", None)
        if not ca:
            return 0.0
        rc = client.api.realtime_candles.get(ca)
        if isinstance(rc, (list, tuple)) and len(rc) >= 3:
            return float(rc[2] or 0)
    except Exception:
        pass
    return 0.0


async def _collect_price_async(client, asset) -> float:
    """يقرأ التيك من أي مفتاح مطابق (رقم أو رمز) مع انتظار قصير بين المحاولات."""
    keys = _quotex_tick_keys(client, asset)
    try:
        for _ in range(55):
            for key in keys:
                try:
                    price = await client.get_realtime_price(key)
                except Exception:
                    continue
                p = _parse_rt_price(price)
                if p > 0:
                    _append_price_tick(asset, p)
                    return p
            p2 = _price_from_realtime_candles_tuple(client)
            if p2 > 0:
                _append_price_tick(asset, p2)
                return p2
            await asyncio.sleep(0.2)
    except Exception:
        pass
    return 0

def _collect_price(client, asset, email: str = "") -> float:
    """جلب سعر حي — يجب تشغيله على نفس event loop الخاصة بجلسة Quotex (connect)."""
    try:
        if email and QX:
            p = run_async_for(email, _collect_price_async(client, asset), timeout=12)
        else:
            p = run_async(_collect_price_async(client, asset), timeout=12)
        return p or 0
    except Exception:
        return 0

def _build_candles(asset, candle_secs=5) -> list:
    if asset not in _price_buffers or len(_price_buffers[asset]) < 10:
        return []
    prices = sorted(_price_buffers[asset], key=lambda x: x["time"])
    candles, buf, t0 = [], [], prices[0]["time"] - (prices[0]["time"] % candle_secs)
    for e in prices:
        ct = e["time"] - (e["time"] % candle_secs)
        if ct == t0:
            buf.append(e["price"])
        else:
            if buf:
                candles.append({"open":buf[0],"high":max(buf),"low":min(buf),"close":buf[-1]})
            buf = [e["price"]]; t0 = ct
    if buf:
        candles.append({"open":buf[0],"high":max(buf),"low":min(buf),"close":buf[-1]})
    log.debug("📊 %s شمعة من %s سعر", len(candles), len(prices))
    return candles


def _sim_init_asset(asset: str) -> None:
    if asset in _sim_market_state:
        return
    base = 1.0 + ((abs(hash(asset)) % 9000) / 10000.0)
    _sim_market_state[asset] = {
        "price": base,
        "drift": random.uniform(-0.00003, 0.00003),
        "vol": random.uniform(0.00025, 0.0009),
    }


def _sim_next_price(asset: str, dt: float = 1.0) -> float:
    _sim_init_asset(asset)
    st = _sim_market_state[asset]
    p = float(st["price"])
    drift = float(st["drift"])
    vol = float(st["vol"])
    # Mean reversion خفيف + ضوضاء Gaussian لإعطاء سلوك سعر أقرب للواقع
    noise = random.gauss(0.0, vol) * max(dt, 0.2)
    mean_revert = (1.1 - p) * 0.00008
    p = p * (1.0 + drift + mean_revert + noise)
    p = max(0.2, min(5.0, p))
    st["price"] = p
    if random.random() < 0.03:
        st["drift"] = max(-0.00012, min(0.00012, drift + random.uniform(-0.00002, 0.00002)))
    return p


def _sim_collect_price(asset: str, ticks: int = 1) -> float:
    p = 0.0
    for _ in range(max(1, int(ticks))):
        p = _sim_next_price(asset, dt=1.0)
        _append_price_tick(asset, p)
    return p


def _sim_latest_price(asset: str) -> float:
    arr = _price_buffers.get(asset) or []
    if arr:
        try:
            return float(arr[-1]["price"])
        except Exception:
            pass
    return _sim_collect_price(asset, ticks=1)

_husaam_api_candle_cache: dict = {}  # asset -> {t, candles, source: "quotex"}
_husaam_ema10_analysis_log_ts: dict = {}  # asset -> وقت آخر log تفصيلي (تخفيف تكرار الكاش)


def _normalize_quotex_candle_row(c):
    """يحوّل عنصر شمعة من Quotex إلى {open,high,low,close,time?}."""
    if isinstance(c, dict):
        cl = float(c.get("close", 0) or 0)
        if cl <= 0:
            return None
        o = float(c.get("open", cl) or cl)
        h = float(c.get("high", cl) or cl)
        l = float(c.get("low", cl) or cl)
        row = {"open": o, "high": h, "low": l, "close": cl}
        ts = c.get("time")
        if ts is not None:
            row["time"] = float(ts)
        return row
    if isinstance(c, (list, tuple)) and len(c) >= 5:
        try:
            t0, o, cl, h, l = (float(c[0]), float(c[1]), float(c[2]), float(c[3]), float(c[4]))
        except (TypeError, ValueError):
            return None
        if cl <= 0:
            return None
        row = {"open": o, "high": h, "low": l, "close": cl, "time": t0}
        return row
    return None


def _sort_candles_by_time(rows: list) -> list:
    """أقدم → أحدث حسب time (للشموع من v2 قد يعيد السيرفر ترتيباً عشوائياً)."""
    if not rows:
        return []
    return sorted(
        rows,
        key=lambda r: float(r.get("time", 0)) if isinstance(r, dict) else 0.0,
    )


def _prepare_husaam_ema10_candles_for_analysis(rows: list, max_bars=None) -> list:
    """ترتيب زمني أقدم→أحدث، إزالة تكرار الوقت، OHLC صالحة، آخر max_bars شمعة (افتراضي 40)."""
    if not rows:
        return []
    n = max_bars if max_bars is not None else _HUSAAM_EMA10_ANALYSIS_BARS
    if n < 1:
        n = _HUSAAM_EMA10_ANALYSIS_BARS
    tmp = []
    for r in rows:
        row = _normalize_quotex_candle_row(r)
        if not row:
            continue
        o = float(row.get("open", 0) or 0)
        cl = float(row.get("close", 0) or 0)
        h = float(row.get("high", 0) or 0)
        l = float(row.get("low", 0) or 0)
        if cl <= 0:
            continue
        hi = max(o, h, cl, l)
        lo = min(o, h, cl, l)
        d = {"open": o, "high": hi, "low": lo, "close": cl}
        ts = row.get("time")
        if ts is not None:
            d["time"] = float(ts)
        tmp.append(d)
    tmp.sort(key=lambda x: x.get("time", 0.0))
    by_t = {}
    for d in tmp:
        t = d.get("time")
        if t is None:
            continue
        by_t[t] = d
    out = sorted(by_t.values(), key=lambda x: x["time"])
    return out[-n:]


async def _quotex_prepare_candles_session(client, asset: str, period: int) -> None:
    """ضبط current_asset والاشتراك في الستريم قبل get_candle_v2 / get_candles (مطلوب pyquotex)."""
    try:
        client.api.current_asset = asset
        client.start_candles_stream(asset, period)
        await asyncio.sleep(1.0)
    except Exception as e:
        log.debug("quotex_prepare_candles_session: %s", e)


def _extract_v2_chart_ohlc_candles(client, asset: str) -> list:
    """شموع 1m جاهزة من history/list/v2 في candle_v2_data['candles'] — ليست ناتج prepare_candles من التيكات."""
    api = getattr(client, "api", None)
    if api is None:
        return []
    keys = [asset]
    codes = getattr(api, "codes_asset", None) or {}
    aid = codes.get(asset)
    if aid is not None and aid not in keys:
        keys.append(aid)
    msg = None
    for k in keys:
        m = api.candle_v2_data.get(k) if hasattr(api, "candle_v2_data") else None
        if isinstance(m, dict) and m.get("candles"):
            msg = m
            break
    if not msg:
        return []
    out = []
    for c in msg.get("candles") or []:
        row = _normalize_quotex_candle_row(c)
        if row:
            out.append(row)
    return _sort_candles_by_time(out)


async def _fetch_quotex_minute_once(client, asset: str) -> list:
    """شموع 1m: أصل نصّي — ترتيب: تهيئة أصل + stream + انتظار → v2 ثم history/load (offset صغير)."""
    period = _HUSAAM_EMA10_CANDLE_SECS
    need_slots = max(_HUSAAM_EMA10_ANALYSIS_BARS, _HUSAAM_PRIVATE_MIN_BARS) + 5
    off = max(20, need_slots) * period

    try:
        if hasattr(client, "get_all_assets") and len(getattr(client, "codes_asset", {}) or {}) == 0:
            await client.get_all_assets()
    except Exception:
        pass

    _HUSAAM_WS_LAST["patched_fill"] = False
    await _quotex_prepare_candles_session(client, asset, period)
    stream_ok = getattr(getattr(client, "api", None), "current_asset", None) == asset
    cur = getattr(getattr(client, "api", None), "current_asset", None)

    best: list = []

    try:
        try:
            client.api.candle_v2_data[asset] = None
        except Exception:
            pass
        log.info(
            "📊 EMA10: طلب get_candle_v2 بعد stream | asset=%s current=%s period=%s",
            asset,
            cur,
            period,
        )
        raw_v2 = await asyncio.wait_for(client.get_candle_v2(asset, period), timeout=5.0)
    except Exception as e:
        log.debug("get_candle_v2(%s): %s", asset, e)
        raw_v2 = []
    if raw_v2:
        out_v2 = []
        for c in raw_v2:
            row = _normalize_quotex_candle_row(c)
            if row:
                out_v2.append(row)
        if len(out_v2) > len(best):
            best = out_v2
    v2_ohlc = _extract_v2_chart_ohlc_candles(client, asset)
    if len(v2_ohlc) > len(best):
        best = v2_ohlc
        log.info(
            "📊 EMA10: شموع OHLC من candle_v2_data (عدد=%s) — أوضح من تجميع التيكات في prepare_candles",
            len(best),
        )
    if len(best) >= _HUSAAM_EMA10_ANALYSIS_BARS:
        best = _sort_candles_by_time(best)
        _log_husaam_fetch_diag(asset, cur, period, need_slots, off, best, stream_ok, False, False)
        return best

    try:
        client.api.candles.candles_data = None
    except Exception:
        pass
    log.info(
        "📊 EMA10: طلب get_candles (history/load) | asset=%s offset_sec=%s period=%s stream_ok=%s",
        asset,
        off,
        period,
        stream_ok,
    )
    try:
        raw = await asyncio.wait_for(
            client.get_candles(asset, int(time.time()), off, period),
            timeout=8.0,
        )
    except asyncio.TimeoutError:
        log.debug("get_candles timeout (%s)", asset)
        raw = []
    except Exception as e:
        log.warning("get_candles(%s): %s", asset, e)
        raw = []
    if raw:
        out = []
        for c in raw:
            row = _normalize_quotex_candle_row(c)
            if row:
                out.append(row)
        if len(out) > len(best):
            best = out
    v2_ohlc2 = _extract_v2_chart_ohlc_candles(client, asset)
    if len(v2_ohlc2) > len(best):
        best = v2_ohlc2
        log.info("📊 EMA10: بعد get_candles — أخذنا %s شمعة من candle_v2_data", len(best))

    best = _sort_candles_by_time(best)
    hist_sent = True
    _log_husaam_fetch_diag(asset, cur, period, need_slots, off, best, stream_ok, hist_sent, True)
    return best


def _log_husaam_fetch_diag(
    asset: str,
    current_asset,
    period: int,
    need_slots: int,
    off_sec: int,
    best: list,
    stream_ok: bool,
    history_sent: bool,
    after_get_candles: bool,
) -> None:
    n = len(best)
    first_ts = last_ts = None
    if best:
        times = [
            float(x["time"])
            for x in best
            if isinstance(x, dict) and x.get("time") is not None
        ]
        if times:
            first_ts = int(min(times))
            last_ts = int(max(times))
    _diag_n = max(_HUSAAM_EMA10_ANALYSIS_BARS, _HUSAAM_PRIVATE_MIN_BARS)
    prep = _prepare_husaam_ema10_candles_for_analysis(best, max_bars=_diag_n) if best else []
    cleaned = len(prep)
    ok40 = cleaned >= _HUSAAM_EMA10_ANALYSIS_BARS
    ws = _HUSAAM_WS_LAST
    log.info(
        "📊 EMA10 pipeline | مطلوب=%s | current=%s | stream_ok=%s | history_sent=%s | "
        "after_ws_get_candles=%s | ws_last_hist_len=%s | ws_patched=%s | "
        "offset_sec=%s period=%s | raw_rows=%s | cleaned=%s | 40/40=%s | first_ts=%s last_ts=%s",
        asset,
        current_asset,
        stream_ok,
        history_sent,
        after_get_candles,
        ws.get("last_history_len"),
        ws.get("patched_fill"),
        off_sec,
        period,
        n,
        cleaned,
        ok40,
        first_ts,
        last_ts,
    )


async def _fetch_quotex_minute_candles_async(client, asset: str) -> list:
    """محاولة ثانية خفيفة إذا بقي العدد أقل من 40 شمعة خام."""
    best = await _fetch_quotex_minute_once(client, asset)
    if len(best) >= _HUSAAM_EMA10_ANALYSIS_BARS:
        return best
    await asyncio.sleep(1.5)
    try:
        if hasattr(client, "get_all_assets"):
            await client.get_all_assets()
    except Exception:
        pass
    best2 = await _fetch_quotex_minute_once(client, asset)
    if len(best2) > len(best):
        best = best2
    return best


def _get_husaam_ema10_candles(client, email: str, asset: str, need_len: int, analysis_bars=None) -> tuple:
    """يعيد (شموع للتحليل, مصدر). التحليل من آخر N شمعة 1m من Quotex بعد التنظيف — لا تيكات."""
    tgt = analysis_bars if analysis_bars is not None else _HUSAAM_EMA10_ANALYSIS_BARS
    if not QX or not client:
        return _build_candles(asset, candle_secs=_HUSAAM_EMA10_CANDLE_SECS), "sim_tick_1m"
    now = time.time()
    ce = _husaam_api_candle_cache.get(asset)

    def _finalize(raw: list, label: str) -> tuple:
        prep = _prepare_husaam_ema10_candles_for_analysis(raw, max_bars=tgt)
        if len(prep) < tgt:
            return [], label + "_insufficient"
        out = prep[-tgt:] if len(prep) > tgt else prep
        cur = getattr(getattr(client, "api", None), "current_asset", None)
        t0 = out[0].get("time")
        t1 = out[-1].get("time")
        _tl = time.time()
        _throttle = label == "quotex_1m_cache" and (_tl - _husaam_ema10_analysis_log_ts.get(asset, 0) < 30)
        if not _throttle:
            log.info(
                "📊 EMA10 analysis: asset=%s current_asset=%s period=60 need=%s got=%s first_ts=%s last_ts=%s src=%s",
                asset,
                cur,
                tgt,
                len(out),
                int(t0) if t0 is not None else None,
                int(t1) if t1 is not None else None,
                label,
            )
            _husaam_ema10_analysis_log_ts[asset] = _tl
        return out, label

    if (
        ce
        and ce.get("source") == "quotex"
        and (now - ce["t"]) < _HUSAAM_QUOTEX_CACHE_SEC
    ):
        prep0 = _prepare_husaam_ema10_candles_for_analysis(
            ce.get("candles", []), max_bars=tgt
        )
        if len(prep0) >= tgt:
            return _finalize(ce["candles"], "quotex_1m_cache")

    try:
        lst = run_async_for(email, _fetch_quotex_minute_candles_async(client, asset), _HUSAAM_QUOTEX_FETCH_TIMEOUT_SEC)
    except Exception as e:
        log.warning("جلب شموع Quotex: %s", e)
        lst = []

    if lst:
        _husaam_api_candle_cache[asset] = {"t": now, "candles": lst, "source": "quotex"}
        got, lab = _finalize(lst, "quotex_1m")
        if got:
            return got, lab

    if ce and ce.get("candles"):
        log.warning("⚠️ fallback: كاش شموع قديم لـ %s (أقل من %s بعد التنظيف قد يفشل)", asset, tgt)
        got, lab = _finalize(ce["candles"], "quotex_stale_cache")
        if got:
            return got, lab

    log.warning(
        "⚠️ بيانات غير كافية لجلب شموع 1m — لا توجد %s شمعة دقيقة من الشارت لـ %s (بعد التنظيف)",
        tgt,
        asset,
    )
    return [], "quotex_insufficient"

# ══════════════════════════════════════════════════════════════════════════════
# Quotex Operations
# ══════════════════════════════════════════════════════════════════════════════
def _extract_balance(p):
    real = demo = 0.0
    try:
        if isinstance(p, dict):
            real = float(p.get("live_balance", p.get("liveBalance", p.get("real_balance",0))) or 0)
            demo = float(p.get("demo_balance", p.get("demoBalance", p.get("practice_balance",0))) or 0)
        else:
            for a in ["live_balance","liveBalance","real_balance"]:
                v = getattr(p,a,None)
                if v is not None: real=float(v or 0); break
            for a in ["demo_balance","demoBalance","practice_balance"]:
                v = getattr(p,a,None)
                if v is not None: demo=float(v or 0); break
    except: pass
    return real, demo

def _extract_currency(p):
    try:
        if isinstance(p, dict): return p.get("currency", p.get("currency_char","USD"))
        for a in ["currency","currency_char"]:
            v = getattr(p,a,None)
            if v: return str(v)
    except: pass
    return "USD"

async def _get_balances(client):
    real = demo = 0.0
    try:
        p = await client.get_profile()
        log.info(f"📋 profile type={type(p).__name__}")
        real, demo = _extract_balance(p)
        if real > 0 or demo > 0: return real, demo
    except Exception as e:
        log.warning(f"get_profile: {e}")
    try:
        await client.change_account("REAL")
        await asyncio.sleep(3)
        real = float(await client.get_balance() or 0)
        await asyncio.sleep(2)
        await client.change_account("PRACTICE")
        await asyncio.sleep(3)
        demo = float(await client.get_balance() or 0)
        if real == demo and real > 0: real = 0.0
        return real, demo
    except Exception as e:
        log.error(f"_get_balances: {e}")
        return 0.0, 0.0

async def _get_single_balance(client, mode) -> float:
    try:
        await client.change_account(mode)
        await asyncio.sleep(1)
        return float(await client.get_balance() or 0)
    except: return 0.0

async def _login_qx(email, password, S):
    try:
        client = Quotex(email=email, password=password, lang="en")
        S["client"] = client
        import builtins
        orig = builtins.input
        builtins.input = make_pin_input(email, S)
        try: check, msg = await client.connect()
        finally: builtins.input = orig
        if not check:
            if S["needs_pin"]:
                return {"ok":False,"pin":True,"msg":"أدخل PIN من بريدك"}
            return {"ok":False,"pin":False,"msg":str(msg) or "فشل الاتصال"}
        real, demo = await _get_balances(client)
        cur = "USD"
        try:
            p = await client.get_profile()
            cur = _extract_currency(p)
        except: pass
        return {"ok":True,"client":client,"real":real,"demo":demo,"cur":cur}
    except Exception as e:
        log.error(traceback.format_exc())
        return {"ok":False,"pin":False,"msg":str(e)}


async def _pw_fill_first(page, selectors, value: str, timeout_ms: int = 6000) -> bool:
    for sel in selectors:
        try:
            loc = page.locator(sel).first
            if await loc.count() > 0:
                await loc.fill(value, timeout=timeout_ms)
                return True
        except Exception:
            pass
    return False


async def _pw_click_first(page, selectors, timeout_ms: int = 6000) -> bool:
    for sel in selectors:
        try:
            loc = page.locator(sel).first
            if await loc.count() > 0:
                await loc.click(timeout=timeout_ms)
                return True
        except Exception:
            pass
    return False


async def _login_qx_browser(email: str, password: str):
    """
    المرحلة 1 (Browser-based):
    - تسجيل دخول Quotex عبر متصفح حقيقي (Playwright)
    - حفظ storage_state لكل مستخدم
    """
    if not PLAYWRIGHT_OK:
        return {"ok": False, "msg": "Playwright غير مثبت على الخادم"}
    os.makedirs(os.path.join("data", "browser_state"), exist_ok=True)
    state_file = os.path.join("data", "browser_state", f"{email.replace('@', '_at_')}.json")
    browser = context = page = None
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
            if os.path.exists(state_file):
                context = await browser.new_context(storage_state=state_file)
            else:
                context = await browser.new_context()
            page = await context.new_page()
            await page.goto("https://qxbroker.com/en/sign-in", wait_until="networkidle", timeout=90000)
            await asyncio.sleep(1.5)

            # إذا كانت الجلسة محفوظة ومفعّلة، قد يتم التحويل تلقائياً إلى صفحة التداول.
            if "/trade" not in (page.url or ""):
                email_ok = await _pw_fill_first(
                    page,
                    [
                        "input[type='email']",
                        "input[name='email']",
                        "input[autocomplete='email']",
                        "input[placeholder*='mail' i]",
                        "input[id*='email' i]",
                    ],
                    email,
                    timeout_ms=12000,
                )
                pass_ok = await _pw_fill_first(
                    page,
                    [
                        "input[type='password']",
                        "input[name='password']",
                        "input[autocomplete='current-password']",
                        "input[id*='pass' i]",
                    ],
                    password,
                    timeout_ms=12000,
                )
                if not email_ok or not pass_ok:
                    body = ""
                    try:
                        body = (await page.inner_text("body"))[:500]
                    except Exception:
                        pass
                    return {
                        "ok": False,
                        "msg": "Browser login: تعذر العثور على حقول تسجيل الدخول (email/password). "
                               f"url={page.url} body={body}",
                    }

                click_ok = await _pw_click_first(
                    page,
                    [
                        "button[type='submit']",
                        "button:has-text('Sign in')",
                        "button:has-text('Log in')",
                        "button:has-text('Login')",
                        "button:has-text('تسجيل')",
                    ],
                    timeout_ms=12000,
                )
                if not click_ok:
                    # fallback: Enter من حقل كلمة المرور
                    try:
                        await page.keyboard.press("Enter")
                    except Exception:
                        pass
            try:
                await page.wait_for_url("**/trade/**", timeout=60000)
            except PwTimeoutError:
                txt = ""
                try:
                    txt = (await page.inner_text("body"))[:400]
                except Exception:
                    pass
                return {"ok": False, "msg": f"Browser login لم يكتمل. {txt}"}
            await context.storage_state(path=state_file)
            bal = await _browser_fetch_balances(state_file)
            return {
                "ok": True,
                "cur": "USD",
                "real": bal.get("real", 0.0),
                "demo": bal.get("demo", 10_000.0),
                "state_file": state_file,
            }
    except Exception as e:
        return {"ok": False, "msg": f"Browser login error: {e}"}
    finally:
        try:
            if context:
                await context.close()
        except Exception:
            pass
        try:
            if browser:
                await browser.close()
        except Exception:
            pass


def _parse_num_text(s: str) -> float:
    if not s:
        return 0.0
    t = s.replace(",", "").replace("USD", "").replace("$", "").strip()
    m = re.search(r"-?\d+(?:\.\d+)?", t)
    if not m:
        return 0.0
    try:
        return float(m.group(0))
    except Exception:
        return 0.0


async def _browser_fetch_balances(state_file: str) -> dict:
    if not PLAYWRIGHT_OK:
        return {"real": 0.0, "demo": 10_000.0}
    browser = context = page = None
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
            context = await browser.new_context(storage_state=state_file)
            page = await context.new_page()
            await page.goto("https://qxbroker.com/en/trade", wait_until="domcontentloaded", timeout=90000)
            await asyncio.sleep(2.0)
            txt = await page.inner_text("body")
            nums = [float(x) for x in re.findall(r"\d+(?:\.\d+)?", txt or "")]
            if not nums:
                return {"real": 0.0, "demo": 10_000.0}
            # عادة يظهر أول رقمين كأرصدة الحسابين.
            n1 = nums[0] if len(nums) > 0 else 0.0
            n2 = nums[1] if len(nums) > 1 else n1
            real = min(n1, n2)
            demo = max(n1, n2)
            if demo <= 0:
                demo = 10_000.0
            return {"real": real, "demo": demo}
    except Exception:
        return {"real": 0.0, "demo": 10_000.0}
    finally:
        try:
            if context:
                await context.close()
        except Exception:
            pass
        try:
            if browser:
                await browser.close()
        except Exception:
            pass


async def _browser_trade_action(state_file: str, asset: str, amount: float, direction: str, acc: str) -> dict:
    """
    تنفيذ صفقة عبر متصفح حقيقي (مرحلة 2 مبسطة):
    - يفتح trade page بجلسة محفوظة
    - يضبط مبلغ الصفقة
    - يضغط CALL/PUT
    - يعيد snapshot للرصيد قبل الصفقة
    """
    if not PLAYWRIGHT_OK:
        return {"ok": False, "msg": "Playwright غير متوفر"}
    browser = context = page = None
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
            context = await browser.new_context(storage_state=state_file)
            page = await context.new_page()
            await page.goto("https://qxbroker.com/en/trade", wait_until="domcontentloaded", timeout=90000)
            await asyncio.sleep(2.0)

            # مبلغ الصفقة: محاولة على أول input رقمي ظاهر
            inputs = page.locator("input")
            cnt = await inputs.count()
            for i in range(min(cnt, 12)):
                el = inputs.nth(i)
                try:
                    typ = (await el.get_attribute("type")) or ""
                    im = (await el.get_attribute("inputmode")) or ""
                    if typ in ("number", "text") or im in ("numeric", "decimal"):
                        await el.click(timeout=1000)
                        await el.fill(str(float(amount)), timeout=1000)
                        break
                except Exception:
                    pass

            # نقرأ الرصيد التقريبي قبل الصفقة
            body = await page.inner_text("body")
            nums = [float(x) for x in re.findall(r"\d+(?:\.\d+)?", body or "")]
            bal_before = nums[0] if nums else 0.0

            # زر الاتجاه
            dir_tokens = ["call", "up", "buy", "شراء", "higher"] if direction == "call" else ["put", "down", "sell", "بيع", "lower"]
            clicked = False
            buttons = page.locator("button")
            bcnt = await buttons.count()
            for i in range(min(bcnt, 40)):
                b = buttons.nth(i)
                try:
                    t = ((await b.inner_text(timeout=300)) or "").lower()
                    if any(tok in t for tok in dir_tokens):
                        await b.click(timeout=1000)
                        clicked = True
                        break
                except Exception:
                    pass
            if not clicked:
                return {"ok": False, "msg": "تعذر إيجاد زر CALL/PUT في الواجهة"}

            tid = int(time.time() * 1000)
            _BROWSER_TICKETS[tid] = {
                "state_file": state_file,
                "balance_before": bal_before,
                "amount": float(amount),
                "direction": direction,
                "asset": asset,
                "account_type": acc,
                "created_at": time.time(),
            }
            return {"ok": True, "id": tid}
    except Exception as e:
        return {"ok": False, "msg": f"Browser trade error: {e}"}
    finally:
        try:
            if context:
                await context.close()
        except Exception:
            pass
        try:
            if browser:
                await browser.close()
        except Exception:
            pass


async def _browser_check_result(tid: int) -> dict:
    meta = _BROWSER_TICKETS.get(tid)
    if not meta:
        return {"win": False, "profit": 0.0}
    bal = await _browser_fetch_balances(meta["state_file"])
    after = float(bal.get("demo", 0.0) if meta.get("account_type") != "real" else bal.get("real", 0.0))
    before = float(meta.get("balance_before", 0.0))
    amount = float(meta.get("amount", 0.0))
    if before <= 0 or after <= 0:
        # fallback محافظ إذا تعذر قراءة الرصيد
        return {"win": False, "profit": -amount if amount > 0 else 0.0}
    diff = round(after - before, 2)
    if abs(diff) < 0.01:
        return {"win": False, "profit": -amount if amount > 0 else 0.0}
    return {"win": diff > 0, "profit": diff}


def _should_fallback_to_sim(msg: str) -> bool:
    m = (msg or "").lower()
    keys = [
        "service unavailable",
        "not available in your region",
        "region",
        "cloudflare",
        "websocket",
        "connection rejected",
        "connection was rejected",
        "handshake status 403",
        "403 forbidden",
        "cf-mitigated",
        "challenge",
        "just a moment",
        "ws2.qxbroker.com",
    ]
    return any(k in m for k in keys)

async def _do_trade(client, asset, amount, direction, acc):
    try:
        await client.change_account("REAL" if acc=="real" else "PRACTICE")
        await asyncio.sleep(0.5)
        ok, info = await client.buy(amount=amount, asset=asset,
                                    direction=direction, duration=60)
        if ok:
            tid = info.get("id") if isinstance(info,dict) else None
            log.info(f"✅ صفقة: {asset} {direction.upper()} {amount} id={tid}")
            return {"ok":True,"id":tid}
        log.warning(f"⚠️ رُفضت: {info}")
        return {"ok":False,"msg":str(info)}
    except Exception as e:
        log.error(traceback.format_exc())
        return {"ok":False,"msg":str(e)}

async def _check_win(client, tid):
    try:
        win = await client.check_win(tid)
        prf = client.get_profit() if hasattr(client,"get_profit") else 0
        return {"win":bool(win),"profit":float(prf or 0)}
    except: return {"win":False,"profit":0.0}

async def _close(client):
    try:
        if client: await client.close()
    except: pass

# ══════════════════════════════════════════════════════════════════════════════
# BOT WORKER
# ══════════════════════════════════════════════════════════════════════════════
def wait_for_minute_start(stop):
    """
    ينتظر حتى بداية الدقيقة التالية بالضبط
    يستخدم time.sleep للدقة العالية
    """
    now   = datetime.now()
    secs  = now.second
    usecs = now.microsecond / 1_000_000
    remaining = 60.0 - secs - usecs
    if remaining < 0:
        remaining = 0.0
    log.info(f"⏳ انتظار {remaining:.2f}ث لبداية الدقيقة (الثانية {secs})")
    # انتظار بخطوات صغيرة للتحقق من stop
    end_time = time.time() + remaining
    while time.time() < end_time:
        if stop.is_set(): return
        left = end_time - time.time()
        time.sleep(min(0.5, left) if left > 0 else 0)

def bot_worker(req: BotReq, S: dict, stop: threading.Event):
    browser_live = bool(BROWSER_QX_MODE and S.get("browser_state_file"))
    # قائمة الأزواج — دعم متعدد الأزواج
    all_assets = list(req.assets) if req.assets else [req.asset]
    if req.asset and req.asset not in all_assets:
        all_assets.insert(0, req.asset)
    all_assets = list(dict.fromkeys(all_assets))  # إزالة المكررات
    log.info(f"🤖 {S['email']} | {all_assets} | {req.amount} | {req.strategy} | {req.account_type}")

    # تتبع آخر زوج استُخدم لتجنب التكرار
    last_used_asset = None

    # ── بدء تدفق الأسعار الحية لكل الأزواج ───────────────────────────────────
    if QX and S["client"]:
        try:
            run_async_for(S["email"], _ensure_quotex_assets(S["client"]), 45)
        except Exception:
            pass
        for a in all_assets:
            try:
                run_async_for(S["email"], _start_price_stream(S["client"], a), 20)
            except Exception:
                pass

    # ── جمع تيكات للمراقبة فقط — التحليل من شموع 1m من Quotex لجميع الاستراتيجيات عند الاتصال
    _wu = (
        _WARMUP_COLLECT_SEC_EMA10
        if QX and S["client"]
        else _WARMUP_COLLECT_SEC
    )
    S["status_msg"] = "⏳ تجهيز المراقبة (أسعار حية للواجهة)..."
    log.info("⏳ جمع أسعار حية للمراقبة — %s ثانية...", _wu)
    warmup = time.time()
    while time.time() - warmup < _wu and not stop.is_set():
        if QX and S["client"]:
            for a in all_assets:
                _collect_price(S["client"], a, S["email"])
        else:
            for a in all_assets:
                _sim_collect_price(a, ticks=1)
        stop.wait(timeout=1)
    if stop.is_set(): return

    _tick_total = sum(len(_price_buffers.get(a, [])) for a in all_assets)
    log.info("✅ مراقبة: %s تيك محفوظ — بدء الحلقة (%s)", _tick_total, req.strategy)
    S["status_msg"] = ""

    # ── رصيد البداية — قراءة بعد warmup مباشرة ──────────────────────────────
    start_bal = 0.0
    if QX and S["client"]:
        try:
            mode = "REAL" if req.account_type=="real" else "PRACTICE"
            log.info("⏳ قراءة رصيد البداية...")
            # قراءة متعددة حتى نحصل على قيمة ثابتة متكررة
            readings = []
            prev = 0.0
            for i in range(8):
                try:
                    bal = run_async_for(S["email"], _get_single_balance(S["client"],mode), 10)
                    if bal > 0:
                        readings.append(bal)
                        # إذا تكررت نفس القيمة مرتين متتاليتين فهي صحيحة
                        if len(readings) >= 2 and readings[-1] == readings[-2]:
                            start_bal = readings[-1]
                            break
                except: pass
                time.sleep(1)
            if start_bal == 0 and readings:
                start_bal = readings[-1]
            log.info(f"💰 رصيد البداية: {start_bal} (قراءات: {readings})")
            if req.account_type=="real": S["real_balance"] = start_bal
            else: S["demo_balance"] = start_bal
        except Exception as e:
            log.error(f"خطأ رصيد البداية: {e}")
            start_bal = S["demo_balance"] if req.account_type=="demo" else S["real_balance"]
    elif browser_live:
        bal = run_async_for(S["email"], _browser_fetch_balances(S["browser_state_file"]), 40)
        if req.account_type == "real":
            start_bal = float(bal.get("real", S.get("real_balance", 0.0)) or 0.0)
            S["real_balance"] = start_bal
        else:
            start_bal = float(bal.get("demo", S.get("demo_balance", 0.0)) or 0.0)
            S["demo_balance"] = start_bal
    else:
        start_bal = S["demo_balance"] if req.account_type=="demo" else S["real_balance"]
    S["start_balance"] = start_bal
    log.info(f"🏁 رصيد البداية المعتمد: {start_bal}")


    pending_double_count = 0
    pending_double_asset = ""
    pending_double_dir = "wait"

    while not stop.is_set():
        try:
            # ── جمع الأسعار الحية ──────────────────────────────────────────
            if QX and S["client"]:
                _collect_price(S["client"], req.asset, S["email"])

            # جمع سعر إضافي
            if QX and S["client"]:
                _collect_price(S["client"], req.asset, S["email"])

            # ── تحليل الشمعات ─────────────────────────────────────────────
            direction = "wait"
            chosen_asset = all_assets[0]
            forced_double = False
            burst_count = 1
            any_candles = False

            if pending_double_count > 0 and pending_double_asset:
                forced_double = True
                direction = pending_double_dir if pending_double_dir in ("call", "put") else S.get("last_signal", "wait")
                chosen_asset = pending_double_asset
                burst_count = pending_double_count
                pending_double_count = 0
                log.info(f"♻️ مضاعفة: تنفيذ {burst_count} صفقات مباشرة على {chosen_asset} باتجاه {direction.upper()}")
                S["status_msg"] = "المضاعفة مفعلة: تنفيذ صفقة إضافية بعد خسارة"
            elif QX and S["logged_in"] and S["client"]:
                # جمع أسعار لكل الأزواج
                for a in all_assets:
                    _collect_price(S["client"], a, S["email"])

                # تحليل كل الأزواج واختيار الأقوى إشارة
                best_score = 0
                best_dir   = "wait"
                best_asset = None
                any_candles = False
                candles_by_asset = {}

                _min_bars = (
                    _HUSAAM_PRIVATE_MIN_BARS
                    if req.strategy == "HUSAAM_PRIVATE"
                    else _HUSAAM_EMA10_ANALYSIS_BARS
                )
                _need_len = _min_bars
                _max_len = 0
                ema_src_label = ""
                for a in all_assets:
                    if req.strategy == "HUSAAM_PRIVATE":
                        candles, _csrc = _get_husaam_ema10_candles(
                            S["client"],
                            S["email"],
                            a,
                            _need_len,
                            analysis_bars=_HUSAAM_PRIVATE_MIN_BARS,
                        )
                        if _csrc in ("quotex_1m", "quotex_1m_cache"):
                            ema_src_label = "شموع Quotex 1m — حسام الخاصة"
                        elif _csrc == "quotex_stale_cache":
                            ema_src_label = "شموع 1m — كاش احتياطي"
                        elif _csrc == "sim_tick_1m":
                            ema_src_label = "محاكاة (تيك)"
                        else:
                            ema_src_label = str(_csrc)
                    else:
                        candles, _csrc = _get_husaam_ema10_candles(
                            S["client"], S["email"], a, _need_len
                        )
                        if _csrc in ("quotex_1m", "quotex_1m_cache"):
                            ema_src_label = "شموع Quotex 1m (شارت)"
                        elif _csrc == "quotex_stale_cache":
                            ema_src_label = "شموع 1m — كاش احتياطي"
                        elif _csrc == "sim_tick_1m":
                            ema_src_label = "محاكاة (تيك)"
                        else:
                            ema_src_label = str(_csrc)
                    _max_len = max(_max_len, len(candles))
                    candles_by_asset[a] = candles
                    if len(candles) >= _need_len:
                        any_candles = True
                        d, score = analyze_score(candles, req.strategy)
                        _sig = f"{a}:{d}:{score}"
                        _tn = time.time()
                        if _tn - S.get("_last_scan_sig_ts", 0) >= 12 or S.get("_last_scan_sig") != _sig:
                            log.info(f"🔍 {a}: {d.upper()} score={score}")
                            S["_last_scan_sig_ts"] = _tn
                            S["_last_scan_sig"] = _sig
                        if d != "wait" and score > best_score:
                            # تجنب تكرار نفس الزوج إذا وجد بديل بنفس القوة
                            if score > best_score or a != last_used_asset:
                                best_score = score
                                best_dir   = d
                                best_asset = a

                S["candles_ok"] = any_candles
                S["candle_source"] = ema_src_label

                if best_asset and best_dir != "wait":
                    direction    = best_dir
                    chosen_asset = best_asset
                    S["_wait_streak"] = 0
                    log.info(f"🏆 أفضل زوج: {chosen_asset} → {direction.upper()} (score={best_score})")
                elif not any_candles:
                    _now = time.time()
                    if _now - S.get("_last_candle_warn_ts", 0) >= 15:
                        log.warning(
                            "⚠️ شمعات غير كافية للتحليل — لديك %s/%s شمعة 1m (مطلوب من الشارت لا من التيك)",
                            _max_len,
                            _need_len,
                        )
                        S["_last_candle_warn_ts"] = _now
                    S["status_msg"] = (
                        f"⏳ شموع دقيقة: {_max_len}/{_need_len} · {ema_src_label}"
                    )
                    S["_wait_streak"] = 0
                    stop.wait(timeout=1.0)
                    continue
                else:
                    S["_wait_streak"] = int(S.get("_wait_streak", 0)) + 1
                    if S["_wait_streak"] >= 12:
                        fb_best_score = 0
                        fb_best_dir = "wait"
                        fb_best_asset = None
                        for a, cs in candles_by_asset.items():
                            d2, s2 = _fallback_direction_from_candles(cs)
                            if d2 != "wait" and s2 >= fb_best_score:
                                fb_best_score = s2
                                fb_best_dir = d2
                                fb_best_asset = a
                        if fb_best_asset and fb_best_dir != "wait":
                            direction = fb_best_dir
                            chosen_asset = fb_best_asset
                            S["_wait_streak"] = 0
                            S["status_msg"] = "تم تفعيل دخول بديل بعد تكرار WAIT"
                            log.info(f"⚡ Fallback entry: {chosen_asset} → {direction.upper()} (score={fb_best_score})")
                        else:
                            _now = time.time()
                            if _now - S.get("_last_no_trade_signal_ts", 0) >= 20:
                                log.info("⏸️ شموع كافية — لا إشارة دخول (WAIT) حسب الاستراتيجية في أي زوج")
                                S["_last_no_trade_signal_ts"] = _now
                    else:
                        _now = time.time()
                        if _now - S.get("_last_no_trade_signal_ts", 0) >= 20:
                            log.info("⏸️ شموع كافية — لا إشارة دخول (WAIT) حسب الاستراتيجية في أي زوج")
                            S["_last_no_trade_signal_ts"] = _now
            elif (not QX) and S["logged_in"]:
                # محاكاة احترافية: توليد تيكات + شموع 1m ثم تحليل بنفس الاستراتيجية.
                for a in all_assets:
                    _sim_collect_price(a, ticks=1)
                # في المحاكاة نستخدم شموع أسرع (5 ثوانٍ) لبدء التداول بسرعة مع الحفاظ على المنطق.
                _need_len = 20
                best_score = 0
                best_dir = "wait"
                best_asset = None
                max_len = 0
                candles_by_asset = {}
                for a in all_assets:
                    cs = _build_candles(a, candle_secs=5)
                    candles_by_asset[a] = cs
                    max_len = max(max_len, len(cs))
                    if len(cs) < _need_len:
                        continue
                    any_candles = True
                    if len(cs) >= 35:
                        d, score = analyze_score(cs, req.strategy)
                        if d != "wait" and score >= best_score:
                            best_score = score
                            best_dir = d
                            best_asset = a
                S["candles_ok"] = any_candles
                S["candle_source"] = "محاكاة داخلية (شموع 1m)"
                if best_asset and best_dir != "wait":
                    direction = best_dir
                    chosen_asset = best_asset
                    S["status_msg"] = ""
                    log.info("🧪 SIM best: %s -> %s (score=%s)", chosen_asset, direction.upper(), best_score)
                elif not any_candles:
                    S["status_msg"] = f"⏳ تجهيز محاكاة الشموع: {max_len}/{_need_len}"
                    stop.wait(timeout=1.0)
                    continue
                else:
                    fb_dir = "wait"
                    fb_asset = None
                    fb_score = 0
                    for a, cs in candles_by_asset.items():
                        d2, s2 = _fallback_direction_from_candles(cs)
                        if d2 != "wait" and s2 >= fb_score:
                            fb_dir, fb_asset, fb_score = d2, a, s2
                    if fb_asset:
                        direction = fb_dir
                        chosen_asset = fb_asset
                        S["status_msg"] = "تم تفعيل دخول بديل (محاكاة)"
                        log.info("🧪 SIM fallback: %s -> %s", chosen_asset, direction.upper())
            else:
                log.warning("⚠️ لا اتصال — تخطّ")
                stop.wait(timeout=1.0)
                continue

            if direction == "wait":
                # any_candles=True: شموع كافية لكن الاستراتيجية أعطت WAIT — ليس «نقص شموع»
                if any_candles:
                    wt = 4.0
                    stop.wait(timeout=wt)
                else:
                    log.info("⏸️ انتظار (لا بيانات تحليل كافية)")
                    stop.wait(timeout=1.0)
                continue
            else:
                skips = 0

            last_used_asset  = chosen_asset
            S["last_signal"] = direction

            # ── توقيت الإرسال
            # في وضع المضاعفة بعد الخسارة: تنفيذ فوري (بدون انتظار الدقيقة)
            if not forced_double and QX and not browser_live:
                wait_for_minute_start(stop)
                if stop.is_set(): break
            else:
                if stop.is_set(): break

            log.info(f"🎯 {direction.upper()} | {datetime.now().strftime('%H:%M:%S')}")

            # ── تسجيل/تنفيذ الصفقات (Burst) ────────────────────────────────
            opened_trades = []
            for i in range(max(1, int(burst_count))):
                trade = {
                    "id":         int(time.time()*1000) + i,
                    "asset":      chosen_asset,
                    "direction":  direction,
                    "amount":     req.amount,
                    "duration":   60,
                    "status":     "pending",
                    "profit":     0,
                    "started_at": datetime.now().isoformat(),
                    "account":    req.account_type,
                    "strategy":   req.strategy,
                }
                S["current_trade"] = trade
                tid = None
                if QX and S["logged_in"] and S["client"]:
                    r = run_async_for(S["email"], _do_trade(S["client"],chosen_asset,req.amount,
                                            direction,req.account_type), 20)
                    if r["ok"]:
                        tid = r.get("id")
                    else:
                        msg = str(r.get("msg",""))
                        log.warning(f"⚠️ {msg}")
                        if "not_money" in msg.lower():
                            log.warning("💸 رصيد غير كافٍ — توقف")
                            S["current_trade"] = None
                            stop.set()
                            break
                        continue
                elif browser_live:
                    r = run_async_for(
                        S["email"],
                        _browser_trade_action(
                            S["browser_state_file"], chosen_asset, req.amount, direction, req.account_type
                        ),
                        60,
                    )
                    if r["ok"]:
                        tid = r.get("id")
                    else:
                        msg = str(r.get("msg",""))
                        log.warning(f"⚠️ {msg}")
                        if "not_money" in msg.lower():
                            log.warning("💸 رصيد غير كافٍ — توقف")
                            S["current_trade"] = None
                            stop.set()
                            break
                        continue
                else:
                    trade["entry_price"] = _sim_latest_price(chosen_asset)
                    log.info(f"🔵 محاكاة {chosen_asset} {direction.upper()} ({i+1}/{max(1, int(burst_count))})")
                opened_trades.append((trade, tid))

            if stop.is_set():
                break
            if not opened_trades:
                S["current_trade"] = None
                stop.wait(timeout=2.0)
                continue

            # ── انتظار 60 ثانية كاملة ─────────────────────────────────────
            log.info("⏳ انتظار 60 ثانية...")
            # جمع أسعار أثناء الانتظار
            trade_end = time.time() + 60
            while time.time() < trade_end and not stop.is_set():
                if QX and S["client"]:
                    for a in all_assets:
                        _collect_price(S["client"], a, S["email"])
                else:
                    for a in all_assets:
                        _sim_collect_price(a, ticks=1)
                stop.wait(timeout=1)
            if stop.is_set(): break

            # ── النتائج لكل صفقة في الـ Burst ─────────────────────────────
            total_prf = 0.0
            any_loss = False
            for trade, tid in opened_trades:
                if QX and tid and S["client"]:
                    r2  = run_async_for(S["email"], _check_win(S["client"],tid), 15)
                    win = r2["win"]
                    prf = r2["profit"] if win else -(r2.get("profit",0) or req.amount)
                elif browser_live and tid:
                    r2 = run_async_for(S["email"], _browser_check_result(tid), 40)
                    win = bool(r2.get("win"))
                    prf = float(r2.get("profit", 0.0) or 0.0)
                else:
                    ep = float(trade.get("entry_price", 0) or 0)
                    xp = _sim_latest_price(trade.get("asset", chosen_asset))
                    if ep <= 0:
                        ep = xp
                    diff = xp - ep
                    if trade.get("direction") == "call":
                        win = diff > 0
                    else:
                        win = diff < 0
                    if abs(diff) < max(ep * 0.00008, 1e-8):
                        win = False
                    prf = round(req.amount * _SIM_PAYOUT, 2) if win else -req.amount
                    trade["entry_price"] = round(ep, 6)
                    trade["exit_price"] = round(xp, 6)

                prf = round(prf,2)
                total_prf += prf
                any_loss = any_loss or (not win)
                trade.update({"status":"win" if win else "loss",
                              "profit":prf,"ended_at":datetime.now().isoformat()})
                S["trades"].insert(0, dict(trade))
                if len(S["trades"]) > 200: S["trades"].pop()

                if win: S["wins"]+=1;   log.info(f"✅ فوز +{prf}")
                else:   S["losses"]+=1; log.info(f"❌ خسارة {prf}")

            if (len(opened_trades) == 1) and any_loss and req.double_on_loss:
                pending_double_count = 2
                pending_double_asset = chosen_asset
                pending_double_dir = direction
                log.info(f"🧮 المضاعفة: بعد الخسارة سيتم تنفيذ صفقتين فوراً على {chosen_asset}")

            S["current_trade"] = None

            # ── قراءة الرصيد الفعلي من المنصة ──────────────────────────
            current_bal = 0.0
            if QX and S["client"]:
                mode = "REAL" if req.account_type=="real" else "PRACTICE"
                prev_bal = S["real_balance"] if req.account_type=="real" else S["demo_balance"]
                # انتظار حتى يتغير الرصيد فعلاً في المنصة
                log.info(f"⏳ انتظار تحديث الرصيد (الرصيد قبل: {prev_bal})")
                for wait_attempt in range(20):
                    time.sleep(3)
                    try:
                        bal = run_async_for(S["email"], _get_single_balance(S["client"],mode), 12)
                        if bal > 0 and bal != prev_bal:
                            current_bal = bal
                            log.info(f"💰 رصيد تحدّث (محاولة {wait_attempt+1}): {prev_bal} → {current_bal}")
                            break
                        elif bal > 0:
                            log.info(f"⏳ رصيد لم يتغير بعد: {bal} (محاولة {wait_attempt+1})")
                            current_bal = bal
                    except: pass
                if current_bal > 0:
                    if req.account_type=="real": S["real_balance"] = current_bal
                    else: S["demo_balance"] = current_bal
                else:
                    current_bal = prev_bal
                    log.warning("⚠️ لم يتم تحديث الرصيد")
            elif browser_live:
                bal = run_async_for(S["email"], _browser_fetch_balances(S["browser_state_file"]), 40)
                if req.account_type == "real":
                    current_bal = float(bal.get("real", S.get("real_balance", 0.0)) or 0.0)
                    if current_bal > 0:
                        S["real_balance"] = current_bal
                else:
                    current_bal = float(bal.get("demo", S.get("demo_balance", 0.0)) or 0.0)
                    if current_bal > 0:
                        S["demo_balance"] = current_bal
                if current_bal <= 0:
                    current_bal = S["demo_balance"] if req.account_type=="demo" else S["real_balance"]
            else:
                # محاكاة
                if req.account_type == "demo":
                    S["demo_balance"] = round((S.get("demo_balance") or start_bal) + total_prf, 2)
                    current_bal = S["demo_balance"]
                else:
                    S["real_balance"] = round((S.get("real_balance") or start_bal) + total_prf, 2)
                    current_bal = S["real_balance"]

            # الربح الحقيقي = رصيد المنصة الحالي - رصيد البداية فقط
            real_profit = round(current_bal - start_bal, 2)
            S["session_profit"] = real_profit
            log.info(f"💰 رصيد={current_bal} | بداية={start_bal} | ربح={real_profit:+.2f}")

            # ── فحص الحدود ────────────────────────────────────────────────
            if req.profit_limit > 0 and real_profit >= req.profit_limit:
                msg = f"🎯 وصلت لهدف الربح! +{real_profit} {S['currency']} (الهدف: +{req.profit_limit})"
                log.info(msg)
                S["status_msg"] = msg
                S["running"] = False
                stop.set(); break

            if req.stop_loss > 0 and real_profit <= -req.stop_loss:
                msg = f"🛑 تم الوصول لحد الخسارة! {real_profit} {S['currency']} (الحد: -{req.stop_loss})"
                log.info(msg)
                S["status_msg"] = msg
                S["running"] = False
                stop.set(); break

            # صفقة كل دقيقة — لا انتظار إضافي

        except Exception as e:
            log.error(f"خطأ:\n{traceback.format_exc()}")
            stop.wait(timeout=6)

    S["running"] = False
    S["current_trade"] = None
    log.info(f"🤖 توقف: {S['email']}")

# ══════════════════════════════════════════════════════════════════════════════
# FastAPI
# ══════════════════════════════════════════════════════════════════════════════
app = FastAPI(title="abood trader")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])


@app.on_event("startup")
async def _startup_quiet_logs():
    _configure_quiet_loggers()

_fe = os.path.join(os.path.dirname(__file__), "frontend.html")
_ad = os.path.join(os.path.dirname(__file__), "admin.html")
HTML  = open(_fe,encoding="utf-8").read() if os.path.exists(_fe) else "<h1>frontend.html مفقود</h1>"
ADMIN = open(_ad,encoding="utf-8").read() if os.path.exists(_ad) else "<h1>admin.html مفقود</h1>"


@app.get("/", response_class=HTMLResponse)
async def ui(): return HTML

@app.get("/admin", response_class=HTMLResponse)
async def admin_ui(): return ADMIN

# ── Admin ─────────────────────────────────────────────────────────────────────
@app.post("/api/admin/login")
async def admin_login(req: AdminLoginReq):
    if req.password != ADMIN_PW: raise HTTPException(401,"كلمة مرور خاطئة")
    t = secrets.token_hex(16)
    _persist_admin_token(t)
    return {"success": True, "token": t}

@app.get("/api/admin/users")
async def admin_users(admin_token: str):
    if admin_token not in ADMIN_TOKENS: raise HTTPException(403,"غير مصرح")
    result = []
    for email,u in USERS.items():
        S = get_session_by_email(email)
        result.append({
            "email":email,"name":u.get("name",""),"status":u["status"],
            "registered_at":u.get("registered_at",""),"approved_at":u.get("approved_at",""),
            "note":u.get("note",""),"online":bool(S and S["logged_in"]),
            "running":bool(S and S["running"]),"wins":S["wins"] if S else 0,
            "losses":S["losses"] if S else 0,"session_profit":S["session_profit"] if S else 0,
            "real_balance":S["real_balance"] if S else 0,"demo_balance":S["demo_balance"] if S else 0,
            "candles_ok":S.get("candles_ok",False) if S else False,
            "candle_source":S.get("candle_source","") if S else "",
        })
    return {"users":result}

@app.post("/api/admin/approve")
async def admin_approve(req: ApproveReq):
    if req.admin_token not in ADMIN_TOKENS: raise HTTPException(403,"غير مصرح")
    if req.email not in USERS: raise HTTPException(404,"مستخدم غير موجود")
    if req.action == "approve":
        token = secrets.token_hex(20)
        USERS[req.email].update({"status":"approved","session_token":token,
            "approved_at":datetime.now().isoformat(),"note":req.note})
        SESSIONS[token] = new_session(req.email)
        save_users()
        log.info(f"✅ قبول: {req.email}")
        return {"success":True,"message":f"تم قبول {req.email}","token":token}
    elif req.action == "reject":
        old = USERS[req.email].get("session_token","")
        if old in SESSIONS: SESSIONS[old]["stop_event"].set(); del SESSIONS[old]
        USERS[req.email].update({"status":"rejected","note":req.note})
        save_users()
        return {"success":True,"message":f"تم رفض {req.email}"}
    raise HTTPException(400,"action خاطئ")

@app.post("/api/admin/stop_user")
async def admin_stop(req: ApproveReq):
    if req.admin_token not in ADMIN_TOKENS: raise HTTPException(403,"غير مصرح")
    S = get_session_by_email(req.email)
    if S: S["stop_event"].set(); S["running"]=False; return {"success":True}
    return {"success":False,"message":"غير متصل"}

@app.post("/api/admin/delete_user")
async def admin_delete(req: ApproveReq):
    if req.admin_token not in ADMIN_TOKENS: raise HTTPException(403,"غير مصرح")
    if req.email not in USERS: raise HTTPException(404,"مستخدم غير موجود")
    # إيقاف الجلسة إن وجدت
    S = get_session_by_email(req.email)
    if S:
        S["stop_event"].set()
        S["running"] = False
        if S.get("client"):
            try: run_async_for(req.email, _close(S["client"]), 5)
            except: pass
    # حذف من SESSIONS
    token = USERS[req.email].get("session_token","")
    if token and token in SESSIONS:
        del SESSIONS[token]
    # حذف من USERS
    del USERS[req.email]
    save_users()
    log.info(f"🗑️ حُذف المشترك: {req.email}")
    return {"success":True,"message":f"تم حذف {req.email}"}

# ── User ──────────────────────────────────────────────────────────────────────
@app.post("/api/register")
async def register(req: RegisterReq):
    if "@" not in req.email: raise HTTPException(400,"بريد غير صحيح")
    if req.email in USERS:
        s = USERS[req.email]["status"]
        msgs = {"approved":"حسابك مفعّل — سجّل دخولك",
                "pending":"طلبك قيد المراجعة","rejected":"تم رفض طلبك"}
        return {"success":False,"status":s,"message":msgs.get(s,"")}
    USERS[req.email] = {"name":req.name,"status":"pending",
        "registered_at":datetime.now().isoformat(),"session_token":"","approved_at":"","note":""}
    save_users()
    log.info(f"📝 طلب: {req.email}")
    return {"success":True,"status":"pending","message":"تم إرسال طلبك — انتظر موافقة المسؤول"}

@app.get("/api/check_status")
async def check_status(email: str):
    if email not in USERS: raise HTTPException(404,"البريد غير مسجل")
    u = USERS[email]
    r = {"status":u["status"],"email":email}
    if u["status"] == "approved":
        token = u.get("session_token","")
        if token and token not in SESSIONS:
            SESSIONS[token] = new_session(email)
        r["session_token"] = token
        r["message"] = "تم قبول طلبك — سجّل دخولك"
    elif u["status"] == "pending":
        r["message"] = "طلبك قيد المراجعة"
    else:
        r["message"] = "تم رفض طلبك: " + u.get("note","")
    return r

@app.post("/api/login")
async def login(req: LoginReq):
    if "@" not in req.email: raise HTTPException(400,"بريد غير صحيح")
    if len(req.password)<4:  raise HTTPException(400,"كلمة مرور قصيرة")
    if req.email not in USERS: raise HTTPException(403,"غير مسجّل — أرسل طلب انضمام")
    if USERS[req.email]["status"] != "approved":
        raise HTTPException(403,"لم يتم قبول طلبك بعد")
    if USERS[req.email].get("session_token","") != req.token:
        raise HTTPException(403,"انتهت الجلسة — تحقق من الحالة مرة أخرى")
    S = get_session(req.token)
    if not S:
        SESSIONS[req.token] = new_session(req.email)
        S = SESSIONS[req.token]
    if QX:
        if S.get("client"):
            try: run_async_for(req.email, _close(S["client"]),5)
            except: pass
        S["email"] = req.email
        try:
            if BROWSER_QX_MODE:
                # تجنب 504 من الـ reverse proxy: نعيد رد واضح خلال أقل من دقيقة.
                r = run_async_for(req.email, _login_qx_browser(req.email, req.password), 50)
            else:
                r = run_async_for(req.email, _login_qx(req.email,req.password,S), 150)
        except TimeoutError:
            raise HTTPException(408, "انتهت مهلة تسجيل الدخول. حاول مرة أخرى")
        if r.get("pin"):
            S["needs_pin"]=True
            return {"success":False,"needs_pin":True,"message":r["msg"]}
        if not r["ok"]:
            msg = str(r.get("msg","فشل"))
            # إذا كانت الخدمة محجوبة حسب المنطقة، اسمح بالدخول على وضع المحاكاة
            # بدل إرجاع 401 يوقف استخدام الواجهة بالكامل.
            blocked_region = _should_fallback_to_sim(msg)
            if blocked_region:
                S["client"] = None
                S["real_balance"] = 0.0
                S["demo_balance"] = 10_000.0
                S["currency"] = "USD"
                S["logged_in"] = True
                S["needs_pin"] = False
                S["email"] = req.email
                S["status_msg"] = "تم التحويل لوضع المحاكاة لأن اتصال Quotex مرفوض (Cloudflare/WebSocket)"
                return {"success":True,"needs_pin":False,"email":req.email,
                        "user_id":abs(hash(req.email))%90_000_000+10_000_000,
                        "real_balance":S["real_balance"],"demo_balance":S["demo_balance"],
                        "currency":S["currency"],"sim_mode":True}
            raise HTTPException(401, msg)
        S["client"]=r.get("client")
        S["browser_state_file"] = r.get("state_file", "")
        S["real_balance"]=r["real"]
        S["demo_balance"]=r["demo"]; S["currency"]=r.get("cur","USD")
    else:
        S["real_balance"]=0.0; S["demo_balance"]=10_000.0; S["currency"]="USD"
    S["logged_in"]=True; S["needs_pin"]=False; S["email"]=req.email
    S["status_msg"] = ""
    sim_mode = _is_sim_mode_session(S)
    return {"success":True,"needs_pin":False,"email":req.email,
            "user_id":abs(hash(req.email))%90_000_000+10_000_000,
            "real_balance":S["real_balance"],"demo_balance":S["demo_balance"],
            "currency":S["currency"],"sim_mode":sim_mode}

@app.post("/api/pin")
async def pin_ep(req: PinReq):
    S = get_session(req.token)
    if not S: raise HTTPException(403,"جلسة غير صالحة")
    p = req.pin.strip()
    if len(p)<4: raise HTTPException(400,"PIN قصير")
    if not QX:
        S["logged_in"]=True; S["needs_pin"]=False
        return {"success":True,"email":S["email"],"user_id":12345678,
                "real_balance":0.0,"demo_balance":10_000.0,"currency":"USD","sim_mode":True}
    get_pin_q(S["email"]).put(p)
    for _ in range(30):
        await asyncio.sleep(1)
        if S["logged_in"]:
            sim_mode = _is_sim_mode_session(S)
            return {"success":True,"email":S["email"],
                    "user_id":abs(hash(S["email"]))%90_000_000+10_000_000,
                    "real_balance":S["real_balance"],"demo_balance":S["demo_balance"],
                    "currency":S["currency"],"sim_mode":sim_mode}
        if not S["needs_pin"] and not S["logged_in"]:
            raise HTTPException(401,"PIN خاطئ")
    raise HTTPException(408,"انتهت المهلة")

@app.post("/api/logout")
async def logout(req: TokenReq):
    S = get_session(req.token)
    if S:
        if S.get("running"): S["stop_event"].set(); S["running"]=False
        if S.get("client"):
            try: run_async_for(S.get("email","_"), _close(S["client"]),5)
            except: pass
        S.update({"logged_in":False,"needs_pin":False,"trades":[],"wins":0,
                  "losses":0,"session_profit":0.0,"current_trade":None,"client":None,
                  "browser_state_file": ""})
    return {"success":True}

@app.post("/api/bot/start")
async def start(req: BotReq):
    S = get_session(req.token)
    if not S: raise HTTPException(403,"جلسة غير صالحة")
    if not S["logged_in"]: raise HTTPException(401,"سجّل الدخول أولاً")
    if BROWSER_QX_MODE and not S.get("client") and not S.get("browser_state_file"):
        raise HTTPException(400, "Browser mode: لا يوجد state محفوظ. أعد تسجيل الدخول أولاً.")
    if S["running"]: raise HTTPException(400,"البوت يعمل بالفعل")
    # ── إعادة تعيين كاملة لكل جلسة جديدة ──────────────────────────────────
    # أوقف أي thread قديم أولاً
    old_stop = S.get("stop_event")
    if old_stop:
        old_stop.set()
    time.sleep(0.8)  # انتظر حتى يتوقف thread القديم

    # إعادة تعيين كاملة مع الحفاظ على client
    S["running"]        = True
    S["stop_event"]     = threading.Event()
    S["session_profit"] = 0.0
    S["start_balance"]  = 0.0
    S["wins"]           = 0
    S["losses"]         = 0
    S["trades"]         = []
    S["status_msg"]     = ""   # امسح رسالة الهدف القديمة
    S["current_trade"]  = None
    S["candles_ok"]     = False
    S["candle_source"]  = ""
    threading.Thread(target=bot_worker, args=(req,S,S["stop_event"]),
                     daemon=True).start()
    return {"success":True}

@app.post("/api/bot/stop")
async def stop_ep(req: TokenReq):
    S = get_session(req.token)
    if S:
        S["stop_event"].set()
        S["running"] = False
        S["status_msg"] = ""
        S["candle_source"] = ""
    return {"success":True}

@app.get("/api/status")
async def status(token: str=""):
    S = get_session(token)
    if not S:
        sim_mode = _is_sim_mode_session(None)
        return {
            "logged_in": False,
            "running": False,
            "needs_pin": False,
            "sim_mode": sim_mode,
            "status_msg": "وضع المحاكاة مفعل" if sim_mode else "",
        }
    total = S["wins"]+S["losses"]
    sim_mode = _is_sim_mode_session(S)
    return {
        "logged_in":      S["logged_in"],
        "needs_pin":      S["needs_pin"],
        "email":          S["email"],
        "real_balance":   S["real_balance"],
        "demo_balance":   S["demo_balance"],
        "currency":       S["currency"],
        "running":        S["running"],
        "wins":           S["wins"],
        "losses":         S["losses"],
        "total":          total,
        "win_rate":       round(S["wins"]/total*100,1) if total else 0,
        "session_profit": S["session_profit"],
        "start_balance":  S["start_balance"],
        "current_trade":  S["current_trade"],
        "trades":         S["trades"][:50],
        "sim_mode":       sim_mode,
        "last_signal":    S["last_signal"],
        "candles_ok":     S.get("candles_ok",False),
        "candle_source":  S.get("candle_source",""),
        "status_msg":     S.get("status_msg",""),
    }

if __name__ == "__main__":
    log.info("🚀 abood trader — http://localhost:8000")
    log.info(f"👤 Admin: http://localhost:8000/admin | Password: {ADMIN_PW}")
    log.info(f"📦 pyquotex: {'✅' if QX else '❌ محاكاة'}")
    if FORCE_SIM_MODE:
        log.info("🧪 FORCE_SIM_MODE=ON (تم تعطيل اتصال Quotex الحقيقي)")
    if BROWSER_QX_MODE:
        log.info("🌐 BROWSER_QX_MODE=ON (تداول Quotex عبر Playwright - مرحلة 2)")
    if QUOTEX_USE_PLAYWRIGHT_BRIDGE:
        log.info("🌉 QUOTEX_USE_PLAYWRIGHT_BRIDGE=ON | port=%s | proxy=%s",
                 QUOTEX_WS_BRIDGE_PORT,
                 "set" if QUOTEX_PROXY_URL else "none")
    log.info(f"📦 pydantic : v{pydantic.VERSION}")
    uvicorn.run(app, host="0.0.0.0", port=8000, access_log=False)
