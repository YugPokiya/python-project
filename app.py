"""ProjAlpha — system monitor + XAU/XAG markets, Hyperliquid crypto & gold news + perps."""

from __future__ import annotations

import json
import os
import platform
import time
import xml.etree.ElementTree as ET
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from html import unescape
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import psutil
from flask import Flask, jsonify, render_template
from flask_socketio import SocketIO

app = Flask(__name__)
app.config["SECRET_KEY"] = "projalpha-monitor"
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

HISTORY_LEN = 60
cpu_history: deque[float] = deque(maxlen=HISTORY_LEN)
mem_history: deque[float] = deque(maxlen=HISTORY_LEN)
net_sent_history: deque[float] = deque(maxlen=HISTORY_LEN)
net_recv_history: deque[float] = deque(maxlen=HISTORY_LEN)
xau_history: deque[float] = deque(maxlen=HISTORY_LEN)
xag_history: deque[float] = deque(maxlen=HISTORY_LEN)

CRYPTO_COINS = ("BTC", "ETH", "HYPE", "SOL")
CRYPTO_NAMES = {"BTC": "Bitcoin", "ETH": "Ethereum", "HYPE": "Hyperliquid", "SOL": "Solana"}
crypto_histories: dict[str, deque[float]] = {
    coin: deque(maxlen=HISTORY_LEN) for coin in CRYPTO_COINS
}

_prev_net = psutil.net_io_counters()
_prev_net_ts = time.monotonic()
_boot_time = psutil.boot_time()

_metals_cache: dict = {"xau": None, "xag": None, "ratio": None, "updated_at": None, "error": None}
_crypto_cache: dict = {"coins": {}, "updated_at": None, "error": None, "network": "mainnet"}
_news_cache: dict = {"items": [], "updated_at": None, "error": None}
_perps_cache: dict = {"assets": {}, "updated_at": None, "error": None}
_deribit_instruments: dict[str, dict] = {}
_last_metals_fetch = 0.0
_last_crypto_fetch = 0.0
_last_news_fetch = 0.0
_last_perps_fetch = 0.0
METALS_TTL = 30
CRYPTO_TTL = 2
NEWS_TTL = 300
PERPS_TTL = 8
INSTRUMENTS_TTL = 300

PERP_COINS = ("BTC", "ETH")
PERP_NAMES = {"BTC": "Bitcoin", "ETH": "Ethereum"}
perp_histories: dict[str, deque[float]] = {
    coin: deque(maxlen=HISTORY_LEN) for coin in PERP_COINS
}

USER_AGENT = "ProjAlpha/1.0 (+local dashboard)"
HL_MAINNET_INFO = "https://api.hyperliquid.xyz/info"
HL_TESTNET_INFO = "https://api.hyperliquid-testnet.xyz/info"
DERIBIT_API = "https://www.deribit.com/api/v2"


def _hl_info_url() -> tuple[str, str]:
    """Return (url, network_label). Set HL_TESTNET=1 to use testnet."""
    if os.environ.get("HL_TESTNET", "").strip().lower() in {"1", "true", "yes"}:
        return HL_TESTNET_INFO, "testnet"
    return HL_MAINNET_INFO, "mainnet"


def _price_digits(price: float) -> int:
    if price >= 10000:
        return 1
    if price >= 100:
        return 2
    if price >= 1:
        return 3
    return 4


def _bytes_to_gb(n: int) -> float:
    return round(n / (1024**3), 2)


def _rate_mbps(delta_bytes: int, elapsed: float) -> float:
    if elapsed <= 0:
        return 0.0
    return round((delta_bytes * 8) / elapsed / 1_000_000, 2)


def _http_json(url: str, timeout: float = 8.0, payload: dict | None = None) -> dict:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    if payload is not None:
        headers["Content-Type"] = "application/json"
    req = Request(url, data=data, headers=headers, method="GET" if payload is None else "POST")
    with urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _http_text(url: str, timeout: float = 10.0) -> str:
    req = Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/rss+xml, application/xml, text/xml"})
    with urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="replace")


def fetch_crypto(force: bool = False) -> dict:
    """Live Hyperliquid mid prices for BTC / ETH / HYPE / SOL (+ simple session analysis)."""
    global _last_crypto_fetch, _crypto_cache

    now = time.monotonic()
    if not force and _crypto_cache["coins"] and (now - _last_crypto_fetch) < CRYPTO_TTL:
        return _crypto_cache

    info_url, network = _hl_info_url()
    try:
        mids = _http_json(info_url, timeout=8.0, payload={"type": "allMids"})
        if not isinstance(mids, dict):
            raise ValueError("Unexpected allMids payload")

        coins: dict = {}
        for symbol in CRYPTO_COINS:
            raw = mids.get(symbol)
            if raw is None:
                continue
            price = float(raw)
            hist = crypto_histories[symbol]
            prev = hist[-1] if hist else price
            hist.append(price)
            digits = _price_digits(price)
            change = price - prev
            change_pct = ((price - prev) / prev) * 100 if prev else 0.0
            # Lightweight custom analysis: distance from sparkline window high/low
            window = list(hist)
            hi = max(window)
            lo = min(window)
            mid_span = hi - lo
            vs_high_pct = ((price - hi) / hi) * 100 if hi else 0.0
            vs_low_pct = ((price - lo) / lo) * 100 if lo else 0.0
            range_pos = ((price - lo) / mid_span) if mid_span > 0 else 0.5

            coins[symbol.lower()] = {
                "symbol": symbol,
                "name": CRYPTO_NAMES[symbol],
                "price": round(price, digits),
                "digits": digits,
                "change": round(change, digits + 1),
                "change_pct": round(change_pct, 4),
                "history": window,
                "analysis": {
                    "window_high": round(hi, digits),
                    "window_low": round(lo, digits),
                    "vs_high_pct": round(vs_high_pct, 3),
                    "vs_low_pct": round(vs_low_pct, 3),
                    "range_position": round(range_pos, 3),  # 0=low … 1=high of recent window
                },
            }

        if not coins:
            raise ValueError("No BTC/ETH/HYPE/SOL mids returned")

        _crypto_cache = {
            "coins": coins,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "error": None,
            "network": network,
            "source": "hyperliquid",
        }
        _last_crypto_fetch = now
    except (URLError, TimeoutError, KeyError, ValueError, OSError, TypeError) as exc:
        _crypto_cache["error"] = str(exc)
        _crypto_cache["network"] = network
        if not _crypto_cache.get("coins"):
            _crypto_cache["updated_at"] = datetime.now(timezone.utc).isoformat()

    return _crypto_cache


def _http_get_json(url: str, timeout: float = 12.0) -> dict:
    req = Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
    with urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _deribit(path: str, params: dict | None = None, timeout: float = 12.0) -> dict:
    query = ""
    if params:
        query = "?" + "&".join(f"{k}={v}" for k, v in params.items())
    payload = _http_get_json(f"{DERIBIT_API}{path}{query}", timeout=timeout)
    if "error" in payload and payload["error"]:
        raise ValueError(str(payload["error"]))
    return payload.get("result", payload)


def _expiry_label(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%d %b %Y").upper()


def _get_option_instruments(currency: str) -> list[dict]:
    """Cached Deribit option instrument list per currency."""
    now = time.monotonic()
    cached = _deribit_instruments.get(currency)
    if cached and (now - cached["fetched_at"]) < INSTRUMENTS_TTL:
        return cached["items"]

    items = _deribit(
        "/public/get_instruments",
        {"currency": currency, "kind": "option", "expired": "false"},
        timeout=20.0,
    )
    if not isinstance(items, list):
        raise ValueError(f"Unexpected instruments payload for {currency}")
    _deribit_instruments[currency] = {"items": items, "fetched_at": now}
    return items


def _ticker_greeks(instrument_name: str) -> dict:
    t = _deribit("/public/ticker", {"instrument_name": instrument_name}, timeout=10.0)
    g = t.get("greeks") or {}
    return {
        "instrument": instrument_name,
        "mark_price": float(t.get("mark_price") or 0),
        "mark_iv": round(float(t.get("mark_iv") or 0), 2),
        "underlying_price": float(t.get("underlying_price") or 0),
        "open_interest": float(t.get("open_interest") or 0),
        "delta": round(float(g.get("delta") or 0), 5),
        "gamma": round(float(g.get("gamma") or 0), 6),
        "vega": round(float(g.get("vega") or 0), 4),
        "theta": round(float(g.get("theta") or 0), 4),
        "rho": round(float(g.get("rho") or 0), 4),
    }


def _atm_pair(options: list[dict], index_price: float) -> tuple[dict, dict]:
    calls = [o for o in options if o.get("option_type") == "call"]
    puts = [o for o in options if o.get("option_type") == "put"]
    if not calls or not puts:
        raise ValueError("Missing call/put strikes")
    atm_call = min(calls, key=lambda o: abs(float(o["strike"]) - index_price))
    atm_put = min(puts, key=lambda o: abs(float(o["strike"]) - index_price))
    return atm_call, atm_put


def _aggregate_market_greeks(options: list[dict], index_price: float, max_instruments: int = 40) -> dict:
    """OI-weighted Greeks around ATM for one expiry (liquid strikes)."""
    with_strike = [o for o in options if o.get("strike") is not None]
    ranked = sorted(with_strike, key=lambda o: abs(float(o["strike"]) - index_price))[:max_instruments]
    names = [o["instrument_name"] for o in ranked]

    agg = {
        "call_delta": 0.0,
        "put_delta": 0.0,
        "net_delta": 0.0,
        "gamma": 0.0,
        "vega": 0.0,
        "theta": 0.0,
        "open_interest": 0.0,
        "instruments": 0,
    }

    with ThreadPoolExecutor(max_workers=16) as pool:
        futures = {pool.submit(_ticker_greeks, name): name for name in names}
        for fut in as_completed(futures):
            try:
                t = fut.result()
            except (URLError, HTTPError, TimeoutError, ValueError, OSError, TypeError, KeyError):
                continue
            oi = float(t["open_interest"])
            if oi <= 0:
                continue
            agg["open_interest"] += oi
            agg["gamma"] += oi * float(t["gamma"])
            agg["vega"] += oi * float(t["vega"])
            agg["theta"] += oi * float(t["theta"])
            agg["instruments"] += 1
            if t["instrument"].endswith("-C"):
                agg["call_delta"] += oi * float(t["delta"])
            else:
                agg["put_delta"] += oi * float(t["delta"])

    agg["net_delta"] = agg["call_delta"] + agg["put_delta"]
    for key in ("call_delta", "put_delta", "net_delta", "gamma", "vega", "theta", "open_interest"):
        agg[key] = round(agg[key], 4)
    return agg


def _fetch_hl_perp_ctxs() -> tuple[dict[str, dict], str]:
    info_url, network = _hl_info_url()
    payload = _http_json(info_url, timeout=8.0, payload={"type": "metaAndAssetCtxs"})
    if not isinstance(payload, list) or len(payload) < 2:
        raise ValueError("Unexpected metaAndAssetCtxs payload")
    universe = payload[0].get("universe") or []
    ctxs = payload[1]
    by_coin: dict[str, dict] = {}
    for i, meta in enumerate(universe):
        name = meta.get("name")
        if name in PERP_COINS and i < len(ctxs):
            by_coin[name] = ctxs[i]
    if not by_coin:
        raise ValueError("BTC/ETH perp contexts missing")
    return by_coin, network


def _build_asset_greeks(currency: str, index_price: float) -> dict:
    instruments = _get_option_instruments(currency)
    by_exp: dict[int, list[dict]] = {}
    for ins in instruments:
        by_exp.setdefault(int(ins["expiration_timestamp"]), []).append(ins)

    expiries = sorted(by_exp.keys())
    # Prefer liquid near-term: skip expiries within ~6h if a later one exists
    now_ms = int(time.time() * 1000)
    usable = [e for e in expiries if e - now_ms > 6 * 3600 * 1000] or expiries
    chosen = usable[:2]

    atm_rows = []
    for exp in chosen:
        call_ins, put_ins = _atm_pair(by_exp[exp], index_price)
        call = _ticker_greeks(call_ins["instrument_name"])
        put = _ticker_greeks(put_ins["instrument_name"])
        call["strike"] = float(call_ins["strike"])
        put["strike"] = float(put_ins["strike"])
        atm_rows.append({
            "expiry_ts": exp,
            "expiry": _expiry_label(exp),
            "call": call,
            "put": put,
            "avg_delta": round((call["delta"] + put["delta"]) / 2, 5),
            "avg_gamma": round((call["gamma"] + put["gamma"]) / 2, 6),
            "avg_vega": round((call["vega"] + put["vega"]) / 2, 4),
        })

    # Market Greeks on the first chosen expiry (around ATM)
    market = _aggregate_market_greeks(by_exp[chosen[0]], index_price)
    market["expiry"] = _expiry_label(chosen[0])
    market["expiry_ts"] = chosen[0]

    return {"atm": atm_rows, "market_greeks": market}


def fetch_perps(force: bool = False) -> dict:
    """Hyperliquid BTC/ETH perps + Deribit ATM / market Greeks (delta, gamma, vega)."""
    global _last_perps_fetch, _perps_cache

    now = time.monotonic()
    if not force and _perps_cache.get("assets") and (now - _last_perps_fetch) < PERPS_TTL:
        return _perps_cache

    try:
        hl_ctxs, network = _fetch_hl_perp_ctxs()
        assets: dict = {}

        for symbol in PERP_COINS:
            ctx = hl_ctxs.get(symbol)
            if not ctx:
                continue
            mark = float(ctx.get("markPx") or ctx.get("midPx") or 0)
            mid = float(ctx.get("midPx") or mark)
            oracle = float(ctx.get("oraclePx") or mark)
            funding = float(ctx.get("funding") or 0)
            premium = float(ctx.get("premium") or 0)
            oi = float(ctx.get("openInterest") or 0)
            day_vol = float(ctx.get("dayNtlVlm") or 0)
            digits = _price_digits(mark)

            hist = perp_histories[symbol]
            prev = hist[-1] if hist else mark
            hist.append(mark)
            change = mark - prev
            change_pct = ((mark - prev) / prev) * 100 if prev else 0.0

            index_payload = _deribit(
                "/public/get_index_price",
                {"index_name": f"{symbol.lower()}_usd"},
            )
            index_price = float(index_payload.get("index_price") or mark)
            greeks_block = _build_asset_greeks(symbol, index_price)

            # Prefer ATM call Greeks for the headline (Δ≈0.5 at ATM; Γ/ν match put)
            front = greeks_block["atm"][0] if greeks_block["atm"] else None
            headline = {
                "delta": front["call"]["delta"] if front else None,
                "gamma": front["call"]["gamma"] if front else None,
                "vega": front["call"]["vega"] if front else None,
                "theta": front["call"]["theta"] if front else None,
                "mark_iv": round((front["call"]["mark_iv"] + front["put"]["mark_iv"]) / 2, 2) if front else None,
                "expiry": front["expiry"] if front else None,
                "strike": front["call"]["strike"] if front else None,
            }

            assets[symbol.lower()] = {
                "symbol": symbol,
                "name": PERP_NAMES[symbol],
                "perp": {
                    "mark": round(mark, digits),
                    "mid": round(mid, digits),
                    "oracle": round(oracle, digits),
                    "funding": funding,
                    "funding_pct": round(funding * 100, 6),
                    "funding_apr_pct": round(funding * 24 * 365 * 100, 2),
                    "premium": premium,
                    "premium_pct": round(premium * 100, 4),
                    "open_interest": round(oi, 4),
                    "day_volume_usd": round(day_vol, 2),
                    "change": round(change, digits + 1),
                    "change_pct": round(change_pct, 4),
                    "digits": digits,
                    "history": list(hist),
                    "source": "hyperliquid",
                },
                "index": round(index_price, digits),
                "greeks": headline,
                "atm": greeks_block["atm"],
                "market_greeks": greeks_block["market_greeks"],
                "greeks_source": "deribit",
            }

        if not assets:
            raise ValueError("No BTC/ETH perp assets built")

        _perps_cache = {
            "assets": assets,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "error": None,
            "network": network,
            "sources": {"perp": "hyperliquid", "greeks": "deribit"},
        }
        _last_perps_fetch = now
    except (URLError, HTTPError, TimeoutError, KeyError, ValueError, OSError, TypeError) as exc:
        _perps_cache["error"] = str(exc)
        if not _perps_cache.get("assets"):
            _perps_cache["updated_at"] = datetime.now(timezone.utc).isoformat()

    return _perps_cache


def fetch_metals(force: bool = False) -> dict:
    global _last_metals_fetch, _metals_cache

    now = time.monotonic()
    if not force and _metals_cache["xau"] and (now - _last_metals_fetch) < METALS_TTL:
        return _metals_cache

    try:
        xau = _http_json("https://api.gold-api.com/price/XAU")
        xag = _http_json("https://api.gold-api.com/price/XAG")
        xau_price = float(xau["price"])
        xag_price = float(xag["price"])
        ratio = round(xau_price / xag_price, 2) if xag_price else None

        xau_history.append(xau_price)
        xag_history.append(xag_price)

        prev_xau = _metals_cache["xau"]["price"] if _metals_cache.get("xau") else xau_price
        prev_xag = _metals_cache["xag"]["price"] if _metals_cache.get("xag") else xag_price

        _metals_cache = {
            "xau": {
                "symbol": "XAU",
                "name": "Gold",
                "price": round(xau_price, 2),
                "currency": xau.get("currency", "USD"),
                "change": round(xau_price - prev_xau, 2),
                "change_pct": round(((xau_price - prev_xau) / prev_xau) * 100, 3) if prev_xau else 0.0,
                "updated_at": xau.get("updatedAt"),
                "history": list(xau_history),
            },
            "xag": {
                "symbol": "XAG",
                "name": "Silver",
                "price": round(xag_price, 3),
                "currency": xag.get("currency", "USD"),
                "change": round(xag_price - prev_xag, 3),
                "change_pct": round(((xag_price - prev_xag) / prev_xag) * 100, 3) if prev_xag else 0.0,
                "updated_at": xag.get("updatedAt"),
                "history": list(xag_history),
            },
            "ratio": ratio,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "error": None,
            "source": "gold-api.com",
        }
        _last_metals_fetch = now
    except (URLError, TimeoutError, KeyError, ValueError, OSError) as exc:
        _metals_cache["error"] = str(exc)
        if not _metals_cache.get("xau"):
            _metals_cache["updated_at"] = datetime.now(timezone.utc).isoformat()

    return _metals_cache


def _clean_title(title: str) -> tuple[str, str]:
    title = unescape(title.strip())
    source = ""
    if " - " in title:
        title, source = title.rsplit(" - ", 1)
    return title.strip(), source.strip()


def fetch_gold_news(force: bool = False) -> dict:
    global _last_news_fetch, _news_cache

    now = time.monotonic()
    if not force and _news_cache["items"] and (now - _last_news_fetch) < NEWS_TTL:
        return _news_cache

    feed_url = (
        "https://news.google.com/rss/search?"
        "q=gold+price+OR+XAUUSD+OR+bullion&hl=en-US&gl=US&ceid=US:en"
    )
    try:
        xml_text = _http_text(feed_url)
        root = ET.fromstring(xml_text)
        items = []
        for item in root.findall("./channel/item")[:8]:
            raw_title = item.findtext("title") or ""
            title, source_from_title = _clean_title(raw_title)
            source_el = item.find("source")
            source = (source_el.text if source_el is not None and source_el.text else source_from_title) or "News"
            link = item.findtext("link") or ""
            pub = item.findtext("pubDate") or ""
            published = pub
            try:
                published = parsedate_to_datetime(pub).astimezone(timezone.utc).isoformat()
            except (TypeError, ValueError, IndexError):
                pass
            items.append({
                "title": title,
                "source": source,
                "url": link,
                "published": published,
            })

        _news_cache = {
            "items": items,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "error": None,
        }
        _last_news_fetch = now
    except (URLError, TimeoutError, ET.ParseError, OSError) as exc:
        _news_cache["error"] = str(exc)
        if not _news_cache["items"]:
            _news_cache["updated_at"] = datetime.now(timezone.utc).isoformat()

    return _news_cache


def collect_metrics() -> dict:
    global _prev_net, _prev_net_ts

    cpu_percent = psutil.cpu_percent(interval=None)
    mem = psutil.virtual_memory()
    disk = psutil.disk_usage("/")
    net = psutil.net_io_counters()
    now = time.monotonic()
    elapsed = now - _prev_net_ts

    sent_rate = _rate_mbps(net.bytes_sent - _prev_net.bytes_sent, elapsed)
    recv_rate = _rate_mbps(net.bytes_recv - _prev_net.bytes_recv, elapsed)
    _prev_net = net
    _prev_net_ts = now

    cpu_history.append(cpu_percent)
    mem_history.append(mem.percent)
    net_sent_history.append(sent_rate)
    net_recv_history.append(recv_rate)

    load_avg = psutil.getloadavg() if hasattr(psutil, "getloadavg") else (0.0, 0.0, 0.0)
    procs = len(psutil.pids())
    uptime_s = int(time.time() - _boot_time)

    services = [
        {"name": "API Gateway", "status": "healthy", "latency_ms": round(2 + cpu_percent * 0.15, 1)},
        {"name": "Worker Pool", "status": "healthy" if cpu_percent < 90 else "degraded", "latency_ms": round(5 + cpu_percent * 0.2, 1)},
        {"name": "Cache Layer", "status": "healthy" if mem.percent < 90 else "degraded", "latency_ms": round(1 + mem.percent * 0.05, 1)},
        {"name": "Object Store", "status": "healthy" if disk.percent < 90 else "degraded", "latency_ms": round(8 + disk.percent * 0.1, 1)},
        {"name": "Metals Feed", "status": "healthy" if not _metals_cache.get("error") else "degraded", "latency_ms": 12.0},
        {"name": "Hyperliquid", "status": "healthy" if not _crypto_cache.get("error") else "degraded", "latency_ms": 8.0},
        {"name": "News Feed", "status": "healthy" if not _news_cache.get("error") else "degraded", "latency_ms": 40.0},
    ]

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "host": {
            "hostname": platform.node(),
            "os": f"{platform.system()} {platform.release()}",
            "arch": platform.machine(),
            "cpus": psutil.cpu_count(logical=True) or 1,
            "uptime_seconds": uptime_s,
        },
        "cpu": {
            "percent": round(cpu_percent, 1),
            "load_1": round(load_avg[0], 2),
            "load_5": round(load_avg[1], 2),
            "load_15": round(load_avg[2], 2),
            "history": list(cpu_history),
        },
        "memory": {
            "percent": round(mem.percent, 1),
            "used_gb": _bytes_to_gb(mem.used),
            "total_gb": _bytes_to_gb(mem.total),
            "available_gb": _bytes_to_gb(mem.available),
            "history": list(mem_history),
        },
        "disk": {
            "percent": round(disk.percent, 1),
            "used_gb": _bytes_to_gb(disk.used),
            "total_gb": _bytes_to_gb(disk.total),
            "free_gb": _bytes_to_gb(disk.free),
        },
        "network": {
            "sent_mbps": sent_rate,
            "recv_mbps": recv_rate,
            "sent_history": list(net_sent_history),
            "recv_history": list(net_recv_history),
        },
        "process_count": procs,
        "services": services,
        "metals": fetch_metals(),
        "crypto": fetch_crypto(),
        "news": fetch_gold_news(),
    }


def metrics_stream() -> None:
    psutil.cpu_percent(interval=None)
    fetch_metals(force=True)
    fetch_crypto(force=True)
    fetch_gold_news(force=True)
    while True:
        socketio.emit("metrics_update", collect_metrics())
        socketio.sleep(1)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/perps")
def perps():
    return render_template("perps.html")


@app.route("/api/metrics")
def api_metrics():
    return jsonify(collect_metrics())


@app.route("/api/metals")
def api_metals():
    return jsonify(fetch_metals(force=True))


@app.route("/api/crypto")
def api_crypto():
    return jsonify(fetch_crypto(force=True))


@app.route("/api/perps")
def api_perps():
    return jsonify(fetch_perps(force=True))


@app.route("/api/news")
def api_news():
    return jsonify(fetch_gold_news(force=True))


@app.route("/health")
def health():
    return jsonify({"status": "ok", "service": "ProjAlpha"})


if __name__ == "__main__":
    socketio.start_background_task(target=metrics_stream)
    socketio.run(app, host="0.0.0.0", debug=True, port=5050, allow_unsafe_werkzeug=True)
