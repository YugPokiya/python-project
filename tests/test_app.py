"""ProjAlpha unit tests — no live market calls."""

from __future__ import annotations

from unittest.mock import patch

import pytest

import app as dashboard
from Connection import AgentConfig, parse_args


@pytest.fixture()
def client():
    dashboard.app.config["TESTING"] = True
    with dashboard.app.test_client() as c:
        yield c


def test_price_digits():
    assert dashboard._price_digits(50000) == 1
    assert dashboard._price_digits(2500) == 2
    assert dashboard._price_digits(12.5) == 3
    assert dashboard._price_digits(0.4) == 4


def test_expiry_label():
    # 2026-09-09 08:00:00 UTC
    assert dashboard._expiry_label(1757404800000) == "09 SEP 2026"


def test_health(client):
    res = client.get("/health")
    assert res.status_code == 200
    body = res.get_json()
    assert body["status"] == "ok"
    assert body["service"] == "ProjAlpha"


def test_index_page(client):
    res = client.get("/")
    assert res.status_code == 200
    html = res.data.decode()
    assert "ProjAlpha" in html
    assert "/perps" in html


def test_perps_page(client):
    res = client.get("/perps")
    assert res.status_code == 200
    html = res.data.decode()
    assert "Perp Dashboard" in html
    assert "Delta" in html
    assert "Gamma" in html
    assert "Vega" in html


def test_api_perps_uses_cache(client):
    fake = {
        "assets": {
            "btc": {
                "symbol": "BTC",
                "name": "Bitcoin",
                "perp": {
                    "mark": 70000.0,
                    "mid": 70000.0,
                    "oracle": 70010.0,
                    "funding": 0.0001,
                    "funding_pct": 0.01,
                    "funding_apr_pct": 87.6,
                    "premium": 0.0,
                    "premium_pct": 0.0,
                    "open_interest": 1000.0,
                    "day_volume_usd": 1e6,
                    "change": 0.0,
                    "change_pct": 0.0,
                    "digits": 1,
                    "history": [70000.0],
                    "source": "hyperliquid",
                },
                "index": 70000.0,
                "greeks": {
                    "delta": 0.5,
                    "gamma": 0.0003,
                    "vega": 15.0,
                    "theta": -20.0,
                    "mark_iv": 40.0,
                    "expiry": "09 SEP 2026",
                    "strike": 70000.0,
                },
                "atm": [],
                "market_greeks": {
                    "net_delta": 1.0,
                    "gamma": 0.2,
                    "vega": 100.0,
                    "theta": -50.0,
                    "open_interest": 10.0,
                    "instruments": 2,
                    "expiry": "09 SEP 2026",
                },
                "greeks_source": "deribit",
            }
        },
        "updated_at": "2026-09-08T00:00:00+00:00",
        "error": None,
        "network": "mainnet",
        "sources": {"perp": "hyperliquid", "greeks": "deribit"},
    }
    with patch.object(dashboard, "fetch_perps", return_value=fake) as mocked:
        res = client.get("/api/perps")
        assert res.status_code == 200
        body = res.get_json()
        assert body["assets"]["btc"]["greeks"]["delta"] == 0.5
        assert body["assets"]["btc"]["greeks"]["gamma"] == 0.0003
        assert body["assets"]["btc"]["greeks"]["vega"] == 15.0
        mocked.assert_called_once_with(force=True)


def test_api_crypto_mocked(client):
    fake = {
        "coins": {
            "btc": {
                "symbol": "BTC",
                "name": "Bitcoin",
                "price": 70000.0,
                "digits": 1,
                "change": 0.0,
                "change_pct": 0.0,
                "history": [70000.0],
                "analysis": {
                    "window_high": 70000.0,
                    "window_low": 70000.0,
                    "vs_high_pct": 0.0,
                    "vs_low_pct": 0.0,
                    "range_position": 0.5,
                },
            }
        },
        "updated_at": "2026-09-08T00:00:00+00:00",
        "error": None,
        "network": "mainnet",
        "source": "hyperliquid",
    }
    with patch.object(dashboard, "fetch_crypto", return_value=fake):
        res = client.get("/api/crypto")
        assert res.status_code == 200
        assert res.get_json()["coins"]["btc"]["price"] == 70000.0


def test_atm_pair_selection():
    options = [
        {"option_type": "call", "strike": 69000, "instrument_name": "BTC-C-69000"},
        {"option_type": "call", "strike": 70000, "instrument_name": "BTC-C-70000"},
        {"option_type": "call", "strike": 71000, "instrument_name": "BTC-C-71000"},
        {"option_type": "put", "strike": 69000, "instrument_name": "BTC-P-69000"},
        {"option_type": "put", "strike": 70000, "instrument_name": "BTC-P-70000"},
        {"option_type": "put", "strike": 71000, "instrument_name": "BTC-P-71000"},
    ]
    call, put = dashboard._atm_pair(options, 70100)
    assert call["strike"] == 70000
    assert put["strike"] == 70000


def test_connection_config_defaults(monkeypatch):
    monkeypatch.setattr("sys.argv", ["Connection.py"])
    cfg = parse_args()
    assert isinstance(cfg, AgentConfig)
    assert "BTC" in cfg.coins
    assert "allMids" in cfg.feeds
    assert cfg.testnet is False
