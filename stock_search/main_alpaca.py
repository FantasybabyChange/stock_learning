"""
Alpaca Market Data client for real-time US stock and ETF quotes.

This module replaces the old Yahoo-based implementation and exposes a
small set of helpers that fit the current project style:

- fetch_us_etf_spot: fetch one symbol's latest snapshot
- fetch_us_etf_spot_batch: fetch multiple symbols at once
- fetch_us_etf_bars: fetch historical or intraday bars
- download_and_save_offline: export bars to CSV for local analysis

Required environment variables:
- APCA_API_KEY_ID
- APCA_API_SECRET_KEY

Optional environment variables:
- APCA_API_DATA_URL (default: https://data.alpaca.markets)
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
import requests

ALPACA_DATA_URL = "https://data.alpaca.markets"
DEFAULT_FEED = "iex"
DEFAULT_TIMEOUT = 15

_SYMBOL_RE = re.compile(r"^[A-Z][A-Z0-9._-]{0,14}$")


def normalize_us_symbol(symbol: str) -> str:
    """
    Normalize a US stock/ETF symbol to the format expected by Alpaca.

    Examples:
    - spy -> SPY
    - qqq.us -> QQQ
    - BRK.B -> BRK.B
    """
    raw = symbol.strip().upper().replace(" ", "")
    if not raw:
        raise ValueError("symbol must not be empty")

    if raw.endswith((".US", ".NYSE", ".NASDAQ", ".ARCA", ".AMEX")):
        raw = raw.split(".", 1)[0]

    if not _SYMBOL_RE.fullmatch(raw):
        raise ValueError(f"invalid US stock/ETF symbol: {symbol}")

    return raw


def _to_rfc3339(value: str | datetime | pd.Timestamp | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return pd.Timestamp(value).isoformat()
    return pd.Timestamp(value).isoformat()


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float("nan")


def _safe_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


@dataclass(slots=True)
class AlpacaCredentials:
    api_key: str
    api_secret: str
    data_url: str = ALPACA_DATA_URL

    @classmethod
    def from_env(cls) -> "AlpacaCredentials":
        api_key = os.getenv("APCA_API_KEY_ID", "").strip()
        api_secret = os.getenv("APCA_API_SECRET_KEY", "").strip()
        data_url = os.getenv("APCA_API_DATA_URL", ALPACA_DATA_URL).strip() or ALPACA_DATA_URL

        if not api_key or not api_secret:
            raise RuntimeError(
                "Missing Alpaca API credentials. Set APCA_API_KEY_ID "
                "and APCA_API_SECRET_KEY first."
            )

        return cls(api_key=api_key, api_secret=api_secret, data_url=data_url.rstrip("/"))


class AlpacaMarketDataClient:
    def __init__(
        self,
        credentials: AlpacaCredentials | None = None,
        session: requests.Session | None = None,
        timeout: int = DEFAULT_TIMEOUT,
    ) -> None:
        self.credentials = credentials or AlpacaCredentials.from_env()
        self.timeout = timeout
        self.session = session or requests.Session()
        self.session.headers.update(
            {
                "APCA-API-KEY-ID": self.credentials.api_key,
                "APCA-API-SECRET-KEY": self.credentials.api_secret,
                "Accept": "application/json",
            }
        )

    def _request(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self.credentials.data_url}{path}"
        response = self.session.get(url, params=params, timeout=self.timeout)
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            detail = response.text.strip()
            if detail:
                raise RuntimeError(f"Alpaca request failed: {response.status_code} {detail}") from exc
            raise RuntimeError(f"Alpaca request failed: {response.status_code}") from exc
        return response.json()

    def get_snapshot(self, symbol: str, feed: str = DEFAULT_FEED) -> dict[str, Any]:
        sym = normalize_us_symbol(symbol)
        return self._request(f"/v2/stocks/{sym}/snapshot", params={"feed": feed})

    def get_snapshots(
        self,
        symbols: Iterable[str],
        feed: str = DEFAULT_FEED,
    ) -> dict[str, dict[str, Any]]:
        normalized = [normalize_us_symbol(symbol) for symbol in symbols]
        if not normalized:
            return {}
        payload = self._request(
            "/v2/stocks/snapshots",
            params={"symbols": ",".join(normalized), "feed": feed},
        )
        return payload.get("snapshots", {})

    def get_latest_quote(self, symbol: str, feed: str = DEFAULT_FEED) -> dict[str, Any]:
        sym = normalize_us_symbol(symbol)
        payload = self._request(f"/v2/stocks/{sym}/quotes/latest", params={"feed": feed})
        return payload.get("quote", {})

    def get_latest_bar(self, symbol: str, feed: str = DEFAULT_FEED) -> dict[str, Any]:
        sym = normalize_us_symbol(symbol)
        payload = self._request(f"/v2/stocks/{sym}/bars/latest", params={"feed": feed})
        return payload.get("bar", {})

    def get_bars(
        self,
        symbols: str | Iterable[str],
        timeframe: str = "1Min",
        start: str | datetime | pd.Timestamp | None = None,
        end: str | datetime | pd.Timestamp | None = None,
        limit: int | None = 500,
        feed: str = DEFAULT_FEED,
        adjustment: str = "raw",
        sort: str = "asc",
    ) -> pd.DataFrame:
        symbol_list = (
            [normalize_us_symbol(symbols)]
            if isinstance(symbols, str)
            else [normalize_us_symbol(symbol) for symbol in symbols]
        )
        params: dict[str, Any] = {
            "symbols": ",".join(symbol_list),
            "timeframe": timeframe,
            "feed": feed,
            "adjustment": adjustment,
            "sort": sort,
        }
        if start is not None:
            params["start"] = _to_rfc3339(start)
        if end is not None:
            params["end"] = _to_rfc3339(end)
        if limit is not None:
            params["limit"] = limit

        payload = self._request("/v2/stocks/bars", params=params)
        bars_map = payload.get("bars", {})
        rows: list[dict[str, Any]] = []

        for symbol, bars in bars_map.items():
            for bar in bars:
                rows.append(
                    {
                        "symbol": symbol,
                        "timestamp": pd.Timestamp(bar.get("t")),
                        "open": _safe_float(bar.get("o")),
                        "high": _safe_float(bar.get("h")),
                        "low": _safe_float(bar.get("l")),
                        "close": _safe_float(bar.get("c")),
                        "volume": _safe_int(bar.get("v")),
                        "trade_count": _safe_int(bar.get("n")),
                        "vwap": _safe_float(bar.get("vw")),
                    }
                )

        if not rows:
            return pd.DataFrame(
                columns=[
                    "symbol",
                    "timestamp",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "trade_count",
                    "vwap",
                ]
            )

        df = pd.DataFrame(rows).sort_values(["symbol", "timestamp"]).reset_index(drop=True)
        return df


def _snapshot_to_series(symbol: str, snapshot: dict[str, Any]) -> pd.Series:
    latest_trade = snapshot.get("latestTrade") or {}
    latest_quote = snapshot.get("latestQuote") or {}
    minute_bar = snapshot.get("minuteBar") or {}
    daily_bar = snapshot.get("dailyBar") or {}
    prev_daily_bar = snapshot.get("prevDailyBar") or {}

    last_price = _safe_float(latest_trade.get("p"))
    prev_close = _safe_float(prev_daily_bar.get("c"))
    pct_change = (
        (last_price - prev_close) / prev_close * 100.0
        if pd.notna(last_price) and pd.notna(prev_close) and prev_close
        else float("nan")
    )

    return pd.Series(
        {
            "symbol": symbol,
            "last_price": last_price,
            "last_size": _safe_int(latest_trade.get("s")),
            "last_trade_time": latest_trade.get("t"),
            "bid_price": _safe_float(latest_quote.get("bp")),
            "bid_size": _safe_int(latest_quote.get("bs")),
            "ask_price": _safe_float(latest_quote.get("ap")),
            "ask_size": _safe_int(latest_quote.get("as")),
            "quote_time": latest_quote.get("t"),
            "minute_open": _safe_float(minute_bar.get("o")),
            "minute_high": _safe_float(minute_bar.get("h")),
            "minute_low": _safe_float(minute_bar.get("l")),
            "minute_close": _safe_float(minute_bar.get("c")),
            "minute_volume": _safe_int(minute_bar.get("v")),
            "today_open": _safe_float(daily_bar.get("o")),
            "today_high": _safe_float(daily_bar.get("h")),
            "today_low": _safe_float(daily_bar.get("l")),
            "today_close": _safe_float(daily_bar.get("c")),
            "today_volume": _safe_int(daily_bar.get("v")),
            "prev_close": prev_close,
            "change_pct": pct_change,
        }
    )


def fetch_us_etf_spot(
    symbol: str,
    feed: str = DEFAULT_FEED,
    client: AlpacaMarketDataClient | None = None,
) -> pd.Series:
    """
    Fetch one US stock/ETF real-time snapshot.

    feed:
    - iex: free plan commonly available, real-time IEX feed
    - delayed_sip: broader market but delayed
    - sip: paid full feed
    """
    api = client or AlpacaMarketDataClient()
    normalized = normalize_us_symbol(symbol)
    snapshot = api.get_snapshot(normalized, feed=feed)
    return _snapshot_to_series(normalized, snapshot)


def fetch_us_etf_spot_batch(
    symbols: Iterable[str],
    feed: str = DEFAULT_FEED,
    client: AlpacaMarketDataClient | None = None,
) -> pd.DataFrame:
    api = client or AlpacaMarketDataClient()
    normalized = [normalize_us_symbol(symbol) for symbol in symbols]
    snapshots = api.get_snapshots(normalized, feed=feed)
    rows = [_snapshot_to_series(symbol, snapshots.get(symbol, {})) for symbol in normalized]
    return pd.DataFrame(rows)


def fetch_us_etf_bars(
    symbols: str | Iterable[str] = "SPY",
    timeframe: str = "1Min",
    start: str | datetime | pd.Timestamp | None = None,
    end: str | datetime | pd.Timestamp | None = None,
    limit: int | None = 500,
    feed: str = DEFAULT_FEED,
    adjustment: str = "raw",
    client: AlpacaMarketDataClient | None = None,
) -> pd.DataFrame:
    api = client or AlpacaMarketDataClient()
    return api.get_bars(
        symbols=symbols,
        timeframe=timeframe,
        start=start,
        end=end,
        limit=limit,
        feed=feed,
        adjustment=adjustment,
    )


def download_and_save_offline(
    ticker: str = "SPY",
    timeframe: str = "1Min",
    start: str | datetime | pd.Timestamp | None = None,
    end: str | datetime | pd.Timestamp | None = None,
    limit: int | None = 500,
    filename: str | os.PathLike[str] | None = None,
    feed: str = DEFAULT_FEED,
    adjustment: str = "raw",
) -> pd.DataFrame:
    """
    Download Alpaca bars and save them as CSV for local analysis.

    Example:
    - download_and_save_offline("SPY", timeframe="1Min", limit=390)
    - download_and_save_offline("QQQ", timeframe="1Day", start="2025-01-01")
    """
    symbol = normalize_us_symbol(ticker)
    df = fetch_us_etf_bars(
        symbols=symbol,
        timeframe=timeframe,
        start=start,
        end=end,
        limit=limit,
        feed=feed,
        adjustment=adjustment,
    )

    if df.empty:
        raise RuntimeError(f"Alpaca returned no bars for {symbol}")

    if filename is None:
        now_str = datetime.now().strftime("%Y%m%d")
        filename = f"{symbol}_alpaca_{timeframe}_{now_str}.csv"

    output_path = Path(filename).resolve()
    df.to_csv(output_path, index=False, encoding="utf-8-sig")

    print(f"Saved {symbol} data to: {output_path}")
    print(
        f"Range: {df['timestamp'].iloc[0]} to {df['timestamp'].iloc[-1]} | "
        f"rows={len(df)}"
    )
    print(f"Latest close/latest bar close: {df['close'].iloc[-1]:.4f}")
    return df


if __name__ == "__main__":
    watchlist = ["SPY", "QQQ", "DIA", "IWM", "VOO"]

    try:
        quotes = fetch_us_etf_spot_batch(watchlist, feed=DEFAULT_FEED)
        print("=== Alpaca real-time US stock/ETF snapshots ===")
        print(quotes.to_string(index=False))

        print("\n=== SPY latest 10 x 1Min bars ===")
        bars = fetch_us_etf_bars("SPY", timeframe="1Min", limit=10, feed=DEFAULT_FEED)
        print(bars.to_string(index=False))

    except Exception as exc:
        print(f"Run failed: {exc}")
        print(
            "Make sure APCA_API_KEY_ID and APCA_API_SECRET_KEY are set, "
            "and that your Alpaca account can access market data."
        )
