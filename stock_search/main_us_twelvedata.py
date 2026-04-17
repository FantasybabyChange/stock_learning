"""
US stock/ETF quote fetcher based on Twelve Data.

This module focuses on two use cases:
1. Fetch the latest TSLA-like quote, optionally including extended hours.
2. Fetch 1-minute time series data with `prepost=true` for full-session prices.

Required local files:
- `tweleve_api_key.local`
- `watchlist_us.local`

Notes from Twelve Data official support/docs:
- `prepost=true` is supported on `/quote`, `/price`, `/time_series`, `/eod`
- U.S. real-time extended hours are available on Pro plan or higher
- Historical extended hours older than one day are available
"""
from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlencode
from urllib.request import Request, urlopen


TWELVE_DATA_BASE_URL = "https://api.twelvedata.com"
DEFAULT_TIMEOUT = 20
DEFAULT_EXCHANGE = "NASDAQ"
API_KEY_FILE_NAME = "tweleve_api_key.local"
WATCHLIST_FILE_NAME = "watchlist_us.local"
US_SYMBOL_RE = re.compile(r"^[A-Z][A-Z0-9._-]{0,14}$")


def normalize_us_symbol(symbol: str) -> str:
    raw = symbol.strip().upper().replace(" ", "")
    if not raw:
        raise ValueError("symbol must not be empty")

    if raw.endswith((".US", ".NYSE", ".NASDAQ", ".ARCA", ".AMEX")):
        raw = raw.split(".", 1)[0]

    if not US_SYMBOL_RE.fullmatch(raw):
        raise ValueError(f"invalid US stock/ETF symbol: {symbol}")

    return raw


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


def _default_base_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def _read_text_file(path: Path) -> str:
    return path.read_text(encoding="utf-8").strip()


def load_api_key_from_local_file(path: Path | None = None) -> tuple[str, str]:
    key_path = (path or _default_base_dir() / API_KEY_FILE_NAME).resolve()
    text = _read_text_file(key_path)

    for line in text.splitlines():
        candidate = line.strip()
        if candidate and not candidate.startswith("#"):
            return candidate, str(key_path)

    raise RuntimeError(f"No API key found in {key_path}")


def load_watchlist_from_csv_file(path: Path | None = None) -> tuple[list[str], str]:
    watchlist_path = (path or _default_base_dir() / WATCHLIST_FILE_NAME).resolve()
    text = _read_text_file(watchlist_path)
    flat = re.sub(r"[\r\n]+", ",", text)

    out: list[str] = []
    for part in flat.split(","):
        symbol = part.strip()
        if symbol and not symbol.startswith("#"):
            out.append(normalize_us_symbol(symbol))

    if not out:
        raise RuntimeError(f"No symbols found in {watchlist_path}")

    return out, str(watchlist_path)


@dataclass(slots=True)
class TwelveDataCredentials:
    api_key: str
    source: str

    @classmethod
    def from_local_file(cls) -> "TwelveDataCredentials":
        api_key, source = load_api_key_from_local_file()
        return cls(api_key=api_key, source=source)


class TwelveDataClient:
    def __init__(
        self,
        credentials: TwelveDataCredentials | None = None,
        timeout: int = DEFAULT_TIMEOUT,
    ) -> None:
        self.credentials = credentials or TwelveDataCredentials.from_local_file()
        self.timeout = timeout

    def _request(self, endpoint: str, **params: Any) -> dict[str, Any]:
        payload = {k: v for k, v in params.items() if v is not None}
        payload["apikey"] = self.credentials.api_key
        url = f"{TWELVE_DATA_BASE_URL}/{endpoint}?{urlencode(payload)}"
        request = Request(
            url,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json",
            },
        )

        with urlopen(request, timeout=self.timeout) as response:
            data = json.loads(response.read().decode("utf-8", errors="replace"))

        if data.get("status") == "error":
            code = data.get("code", "unknown")
            message = data.get("message", "unknown error")
            raise RuntimeError(f"Twelve Data API error {code}: {message}")

        return data

    def get_quote(
        self,
        symbol: str,
        exchange: str = DEFAULT_EXCHANGE,
        prepost: bool = True,
    ) -> dict[str, Any]:
        return self._request(
            "quote",
            symbol=normalize_us_symbol(symbol),
            exchange=exchange,
            prepost=str(prepost).lower(),
        )

    def get_price(
        self,
        symbol: str,
        exchange: str = DEFAULT_EXCHANGE,
        prepost: bool = True,
    ) -> dict[str, Any]:
        return self._request(
            "price",
            symbol=normalize_us_symbol(symbol),
            exchange=exchange,
            prepost=str(prepost).lower(),
        )

    def get_time_series(
        self,
        symbol: str,
        interval: str = "1min",
        exchange: str = DEFAULT_EXCHANGE,
        outputsize: int = 30,
        prepost: bool = True,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> dict[str, Any]:
        return self._request(
            "time_series",
            symbol=normalize_us_symbol(symbol),
            exchange=exchange,
            interval=interval,
            outputsize=outputsize,
            prepost=str(prepost).lower(),
            start_date=start_date,
            end_date=end_date,
        )


def fetch_us_quote_with_fallback(
    symbol: str = "TSLA",
    exchange: str = DEFAULT_EXCHANGE,
    client: TwelveDataClient | None = None,
) -> dict[str, Any]:
    api = client or TwelveDataClient()
    try:
        data = api.get_quote(symbol=symbol, exchange=exchange, prepost=True)
        data["_requested_prepost"] = True
        return data
    except RuntimeError as exc:
        # Free/basic plans may reject real-time extended-hours access.
        if "Pro plan" not in str(exc) and "higher" not in str(exc):
            raise
        data = api.get_quote(symbol=symbol, exchange=exchange, prepost=False)
        data["_requested_prepost"] = False
        data["_warning"] = (
            "Extended-hours quote was not available for this plan, "
            "fallback to regular quote."
        )
        return data


def fetch_us_intraday_series_with_fallback(
    symbol: str = "TSLA",
    exchange: str = DEFAULT_EXCHANGE,
    interval: str = "1min",
    outputsize: int = 10,
    client: TwelveDataClient | None = None,
) -> dict[str, Any]:
    api = client or TwelveDataClient()
    try:
        data = api.get_time_series(
            symbol=symbol,
            exchange=exchange,
            interval=interval,
            outputsize=outputsize,
            prepost=True,
        )
        data["_requested_prepost"] = True
        return data
    except RuntimeError as exc:
        if "Pro plan" not in str(exc) and "higher" not in str(exc):
            raise
        data = api.get_time_series(
            symbol=symbol,
            exchange=exchange,
            interval=interval,
            outputsize=outputsize,
            prepost=False,
        )
        data["_requested_prepost"] = False
        data["_warning"] = (
            "Extended-hours time series was not available for this plan, "
            "fallback to regular intraday series."
        )
        return data


def summarize_quote(data: dict[str, Any]) -> str:
    symbol = data.get("symbol", "")
    name = data.get("name", "")
    close = _safe_float(data.get("close"))
    change = _safe_float(data.get("change"))
    percent_change = _safe_float(data.get("percent_change"))
    dt = data.get("datetime", "")
    extended = data.get("is_extended_hours")
    requested_prepost = data.get("_requested_prepost")

    close_text = f"{close:.4f}" if close == close else "nan"
    change_text = f"{change:+.4f}" if change == change else "nan"
    pct_text = f"{percent_change:+.2f}%" if percent_change == percent_change else "nan"
    return (
        f"{symbol} {name} | price={close_text} | change={change_text} | "
        f"pct={pct_text} | datetime={dt} | "
        f"prepost_requested={requested_prepost} | is_extended_hours={extended}"
    )


def summarize_series(data: dict[str, Any]) -> str:
    meta = data.get("meta", {})
    values = data.get("values", []) or []
    if not values:
        return "No intraday rows returned."

    latest = values[0]
    return (
        f"{meta.get('symbol', '')} {meta.get('interval', '')} rows={len(values)} | "
        f"latest={latest.get('datetime')} close={latest.get('close')} volume={latest.get('volume')} | "
        f"prepost_requested={data.get('_requested_prepost')}"
    )


def build_spot_row(
    symbol: str,
    quote: dict[str, Any],
    series: dict[str, Any],
) -> dict[str, Any]:
    latest_bar = (series.get("values") or [{}])[0]
    latest_price = _safe_float(latest_bar.get("close"))
    if latest_price != latest_price:
        latest_price = _safe_float(quote.get("close"))

    pct = _safe_float(quote.get("percent_change"))
    name = quote.get("name") or symbol
    row = {
        "code": normalize_us_symbol(symbol),
        "name": name,
        "price": latest_price,
        "pct": pct,
        "market": "US",
        "bar_time": latest_bar.get("datetime", ""),
        "bar_volume": _safe_int(latest_bar.get("volume")),
        "interval": (series.get("meta") or {}).get("interval", ""),
        "prepost_requested": series.get("_requested_prepost"),
        "warning": series.get("_warning", ""),
    }
    return row


def fetch_us_spot_row(symbol: str, client: TwelveDataClient | None = None) -> dict[str, Any]:
    api = client or TwelveDataClient()
    quote = fetch_us_quote_with_fallback(symbol=symbol, client=api)
    series = fetch_us_intraday_series_with_fallback(symbol=symbol, outputsize=1, client=api)
    return build_spot_row(symbol, quote, series)


def fetch_us_spot_rows(
    symbols: Iterable[str],
    client: TwelveDataClient | None = None,
) -> list[dict[str, Any]]:
    api = client or TwelveDataClient()
    rows: list[dict[str, Any]] = []
    for symbol in symbols:
        rows.append(fetch_us_spot_row(symbol, client=api))
    return rows


def _fmt_cell(row: dict[str, Any]) -> str:
    name = (row.get("name") or "")[:12]
    price = row.get("price")
    pct = row.get("pct")
    code = row.get("code", "")

    tag = "[U]"

    if pct is None or pct != pct:
        color = "·"
        pct_str = "--.--%"
    elif pct > 0:
        color = "UP"
        pct_str = f"+{pct:.2f}%"
    elif pct < 0:
        color = "DN"
        pct_str = f"{pct:.2f}%"
    else:
        color = "EQ"
        pct_str = "0.00%"

    price_str = f"{price:.2f}" if price == price else "--.--"
    return f"{tag} {code:8} {name:12} {price_str:8} {color} {pct_str}"


def print_rows(rows: list[dict[str, Any]], sep: str = "  |  ") -> None:
    for i in range(0, len(rows), 1):
        chunk = rows[i : i + 1]
        print(sep.join(_fmt_cell(row) for row in chunk))


def print_row_details(rows: list[dict[str, Any]]) -> None:
    for row in rows:
        bar_time = row.get("bar_time", "")
        interval = row.get("interval", "")
        volume = row.get("bar_volume")
        volume_text = str(volume) if volume is not None else "--"
        prepost_text = row.get("prepost_requested")
        print(
            f"    intraday: {interval} latest={bar_time} volume={volume_text} "
            f"prepost={prepost_text}"
        )
        if row.get("warning"):
            print(f"    note: {row['warning']}")


def main() -> None:
    credentials = TwelveDataCredentials.from_local_file()
    client = TwelveDataClient(credentials=credentials)
    watchlist, watchlist_source = load_watchlist_from_csv_file()
    print(
        "Twelve Data US intraday spot "
        f"| fetched_at={datetime.now():%Y-%m-%d %H:%M:%S}\n"
        f"api_key_source={credentials.source} | watchlist_source={watchlist_source} | "
        f"symbols={len(watchlist)}"
    )
    rows = fetch_us_spot_rows(watchlist, client=client)
    print_rows(rows)
    print_row_details(rows)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Run failed: {exc}")
        print(
            f"Create {API_KEY_FILE_NAME} with your Twelve Data API key, and "
            f"{WATCHLIST_FILE_NAME} with comma-separated or line-separated symbols. "
            "If your plan does not include extended-hours intraday data, the script "
            "will fall back to regular intraday series."
        )
