"""
US stock/ETF quote fetcher based on Twelve Data.

This module focuses on two use cases:
1. Fetch the latest TSLA-like quote, including current extended-hours fields.
2. Fetch 1-minute time series data with `prepost=true` and display separate
   premarket plus night/post-market bars.

Required local files:
- `tweleve_api_key.local`
- `watchlist_us.local`

Notes from Twelve Data official support/docs:
- `prepost=true` is supported on `/quote`, `/price`, `/time_series`, `/eod`
- For `/quote`, extended-hours data requires minute-level intervals
- U.S. real-time extended hours are available on Pro plan or higher
- Historical extended hours older than one day are available
"""
from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo


TWELVE_DATA_BASE_URL = "https://api.twelvedata.com"
DEFAULT_TIMEOUT = 20
DEFAULT_EXCHANGE = "NASDAQ"
DEFAULT_INTERVAL = "1min"
DEFAULT_TIMEZONE = "America/New_York"
FULL_EXTENDED_SESSION_OUTPUTSIZE = 960
API_KEY_FILE_NAME = "tweleve_api_key.local"
WATCHLIST_FILE_NAME = "watchlist_us.local"
US_SYMBOL_RE = re.compile(r"^[A-Z][A-Z0-9._-]{0,14}$")
US_EASTERN_TZ = ZoneInfo(DEFAULT_TIMEZONE)


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


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and value == value


def _format_number(value: Any, digits: int = 2) -> str:
    number = _safe_float(value)
    return f"{number:.{digits}f}" if number == number else "--"


def _format_pct(value: Any) -> str:
    pct = _safe_float(value)
    if pct != pct:
        return "--.--%"
    return f"{pct:+.2f}%"


def _parse_exchange_datetime(value: Any) -> datetime | None:
    if not value:
        return None

    text = str(value).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(text, fmt)
            return parsed.replace(tzinfo=US_EASTERN_TZ)
        except ValueError:
            pass

    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=US_EASTERN_TZ)
    return parsed.astimezone(US_EASTERN_TZ)


def _parse_unix_timestamp(value: Any) -> datetime | None:
    try:
        timestamp = int(value)
    except (TypeError, ValueError):
        return None
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).astimezone(US_EASTERN_TZ)


def _format_et_datetime(value: datetime | None) -> str:
    if value is None:
        return ""
    return value.astimezone(US_EASTERN_TZ).strftime("%Y-%m-%d %H:%M:%S ET")


def _us_market_session_label(value: datetime | None) -> str:
    if value is None:
        return "UNKNOWN"

    local_dt = value.astimezone(US_EASTERN_TZ)
    if local_dt.weekday() >= 5:
        return "CLOSED"

    minutes = local_dt.hour * 60 + local_dt.minute
    if 4 * 60 <= minutes < 9 * 60 + 30:
        return "PRE"
    if 9 * 60 + 30 <= minutes < 16 * 60:
        return "REG"
    if 16 * 60 <= minutes < 20 * 60:
        return "POST"
    return "CLOSED"


def _first_number(*values: Any) -> float:
    for value in values:
        number = _safe_float(value)
        if number == number:
            return number
    return float("nan")


def _latest_bar_by_session(series: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for bar in series.get("values", []) or []:
        bar_dt = _parse_exchange_datetime(bar.get("datetime"))
        session = _us_market_session_label(bar_dt)
        if session in {"PRE", "REG", "POST"} and session not in out:
            session_bar = dict(bar)
            session_bar["_session"] = session
            session_bar["_datetime_et"] = _format_et_datetime(bar_dt)
            out[session] = session_bar
        if {"PRE", "REG", "POST"}.issubset(out):
            break
    return out


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
        interval: str = DEFAULT_INTERVAL,
        prepost: bool = True,
        timezone_name: str = DEFAULT_TIMEZONE,
    ) -> dict[str, Any]:
        return self._request(
            "quote",
            symbol=normalize_us_symbol(symbol),
            exchange=exchange,
            interval=interval,
            prepost=str(prepost).lower(),
            timezone=timezone_name,
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
        interval: str = DEFAULT_INTERVAL,
        exchange: str = DEFAULT_EXCHANGE,
        outputsize: int = 30,
        prepost: bool = True,
        start_date: str | None = None,
        end_date: str | None = None,
        timezone_name: str = DEFAULT_TIMEZONE,
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
            timezone=timezone_name,
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
    interval: str = DEFAULT_INTERVAL,
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
    extended_price = _safe_float(data.get("extended_price"))
    extended_change = _safe_float(data.get("extended_change"))
    extended_pct = _safe_float(data.get("extended_percent_change"))
    extended_dt = _parse_unix_timestamp(data.get("extended_timestamp"))
    dt = data.get("datetime", "")
    extended = data.get("is_extended_hours")
    requested_prepost = data.get("_requested_prepost")

    close_text = f"{close:.4f}" if close == close else "nan"
    change_text = f"{change:+.4f}" if change == change else "nan"
    pct_text = f"{percent_change:+.2f}%" if percent_change == percent_change else "nan"
    extended_text = f"{extended_price:.4f}" if extended_price == extended_price else "nan"
    extended_change_text = f"{extended_change:+.4f}" if extended_change == extended_change else "nan"
    extended_pct_text = f"{extended_pct:+.2f}%" if extended_pct == extended_pct else "nan"
    return (
        f"{symbol} {name} | price={close_text} | change={change_text} | "
        f"pct={pct_text} | datetime={dt} | "
        f"extended={extended_text} {extended_change_text} {extended_pct_text} "
        f"at={_format_et_datetime(extended_dt)} | "
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
    bars_by_session = _latest_bar_by_session(series)
    latest_bar = (series.get("values") or [{}])[0]
    latest_bar_dt = _parse_exchange_datetime(latest_bar.get("datetime"))
    extended_dt = _parse_unix_timestamp(quote.get("extended_timestamp")) or latest_bar_dt

    regular_price = _safe_float(quote.get("close"))
    extended_price = _safe_float(quote.get("extended_price"))
    latest_price = _first_number(extended_price, latest_bar.get("close"), regular_price)

    regular_pct = _safe_float(quote.get("percent_change"))
    extended_pct = _safe_float(quote.get("extended_percent_change"))
    pct = _first_number(extended_pct, regular_pct)
    name = quote.get("name") or symbol
    current_session = _us_market_session_label(extended_dt)
    if current_session == "CLOSED" and _is_number(extended_price):
        current_session = "EXT"
    row = {
        "code": normalize_us_symbol(symbol),
        "name": name,
        "price": latest_price,
        "pct": pct,
        "regular_price": regular_price,
        "regular_pct": regular_pct,
        "extended_price": extended_price,
        "extended_change": _safe_float(quote.get("extended_change")),
        "extended_pct": extended_pct,
        "extended_time": _format_et_datetime(extended_dt),
        "session": current_session,
        "market": "US",
        "bar_time": latest_bar.get("datetime", ""),
        "bar_volume": _safe_int(latest_bar.get("volume")),
        "interval": (series.get("meta") or {}).get("interval", ""),
        "prepost_requested": series.get("_requested_prepost"),
        "pre_price": _safe_float((bars_by_session.get("PRE") or {}).get("close")),
        "pre_time": (bars_by_session.get("PRE") or {}).get("_datetime_et", ""),
        "pre_volume": _safe_int((bars_by_session.get("PRE") or {}).get("volume")),
        "post_price": _safe_float((bars_by_session.get("POST") or {}).get("close")),
        "post_time": (bars_by_session.get("POST") or {}).get("_datetime_et", ""),
        "post_volume": _safe_int((bars_by_session.get("POST") or {}).get("volume")),
        "regular_bar_price": _safe_float((bars_by_session.get("REG") or {}).get("close")),
        "regular_bar_time": (bars_by_session.get("REG") or {}).get("_datetime_et", ""),
        "warning": series.get("_warning", ""),
    }
    return row


def fetch_us_spot_row(symbol: str, client: TwelveDataClient | None = None) -> dict[str, Any]:
    api = client or TwelveDataClient()
    quote = fetch_us_quote_with_fallback(symbol=symbol, client=api)
    series = fetch_us_intraday_series_with_fallback(
        symbol=symbol,
        outputsize=FULL_EXTENDED_SESSION_OUTPUTSIZE,
        client=api,
    )
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

    session = row.get("session") or "U"
    tag = f"[{session}]"

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

    price_str = _format_number(price, 2)
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
        print(
            "    regular: "
            f"price={_format_number(row.get('regular_price'), 2)} "
            f"pct={_format_pct(row.get('regular_pct'))}"
        )
        print(
            "    premarket: "
            f"price={_format_number(row.get('pre_price'), 2)} "
            f"time={row.get('pre_time') or '--'} "
            f"volume={row.get('pre_volume') if row.get('pre_volume') is not None else '--'}"
        )
        print(
            "    night/post: "
            f"price={_format_number(row.get('post_price'), 2)} "
            f"time={row.get('post_time') or '--'} "
            f"volume={row.get('post_volume') if row.get('post_volume') is not None else '--'}"
        )
        if _is_number(row.get("extended_price")):
            print(
                "    quote extended: "
                f"price={_format_number(row.get('extended_price'), 2)} "
                f"pct={_format_pct(row.get('extended_pct'))} "
                f"time={row.get('extended_time') or '--'}"
            )
        if row.get("warning"):
            print(f"    note: {row['warning']}")


def main() -> None:
    credentials = TwelveDataCredentials.from_local_file()
    client = TwelveDataClient(credentials=credentials)
    watchlist, watchlist_source = load_watchlist_from_csv_file()
    print(
        "Twelve Data US intraday spot with premarket/night-post "
        f"| fetched_at={datetime.now():%Y-%m-%d %H:%M:%S}\n"
        f"api_key_source={credentials.source} | watchlist_source={watchlist_source} | "
        f"symbols={len(watchlist)} | interval={DEFAULT_INTERVAL} | prepost=true"
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
