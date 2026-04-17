"""
Free US stock/ETF quote fetcher based on Sina's public HQ endpoint.

Supported input examples:
- TSLA
- AAPL
- SPY
- BRK.B
- gb_tsla

Endpoint example:
- https://hq.sinajs.cn/list=gb_tsla
"""
from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any, Iterable
from urllib.request import Request, urlopen


SINA_US_URL = "https://hq.sinajs.cn/list="
DEFAULT_HEADERS = {
    "Referer": "https://finance.sina.com.cn/",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
}
LINE_RE = re.compile(r'var hq_str_(gb_[a-z0-9._-]+)="([^"]*)"')
US_SYMBOL_RE = re.compile(r"^[A-Z][A-Z0-9._-]{0,14}$")


def normalize_us_symbol(symbol: str) -> str:
    raw = symbol.strip().upper().replace(" ", "")
    if not raw:
        raise ValueError("symbol must not be empty")

    if raw.startswith("GB_"):
        raw = raw[3:]

    if raw.endswith((".US", ".NYSE", ".NASDAQ", ".ARCA", ".AMEX")):
        raw = raw.split(".", 1)[0]

    if not US_SYMBOL_RE.fullmatch(raw):
        raise ValueError(f"invalid US stock/ETF symbol: {symbol}")

    return raw


def to_sina_us_code(symbol: str) -> str:
    return f"gb_{normalize_us_symbol(symbol).lower()}"


def _safe_float(value: str | None) -> float:
    try:
        return float(value) if value not in (None, "") else float("nan")
    except ValueError:
        return float("nan")


def _safe_int(value: str | None) -> int | None:
    try:
        return int(value) if value not in (None, "") else None
    except ValueError:
        return None


def _build_request(url: str) -> Request:
    return Request(url, headers=DEFAULT_HEADERS)


def _parse_sina_us_payload(sina_code: str, payload: str) -> dict[str, Any]:
    fields = payload.split(",")
    symbol = sina_code.removeprefix("gb_").upper()

    quote = {
        "symbol": symbol,
        "sina_code": sina_code,
        "name": fields[0] if len(fields) > 0 else "",
        "price": _safe_float(fields[1] if len(fields) > 1 else None),
        "change_pct": _safe_float(fields[2] if len(fields) > 2 else None),
        "quote_time_cn": fields[3] if len(fields) > 3 else "",
        "change_amount": _safe_float(fields[4] if len(fields) > 4 else None),
        "open": _safe_float(fields[5] if len(fields) > 5 else None),
        "high": _safe_float(fields[6] if len(fields) > 6 else None),
        "low": _safe_float(fields[7] if len(fields) > 7 else None),
        "high_52w": _safe_float(fields[8] if len(fields) > 8 else None),
        "low_52w": _safe_float(fields[9] if len(fields) > 9 else None),
        "volume": _safe_int(fields[10] if len(fields) > 10 else None),
        "avg_volume_3m": _safe_int(fields[11] if len(fields) > 11 else None),
        "market_cap": _safe_float(fields[12] if len(fields) > 12 else None),
        "pe_ttm": _safe_float(fields[13] if len(fields) > 13 else None),
        "quote_time_us": fields[25] if len(fields) > 25 else "",
        "prev_close": _safe_float(fields[26] if len(fields) > 26 else None),
    }
    return quote


def fetch_us_quote(symbol: str, timeout: int = 15) -> dict[str, Any]:
    sina_code = to_sina_us_code(symbol)
    request = _build_request(SINA_US_URL + sina_code)
    with urlopen(request, timeout=timeout) as response:
        body = response.read().decode("gbk", errors="replace")

    match = LINE_RE.search(body)
    if not match:
        raise RuntimeError(f"Sina returned no parsable quote for {symbol}: {body[:200]}")

    return _parse_sina_us_payload(match.group(1), match.group(2))


def fetch_us_quote_batch(symbols: Iterable[str], timeout: int = 15) -> list[dict[str, Any]]:
    sina_codes = [to_sina_us_code(symbol) for symbol in symbols]
    if not sina_codes:
        return []

    request = _build_request(SINA_US_URL + ",".join(sina_codes))
    with urlopen(request, timeout=timeout) as response:
        body = response.read().decode("gbk", errors="replace")

    by_code: dict[str, dict[str, Any]] = {}
    for match in LINE_RE.finditer(body):
        by_code[match.group(1)] = _parse_sina_us_payload(match.group(1), match.group(2))

    return [
        by_code.get(
            sina_code,
            {
                "symbol": sina_code.removeprefix("gb_").upper(),
                "sina_code": sina_code,
                "error": "quote_not_found",
            },
        )
        for sina_code in sina_codes
    ]


def format_quote_line(quote: dict[str, Any]) -> str:
    name = quote.get("name") or quote.get("symbol") or ""
    price = quote.get("price")
    pct = quote.get("change_pct")
    us_time = quote.get("quote_time_us") or quote.get("quote_time_cn") or ""

    price_text = f"{price:.4f}" if isinstance(price, (int, float)) and price == price else "nan"
    pct_text = f"{pct:+.2f}%" if isinstance(pct, (int, float)) and pct == pct else "nan"
    return f"{quote.get('symbol', ''):>6}  {name:<12}  {price_text:>10}  {pct_text:>8}  {us_time}"


def main() -> None:
    watchlist = ["TSLA", "AAPL", "NVDA", "SPY", "QQQ"]
    quotes = fetch_us_quote_batch(watchlist)

    print(f"US quote snapshot from Sina | fetched_at={datetime.now():%Y-%m-%d %H:%M:%S}")
    for quote in quotes:
        print(format_quote_line(quote))

    print("\nTSLA full payload:")
    print(json.dumps(fetch_us_quote("TSLA"), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
