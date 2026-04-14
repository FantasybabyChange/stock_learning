"""
新浪 hq.sinajs.cn 批量查询（最终完整版 - 输出已按你的要求改造）
红色代表涨，绿色代表跌
"""

from __future__ import annotations

import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests

_DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
_SINA_HEADERS = {
    "User-Agent": _DEFAULT_UA,
    "Referer": "https://finance.sina.com.cn/",
}
_POLL_INTERVAL_SEC = 180
_US_FIRST_AT_NIGHT = True
_BATCH_SIZE = 40

_LINE_RE = re.compile(
    r'var hq_str_((?:sh|sz)\d{6}|hk\d{5}|hk[A-Z][A-Z0-9]*|(?:gb|usr)_[a-z0-9._-]+)="([^"]*)"'
)

_SINA_HQ_ALIASES: dict[str, str] = {
    "上证": "sh000001",
    "上证指数": "sh000001",
    "深指": "sz399001",
    "深证": "sz399001",
    "深证成指": "sz399001",
    "恒生科技": "hkHSTECH",
    "恒生科技指数": "hkHSTECH",
    "HSTECH": "hkHSTECH",
    "HKHSTECH": "hkHSTECH",
}

_INDEX_HQ_CODES = frozenset({"sh000001", "sz399001", "hkHSTECH"})
_US_EASTERN_TZ = ZoneInfo("America/New_York")
_YAHOO_QUOTE_API = "https://query1.finance.yahoo.com/v7/finance/quote"
_YAHOO_QUOTE_API_FALLBACK = "https://query2.finance.yahoo.com/v7/finance/quote"

_WATCHLIST_TXT_NAME = "watchlist.local"

DEFAULT_WATCHLIST: list[str] = [
    "上证",
    "深指",
    "恒生科技",
    "600519",
    "00700.HK",
    "AAPL",
]


def load_watchlist_from_csv_file(path: Path) -> list[str] | None:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return None
    flat = re.sub(r"[\r\n]+", ",", text)
    out: list[str] = []
    for part in flat.split(","):
        s = part.strip()
        if s and not s.startswith("#"):
            out.append(s)
    return out if out else None


def _default_watchlist_base_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def load_watchlist_or_default(path: Path | None = None) -> tuple[list[str], str]:
    p = (path or _default_watchlist_base_dir() / _WATCHLIST_TXT_NAME).resolve()
    got = load_watchlist_from_csv_file(p)
    if got is not None:
        return got, str(p)
    return list(DEFAULT_WATCHLIST), "内置默认 DEFAULT_WATCHLIST"


def _normalize_sina_code(symbol: str) -> str:
    raw = symbol.strip()
    if not raw:
        raise ValueError("股票代码为空")
    if raw in _SINA_HQ_ALIASES:
        return _SINA_HQ_ALIASES[raw]
    t = raw.upper().replace(" ", "")
    if t in _SINA_HQ_ALIASES:
        return _SINA_HQ_ALIASES[t]

    m = re.fullmatch(r"(?:GB|USR)_([A-Z0-9._-]+)", t)
    if m:
        return f"gb_{m.group(1).lower()}"

    m = re.fullmatch(r"([A-Z][A-Z0-9._-]{0,14})\.(?:US|NYSE|NASDAQ)", t)
    if m:
        return f"gb_{m.group(1).lower()}"

    m = re.fullmatch(r"HK(\d{1,5})", t)
    if m:
        return "hk" + m.group(1).zfill(5)
    m = re.fullmatch(r"(\d{1,5})\.HK", t)
    if m:
        return "hk" + m.group(1).zfill(5)

    if re.fullmatch(r"(SH|SZ)\d{6}", t):
        return t.lower()
    m = re.fullmatch(r"(\d{6})\.(SH|SZ|SS)", t)
    if m:
        six, ex = m.group(1), m.group(2).replace("SS", "SH")
        return f"{ex.lower()}{six}"

    m = re.fullmatch(r"\d{6}", t)
    if m:
        six = m.group(0)
        if six.startswith(("600","601","603","605","688","689","510","511","512","513","515","516","518","560","561","562","563","588")):
            return f"sh{six}"
        return f"sz{six}"

    m = re.fullmatch(r"\d{1,5}", t)
    if m:
        return "hk" + m.group(0).zfill(5)

    m = re.fullmatch(r"[A-Z][A-Z0-9._-]{0,14}", t)
    if m:
        return f"gb_{t.lower()}"

    raise ValueError(f"无法解析为 A 股、港股或美股代码: {symbol}")


def _us_market_session_label(now_et: datetime | None = None) -> str:
    t = now_et or datetime.now(_US_EASTERN_TZ)
    if t.weekday() >= 5:
        return "CLOSED"
    hm = t.hour * 60 + t.minute
    if 4 * 60 <= hm < 9 * 60 + 30:
        return "PRE"
    if 9 * 60 + 30 <= hm < 16 * 60:
        return "REG"
    if 16 * 60 <= hm < 20 * 60:
        return "POST"
    return "CLOSED"


def _safe_float(v: Any) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return float("nan")


def _fetch_us_extended_from_yahoo(us_codes: list[str], session: requests.Session) -> dict[str, dict[str, Any]]:
    if not us_codes:
        return {}
    yahoo_symbols = []
    code_map: dict[str, str] = {}
    for c in us_codes:
        sym = c.split("_", 1)[1].upper() if "_" in c else c.upper()
        yahoo_symbols.append(sym)
        code_map[sym] = c
    data = None
    for url in (_YAHOO_QUOTE_API, _YAHOO_QUOTE_API_FALLBACK):
        try:
            r = session.get(url, params={"symbols": ",".join(yahoo_symbols)}, headers={"User-Agent": _DEFAULT_UA, "Accept": "application/json"}, timeout=15)
            if r.status_code == 200:
                data = r.json()
                break
        except:
            continue
    if data is None:
        return {}
    result = {}
    items = (data.get("quoteResponse") or {}).get("result") or []
    for it in items:
        sym = str(it.get("symbol") or "").upper()
        code = code_map.get(sym)
        if not code:
            continue
        result[code] = {
            "pre_price": _safe_float(it.get("preMarketPrice")),
            "pre_pct": _safe_float(it.get("preMarketChangePercent")),
            "post_price": _safe_float(it.get("postMarketPrice")),
            "post_pct": _safe_float(it.get("postMarketChangePercent")),
        }
    return result


def _parse_hq_inner_us(sina_code: str, inner: str) -> dict[str, Any]:
    if not inner.strip():
        return {"code": sina_code, "name": "(无返回)", "price": float("nan"), "pct": float("nan")}
    parts = inner.split(",")
    name = parts[0]
    price = float(parts[1]) if len(parts) > 1 and parts[1] else float("nan")
    pct = float(parts[2]) if len(parts) > 2 and parts[2] else float("nan")
    return {"code": sina_code, "name": name, "price": price, "pct": pct}


def _parse_hq_inner_hk(sina_code: str, inner: str) -> dict[str, Any]:
    if not inner.strip():
        return {"code": sina_code, "name": "(无返回)", "price": float("nan"), "pct": float("nan")}
    parts = inner.split(",")
    name = parts[1] if len(parts) > 1 and parts[1] else parts[0]
    price = float(parts[6]) if len(parts) > 6 and parts[6] else float("nan")
    pct = float(parts[8]) if len(parts) > 8 and parts[8] else float("nan")
    return {"code": sina_code, "name": name, "price": price, "pct": pct}


def _parse_hq_inner_cn(sina_code: str, inner: str) -> dict[str, Any]:
    if not inner.strip():
        return {"code": sina_code, "name": "(无返回)", "price": float("nan"), "pct": float("nan")}
    parts = inner.split(",")
    name = parts[0]
    prev = float(parts[2]) if len(parts) > 2 and parts[2] else 0.0
    price = float(parts[3]) if len(parts) > 3 and parts[3] else 0.0
    pct = (price - prev) / prev * 100.0 if prev else 0.0
    return {"code": sina_code, "name": name, "price": price, "pct": pct}


def _parse_hq_inner(sina_code: str, inner: str) -> dict[str, Any]:
    if sina_code.startswith("gb_") or sina_code.startswith("usr_"):
        return _parse_hq_inner_us(sina_code, inner)
    if sina_code.startswith("hk"):
        return _parse_hq_inner_hk(sina_code, inner)
    return _parse_hq_inner_cn(sina_code, inner)


def fetch_sina_spot_batch(symbols: list[str], session: requests.Session | None = None) -> list[dict[str, Any]]:
    sina_list = [_normalize_sina_code(s) for s in symbols]
    sess = session or requests.Session()
    sess.headers.update(_SINA_HEADERS)

    by_code: dict[str, dict[str, Any]] = {}
    for i in range(0, len(sina_list), _BATCH_SIZE):
        chunk = sina_list[i : i + _BATCH_SIZE]
        url = "https://hq.sinajs.cn/list=" + ",".join(chunk)
        r = sess.get(url, timeout=20)
        r.encoding = "gbk"
        for line in r.text.splitlines():
            m = _LINE_RE.search(line)
            if not m:
                continue
            code, inner = m.group(1), m.group(2)
            by_code[code] = _parse_hq_inner(code, inner)

    out: list[dict[str, Any]] = []
    for c in sina_list:
        if c in by_code:
            out.append(by_code[c])
        else:
            out.append({"code": c, "name": "(未返回)", "price": float("nan"), "pct": float("nan")})
    return out


# ====================== 只修改输出格式 ======================
def _fmt_cell(q: dict[str, Any]) -> str:
    name = (q.get("name") or "")[:8]
    price = q.get("price")
    pct = q.get("pct")
    code = q.get("code", "")

    if code in _INDEX_HQ_CODES:
        tag = "[指]"
    elif q.get("market") == "HK":
        tag = "[H]"
    elif q.get("market") == "US":
        tag = "[U]"
    else:
        tag = "[A]"

    # 涨跌颜色
    if pct is None or pct != pct:
        color = "⚪"
        pct_str = "--.--%"
    elif pct > 0:
        color = "🔵"   # 红色 = 涨
        pct_str = f"+{pct:.2f}%"
    elif pct < 0:
        color = "🟫"   # 绿色 = 跌
        pct_str = f"{pct:.2f}%"
    else:
        color = "⚪"
        pct_str = "0.00%"

    price_str = f"{price:.2f}" if price == price else "--.--"

    return f"{tag} {code:8} {name:8} {price_str:8} {color} {pct_str}"


def print_three_per_line(quotes: list[dict[str, Any]], sep: str = "  |  ") -> None:
    for i in range(0, len(quotes), 1):  # 改为1，每次只处理一个元素
        chunk = quotes[i:i + 1]  # 改为1，只取一个元素
        print(sep.join(_fmt_cell(q) for q in chunk))


def main() -> None:
    watchlist, watch_src = load_watchlist_or_default()
    sess = requests.Session()
    sess.headers.update(_SINA_HEADERS)
    print(
        f"新浪实时行情（🔵涨 🟫跌） | 每 {_POLL_INTERVAL_SEC // 60} 分钟刷新\n"
        f"自选来源：{watch_src}，共 {len(watchlist)} 条\n",
        flush=True,
    )
    try:
        while True:
            print(f"--- {datetime.now():%Y-%m-%d %H:%M:%S} ---", flush=True)
            rows = fetch_sina_spot_batch(watchlist, session=sess)
            
            # 对数据进行排序：指数放在最前面，其他按涨幅降序排列
            sorted_rows = sort_quotes_by_index_and_change(rows)
            
            print_three_per_line(sorted_rows)
            print(flush=True)
            time.sleep(_POLL_INTERVAL_SEC)
    except KeyboardInterrupt:
        print("\n已停止轮询。", flush=True)


def sort_quotes_by_index_and_change(quotes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """对股票数据进行排序：指数放在最前面，其他按涨幅降序排列"""
    # 分离指数和非指数
    indices = []
    others = []
    
    for quote in quotes:
        code = quote.get("code", "")
        # 判断是否为指数
        if code in _INDEX_HQ_CODES:
            indices.append(quote)
        else:
            others.append(quote)
    
    # 对非指数股票按涨幅排序（降序）
    others.sort(key=lambda x: x.get("pct", 0) or 0, reverse=True)
    
    # 合并结果：指数在前，其他股票按涨幅排序在后
    return indices + others


if __name__ == "__main__":
    main()