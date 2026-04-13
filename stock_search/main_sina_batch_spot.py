"""
新浪 hq.sinajs.cn 批量查询 A 股 + 港股实时行情（仅请求给定代码，不拉全市场）。

- A 股：sh/sz + 6 位，如 sh600519、sz000001
- 港股：hk + 5 位，如 hk00700（腾讯）；支持 00700.HK、hk00700、1810、01810 等写法
- 主要指数：上证 sh000001、深指 sz399001、恒生科技 hkHSTECH；也可用别名「上证」「深指」「恒生科技」等

控制台输出：每行 3 条（股/指数混排）。[指]=大盘指数，[H]=港股。直接运行本文件时默认每 3 分钟拉取一轮（`_POLL_INTERVAL_SEC`）。

自选列表：优先读取与本脚本同目录下的 `watchlist.txt`（UTF-8，逗号分隔，可多行；`#` 开头的片段忽略）。读不到或解析为空时使用内置 `DEFAULT_WATCHLIST`。

依赖：requests（随 akshare 环境一般已有）。
"""
from __future__ import annotations

import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

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
_BATCH_SIZE = 40
# 港股除5 位数字代码外，还有字母类代码（如恒生科技 hkHSTECH）
_LINE_RE = re.compile(r'var hq_str_((?:sh|sz)\d{6}|hk\d{5}|hk[A-Z][A-Z0-9]*)="([^"]*)"')

# 常用指数/别名 → 新浪 hq 代码（与个股同一接口）
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

_WATCHLIST_TXT_NAME = "watchlist.local"

DEFAULT_WATCHLIST: list[str] = [
    "上证",
    "深指",
    "恒生科技",
    "600519",
    "00700.HK"
]


def load_watchlist_from_csv_file(path: Path) -> list[str] | None:
    """
    从文本文件读取逗号分隔代码；成功且至少有一条则返回列表，否则返回 None。
    读文件失败（不存在、无权限等）返回 None。
    """
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
    """
    开发：包内 `stock_search/` 目录。
    PyInstaller 单文件 exe：exe 所在目录（自选文件与 exe 放一起即可）。
    """
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def load_watchlist_or_default(path: Path | None = None) -> tuple[list[str], str]:
    """
    优先从 `path` 读取；未指定则为默认目录下的自选文件（见 `_WATCHLIST_TXT_NAME`）。
    返回 (列表, 来源说明)。
    """
    p = (path or _default_watchlist_base_dir() / _WATCHLIST_TXT_NAME).resolve()
    got = load_watchlist_from_csv_file(p)
    if got is not None:
        return got, str(p)
    return list(DEFAULT_WATCHLIST), "内置默认 DEFAULT_WATCHLIST"


def _normalize_sina_code(symbol: str) -> str:
    """
    返回新浪 hq 代码：A 股 sh600519 / sz000001，港股 hk00700 / hkHSTECH，或主要指数别名。
    """
    raw = symbol.strip()
    if not raw:
        raise ValueError("股票代码为空")
    if raw in _SINA_HQ_ALIASES:
        return _SINA_HQ_ALIASES[raw]
    t = raw.upper().replace(" ", "")
    if t in _SINA_HQ_ALIASES:
        return _SINA_HQ_ALIASES[t]

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
        if six.startswith(
            (
                "600",
                "601",
                "603",
                "605",
                "688",
                "689",
                "510",
                "511",
                "512",
                "513",
                "515",
                "516",
                "518",
                "560",
                "561",
                "562",
                "563",
                "588",
            )
        ):
            return f"sh{six}"
        return f"sz{six}"

    m = re.fullmatch(r"\d{1,5}", t)
    if m:
        return "hk" + m.group(0).zfill(5)

    raise ValueError(f"无法解析为 A 股或港股代码: {symbol}")


def _parse_hq_inner_hk(sina_code: str, inner: str) -> dict[str, Any]:
    """港股字段：英文名,中文名,今开,昨收,最高,最低,最新价,涨跌额,涨跌%, ...日期,时间"""
    if not inner.strip():
        return {
            "code": sina_code,
            "name": "(无返回)",
            "price": float("nan"),
            "pct": float("nan"),
            "volume_hands": float("nan"),
            "time": "",
            "market": "HK",
        }
    parts = inner.split(",")
    name = parts[1] if len(parts) > 1 and parts[1] else parts[0]
    try:
        price = float(parts[6]) if len(parts) > 6 and parts[6] else float("nan")
        pct = float(parts[8]) if len(parts) > 8 and parts[8] else float("nan")
    except ValueError:
        return {
            "code": sina_code,
            "name": name or "(解析失败)",
            "price": float("nan"),
            "pct": float("nan"),
            "volume_hands": float("nan"),
            "time": "",
            "market": "HK",
        }
    vol = float("nan")
    if len(parts) > 11 and parts[11]:
        try:
            vol = float(parts[11])
        except ValueError:
            pass
    tstr = ""
    if len(parts) > 18:
        tstr = f"{parts[17]} {parts[18]}".strip()
    return {
        "code": sina_code,
        "name": name,
        "price": price,
        "pct": pct,
        "volume_hands": vol,
        "time": tstr,
        "market": "HK",
    }


def _parse_hq_inner_cn(sina_code: str, inner: str) -> dict[str, Any]:
    """A 股字段：名称, ... 昨收、现价等（与新浪 A 股列表一致）。"""
    if not inner.strip():
        return {
            "code": sina_code,
            "name": "(无返回)",
            "price": float("nan"),
            "pct": float("nan"),
            "volume_hands": float("nan"),
            "time": "",
            "market": "CN",
        }
    parts = inner.split(",")
    name = parts[0]
    try:
        prev = float(parts[2]) if len(parts) > 2 and parts[2] else 0.0
        price = float(parts[3]) if len(parts) > 3 and parts[3] else 0.0
    except ValueError:
        return {
            "code": sina_code,
            "name": name or "(解析失败)",
            "price": float("nan"),
            "pct": float("nan"),
            "volume_hands": float("nan"),
            "time": "",
            "market": "CN",
        }
    pct = (price - prev) / prev * 100.0 if prev else 0.0
    vol = float("nan")
    if len(parts) > 8 and parts[8]:
        try:
            vol = float(parts[8])
        except ValueError:
            pass
    tstr = ""
    if len(parts) > 31:
        tstr = f"{parts[30]} {parts[31]}".strip()
    return {
        "code": sina_code,
        "name": name,
        "price": price,
        "pct": pct,
        "volume_hands": vol,
        "time": tstr,
        "market": "CN",
    }


def _parse_hq_inner(sina_code: str, inner: str) -> dict[str, Any]:
    if sina_code.startswith("hk"):
        return _parse_hq_inner_hk(sina_code, inner)
    return _parse_hq_inner_cn(sina_code, inner)


def fetch_sina_spot_batch(symbols: list[str], session: requests.Session | None = None) -> list[dict[str, Any]]:
    """
    按用户给定顺序返回行情字典列表。

    symbols 示例：600519、000001.SZ、00700.HK、上证、深指、恒生科技、HSTECH。
    """
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
            out.append(
                {
                    "code": c,
                    "name": "(未返回)",
                    "price": float("nan"),
                    "pct": float("nan"),
                    "volume_hands": float("nan"),
                    "time": "",
                    "market": "HK" if c.startswith("hk") else "CN",
                }
            )
    return out


def _fmt_cell(q: dict[str, Any]) -> str:
    name = (q.get("name") or "")[:8]
    price = q.get("price")
    pct = q.get("pct")
    code = q.get("code", "")
    if code in _INDEX_HQ_CODES:
        tag = "[指]"
    elif q.get("market") == "HK":
        tag = "[H]"
    else:
        tag = ""
    if price != price:  # NaN
        return f"{tag}{code} {name}".strip()
    return f"{tag}{code} {name} {price:.2f} {pct:+.2f}%"


def print_three_per_line(quotes: list[dict[str, Any]], sep: str = "  |  ") -> None:
    """每行输出 3 只股票。"""
    for i in range(0, len(quotes), 3):
        chunk = quotes[i : i + 3]
        print(sep.join(_fmt_cell(q) for q in chunk))


def main() -> None:
    watchlist, watch_src = load_watchlist_or_default()
    sess = requests.Session()
    sess.headers.update(_SINA_HEADERS)
    print(
        f"新浪实时（hq.sinajs.cn，A+H+指数），每行 3 条：[指]=指数、[H]=港股；"
        f"每 {_POLL_INTERVAL_SEC // 60} 分钟刷新，Ctrl+C 结束\n"
        f"自选来源：{watch_src}，共 {len(watchlist)} 条\n",
        flush=True,
    )
    try:
        while True:
            print(f"--- {datetime.now():%Y-%m-%d %H:%M:%S} ---", flush=True)
            rows = fetch_sina_spot_batch(watchlist, session=sess)
            print_three_per_line(rows)
            print(flush=True)
            time.sleep(_POLL_INTERVAL_SEC)
    except KeyboardInterrupt:
        print("\n已停止轮询。", flush=True)


if __name__ == "__main__":
    main()
