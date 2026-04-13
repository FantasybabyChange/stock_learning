"""
A 股主要大盘指数日线数据（AkShare）。

默认使用新浪 `stock_zh_index_daily`（symbol 为 sh000001 / sz399001 等），一般比东方财富接口更稳。
可选：指定日期区间时先尝试东方财富 `index_zh_a_hist`，失败则退回新浪全量后本地筛选。

实时行情：`stock_zh_index_spot_sina` 拉全市场指数快照后按新浪代码筛选（如上证指数 sh000001）。
非交易时段一般为最近一笔行情或昨收附近，与行情软件「当前价」一致取决于数据源。

个股：`stock_individual_info_em` 查询东财个股资料；行情可优先东财 `stock_zh_a_spot_em` 按代码筛选，
失败则用新浪 `stock_zh_a_spot`（全市场分页，首次较慢，模块内缓存同进程复用）。
"""
from __future__ import annotations

import re
from datetime import datetime

import akshare as ak
import pandas as pd

# 常用大盘指数：名称 -> 新浪 symbol（AkShare stock_zh_index_daily）
CN_INDEX_SINA: dict[str, str] = {
    "上证指数": "sh000001",
    "深证成指": "sz399001",
    "创业板指": "sz399006",
    "沪深300": "sh000300",
    "中证500": "sh000905",
    "科创50": "sh000688",
    "上证50": "sh000016",
}

# 同上：东方财富指数代码（AkShare index_zh_a_hist 的 symbol）
CN_INDEX_EM: dict[str, str] = {
    "上证指数": "000001",
    "深证成指": "399001",
    "创业板指": "399006",
    "沪深300": "000300",
    "中证500": "000905",
    "科创50": "000688",
    "上证50": "000016",
}

# 同进程内复用新浪 A 股全市场行情（`stock_zh_a_spot` 会分页拉全市场，避免重复请求）
_spot_zh_a_sina_cache: pd.DataFrame | None = None


def parse_a_share_symbol(symbol: str) -> tuple[str, str]:
    """
    解析 A 股输入，返回 (东财六位代码, 新浪代码)。

    支持：600519、sh600519、SZ000001、600519.SH、000001.SZ 等。
    未带交易所时按常见规则推断：600/601/603/605/688/689 等视为上证，其余默认深证。
    """
    t = symbol.strip().upper().replace(" ", "")
    if not t:
        raise ValueError("股票代码为空")

    if re.fullmatch(r"(SH|SZ)\d{6}", t):
        six = t[2:8]
        return six, f"{t[:2].lower()}{six}"

    m = re.fullmatch(r"(\d{6})\.(SH|SZ|SS)", t)
    if m:
        six, ex = m.group(1), m.group(2).replace("SS", "SH")
        return six, f"{ex.lower()}{six}"

    m = re.fullmatch(r"\d{6}", t)
    if m:
        six = m.group(0)
        if six.startswith(("600", "601", "603", "605", "688", "689", "510", "511", "512", "513", "515", "516", "518", "560", "561", "562", "563", "588")):
            return six, f"sh{six}"
        return six, f"sz{six}"

    raise ValueError(f"无法解析为 A 股代码: {symbol}")


def fetch_stock_info_em(symbol: str) -> pd.DataFrame:
    """东方财富个股资料，返回 item / value 两列。"""
    em_code, _ = parse_a_share_symbol(symbol)
    return ak.stock_individual_info_em(symbol=em_code)


def _get_spot_zh_a_sina_full() -> pd.DataFrame:
    global _spot_zh_a_sina_cache
    if _spot_zh_a_sina_cache is None:
        _spot_zh_a_sina_cache = ak.stock_zh_a_spot()
    return _spot_zh_a_sina_cache


def fetch_stock_spot_sina(symbol: str) -> pd.Series:
    """新浪 A 股行情快照中的一行（全市场拉取 + 缓存）。"""
    _, sina_code = parse_a_share_symbol(symbol)
    df = _get_spot_zh_a_sina_full()
    row = df.loc[df["代码"] == sina_code]
    if row.empty:
        raise ValueError(f"新浪行情中未找到股票: {sina_code}")
    return row.iloc[0]


def fetch_stock_spot_em(symbol: str) -> pd.Series:
    """东财 A 股全表行情中筛选一行；需网络可达东财分页接口。"""
    em_code, _ = parse_a_share_symbol(symbol)
    df = ak.stock_zh_a_spot_em()
    row = df.loc[df["代码"].astype(str) == em_code]
    if row.empty:
        raise ValueError(f"东财行情中未找到股票代码: {em_code}")
    return row.iloc[0]


def fetch_stock_spot(symbol: str, prefer_em: bool = True) -> pd.Series:
    """
    A 股实时/最新快照。默认先东财，失败再新浪（新浪首次会较慢）。
    """
    if prefer_em:
        try:
            return fetch_stock_spot_em(symbol)
        except Exception:
            pass
    return fetch_stock_spot_sina(symbol)


def _parse_yyyymmdd(s: str) -> pd.Timestamp:
    return pd.Timestamp(datetime.strptime(s, "%Y%m%d"))


def fetch_cn_index_sina(
    symbol: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """
    从新浪拉取指数日线。symbol 如 sh000001、sz399001。

    start_date / end_date：可选，格式 YYYYMMDD；不传则返回接口可用的全部历史。
    """
    df = ak.stock_zh_index_daily(symbol=symbol)
    if df.empty:
        return df

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])

    if start_date:
        df = df[df["date"] >= _parse_yyyymmdd(start_date)]
    if end_date:
        df = df[df["date"] <= _parse_yyyymmdd(end_date)]

    df = df.sort_values("date").reset_index(drop=True)
    return df


def fetch_cn_index_em(
    symbol_em: str,
    start_date: str = "19900101",
    end_date: str | None = None,
    period: str = "daily",
) -> pd.DataFrame:
    """东方财富 A 股指数日线；需网络可达东财接口。"""
    if end_date is None:
        end_date = datetime.now().strftime("%Y%m%d")
    return ak.index_zh_a_hist(
        symbol=symbol_em,
        period=period,
        start_date=start_date,
        end_date=end_date,
    )


def fetch_cn_index_by_name(
    name: str,
    start_date: str | None = None,
    end_date: str | None = None,
    prefer_em: bool = False,
) -> pd.DataFrame:
    """
    按中文名称拉取指数日线。

    prefer_em=True 时优先东财（适合只要一段日期、且网络畅通）；否则只用新浪。
    """
    if name not in CN_INDEX_SINA:
        known = ", ".join(sorted(CN_INDEX_SINA))
        raise ValueError(f"未知指数名称: {name}。可选: {known}")

    sina_sym = CN_INDEX_SINA[name]
    em_sym = CN_INDEX_EM[name]

    if prefer_em and start_date and end_date:
        try:
            return fetch_cn_index_em(
                symbol_em=em_sym,
                start_date=start_date,
                end_date=end_date,
            )
        except Exception:
            pass

    return fetch_cn_index_sina(
        symbol=sina_sym,
        start_date=start_date,
        end_date=end_date,
    )


def fetch_cn_index_spot_sina(sina_code: str) -> pd.Series:
    """
    从新浪获取指定指数的当前快照（一行）。sina_code 如 sh000001（上证）、sz399001（深证）。
    """
    df = ak.stock_zh_index_spot_sina()
    row = df.loc[df["代码"] == sina_code]
    if row.empty:
        raise ValueError(f"新浪行情中未找到指数代码: {sina_code}")
    return row.iloc[0]


def fetch_cn_index_spot_by_name(name: str) -> pd.Series:
    """按 `CN_INDEX_SINA` 中的中文名称取实时快照。"""
    if name not in CN_INDEX_SINA:
        known = ", ".join(sorted(CN_INDEX_SINA))
        raise ValueError(f"未知指数名称: {name}。可选: {known}")
    return fetch_cn_index_spot_sina(CN_INDEX_SINA[name])


def fetch_cn_index_spot_em_main(code: str = "000001") -> pd.Series:
    """
    东方财富「沪深重要指数」列表中的实时一行。上证指数代码一般为 000001。
    需网络可达东财；失败时请改用 `fetch_cn_index_spot_sina`。
    """
    df = ak.stock_zh_index_spot_em(symbol="沪深重要指数")
    row = df.loc[df["代码"].astype(str) == str(code)]
    if row.empty:
        raise ValueError(f"东财沪深重要指数中未找到代码: {code}")
    return row.iloc[0]


def shanghai_index_spot_realtime(prefer_em: bool = False) -> pd.Series:
    """
    上证指数实时/最新快照。

    默认新浪；`prefer_em=True` 时先尝试东财「沪深重要指数」，失败则退回新浪。
    """
    if prefer_em:
        try:
            return fetch_cn_index_spot_em_main("000001")
        except Exception:
            pass
    return fetch_cn_index_spot_sina("sh000001")


if __name__ == "__main__":
    end = datetime.now().strftime("%Y%m%d")
    start = "20240101"

    for idx_name in ("上证指数", "深证成指", "创业板指"):
        dfi = fetch_cn_index_by_name(idx_name, start_date=start, end_date=end)
        print(f"\n=== {idx_name} ({CN_INDEX_SINA[idx_name]}) | {len(dfi)} 条 ===")
        if not dfi.empty:
            print(dfi.tail(5).to_string(index=False))

    print("\n=== 上证指数 实时快照（新浪 sh000001）===")
    spot = shanghai_index_spot_realtime(prefer_em=False)
    print(spot.to_string())

    print("\n=== 个股示例：600256 资料（东财）===")
    try:
        info = fetch_stock_info_em("600519")
        print(info.to_string(index=False))
    except Exception as e:
        print(f"个股资料拉取失败: {e}")

    print("\n=== 个股示例：600256 行情（优先东财，失败则新浪）===")
    try:
        q = fetch_stock_spot("600256", prefer_em=True)
        print(q.to_string())
    except Exception as e:
        print(f"行情拉取失败: {e}")
