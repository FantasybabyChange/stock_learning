import yfinance as yf
import pandas as pd
from datetime import datetime

def download_and_save_offline(ticker="0700.HK", period="max", filename=None):
    """
    下载雅虎历史数据并保存为离线 CSV
    period 可选: '1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y', '10y', 'ytd', 'max'
    """
    print(f"正在从 Yahoo Finance 下载 {ticker} 数据...（需网络）")
    
    # 方法1：用 Ticker.history（推荐，包含股息、分拆等）
    stock = yf.Ticker(ticker)
    df = stock.history(period=period, auto_adjust=True)  # auto_adjust=True 自动调整拆股/分红
    
    if df.empty:
        print("❌ 下载失败或无数据，请检查 ticker 或网络")
        return None
    
    # 方法2（备选）：用 yf.download，支持多只股票
    # df = yf.download(tickers=ticker, period=period, auto_adjust=True)
    
    # 添加简单指标供离线分析
    import pandas_ta as ta
    df['SMA20'] = ta.sma(df['Close'], length=20)
    df['RSI'] = ta.rsi(df['Close'], length=14)
    
    # 生成文件名（带日期方便管理）
    if filename is None:
        filename = f"{ticker.replace('.HK','')}_yahoo_history_{datetime.now().strftime('%Y%m%d')}.csv"
    
    df.to_csv(filename)
    print(f"✅ 下载成功！已保存到本地离线文件：{filename}")
    print(f"数据范围：{df.index[0].date()} 到 {df.index[-1].date()} | 共 {len(df)} 条记录")
    print(f"最新收盘价（上一个交易日）：{df['Close'].iloc[-1]:.2f}")
    
    return df

# ==================== 使用示例 ====================
if __name__ == "__main__":
    # 下载腾讯控股（港股）2年数据
    df = download_and_save_offline(ticker="0700.HK", period="2y")
    
    # 离线读取示例（以后无需网络）
    # df_offline = pd.read_csv("00700_yahoo_history_20260412.csv", index_col=0, parse_dates=True)
    # print(df_offline.tail())