import akshare as ak
import pandas as pd
print('AkShare 版本:', ak.__version__)
print('Pandas 版本:', pd.__version__)
import yfinance as yf
import akshare as ak
import pandas as pd
import time
from datetime import datetime

def get_stock_history_safe(ticker="0700.HK", period="1y", max_retries=3):
    """
    安全拉取港股历史数据：
    - 优先 yfinance（休盘时最稳定，能拿到上一个交易日收盘数据）
    - 备用 AkShare（带重试和延时）
    - 自动处理休盘/空数据
    """
    symbol_ak = ticker.replace(".HK", "").zfill(5)  # AkShare 需要 00700 格式
    
    # 步骤1: 优先用 yfinance（推荐，周末/休盘时可靠）
    try:
        df = yf.Ticker(ticker).history(period=period, auto_adjust=True)
        if not df.empty:
            latest_price = df['Close'].iloc[-1]
            print(f"✅ yfinance 获取成功 | {ticker} 最新收盘价: {latest_price:.2f} HKD | 数据日期: {df.index[-1].date()}")
            return df
    except Exception as e:
        print(f"yfinance 尝试失败: {e}")
    
    # 步骤2: AkShare 备用（带指数退避重试）
    for attempt in range(max_retries):
        try:
            df = ak.stock_hk_hist(
                symbol=symbol_ak,
                period="daily",
                start_date="20250101",      # 根据需要调整
                end_date=datetime.now().strftime("%Y%m%d")
            )
            
            if df is None or df.empty:
                print(f"⚠️ AkShare 返回空数据（可能是休盘或无交易日）")
                break
            
            print(f"✅ AkShare 获取成功 | 共 {len(df)} 条记录")
            return df
            
        except Exception as e:
            print(f"❌ AkShare 第 {attempt+1}/{max_retries} 次失败: {e}")
            time.sleep(3 * (attempt + 1))  # 逐步增加等待时间
    
    # 最终兜底：返回空或提示
    raise Exception(f"❌ 双源均失败（当前为休盘期），建议周一早盘后重试或检查网络")

# ==================== 使用示例（直接替换你之前的代码） ====================
if __name__ == "__main__":
    try:
        df = get_stock_history_safe(ticker="0700.HK", period="2y")
        
        # 继续计算技术指标（MACD、RSI 等）
        import pandas_ta as ta
        df['RSI'] = ta.rsi(df['Close'], length=14)
        df['MACD'] = ta.macd(df['Close'])['MACD_12_26_9']
        
        print("\n最近5条数据 + 指标预览：")
        print(df[['Close', 'RSI', 'MACD']].tail(5))
        
        # 这里可以继续画图或生成信号...
        
    except Exception as e:
        print(f"整体拉取失败: {e}")
        print("💡 建议：今天是周日休盘，周一 9:30 开盘后再运行。或直接用 yfinance 历史数据做离线分析。")