"""
控制台入口：供开发直接运行，以及 PyInstaller 打 exe 时使用。

构建（在项目根目录执行）：
  uv sync --extra exe
  uv run pyinstaller --noconfirm --clean --onefile --console --name sina_batch_spot ^
    --collect-all requests --collect-all certifi --collect-all urllib3 run_sina_spot_exe.py

产物：dist/sina_batch_spot.exe 。将 watchlist.local（逗号分隔自选）放在 exe 同目录即可覆盖内置列表。
"""
from stock_search.main_sina_batch_spot import main

if __name__ == "__main__":
    main()
