# stock_learning
learn stock 
UV uv 0.11.6
Python 3.13
uv sync --extra exe
uv run pyinstaller --noconfirm --clean --onefile --console --name sina_batch_spot `
  --collect-all requests --collect-all certifi --collect-all urllib3 `
  run_sina_spot_exe.py