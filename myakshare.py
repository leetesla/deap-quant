import time

import akshare as ak
import os
import pandas as pd
import sqlite3

BASE_DIR = 'fin-data'
SYMBOL_MAP_FILE = 'symbol-map.csv'

def get_fnd_data(conn_, symbols_):
  db_name = 'fnd'
  table_name = 'fnd_data'
  fnd_dir = f'{BASE_DIR}/fnd'

  os.makedirs(fnd_dir, exist_ok=True)

  # 创建一个空的DataFrame来存储所有股票数据
  all_stocks_data = pd.DataFrame()
  # 获取所有股票的财务数据
  for symbol in symbols_:
    try:
      print(f"************************* 开始获取 {symbol} 的数据 *************************")
      # 左边添加symbol列:使用SECURITY_CODE列数据填充
      # 数据保存到../data/quotes目录下，文件名{symbol}.csv
      # indicator="年报"; choice of {"年报", "单季报", "累计季报"}
      stock_financial_us_analysis_indicator_em_df = ak.stock_financial_us_analysis_indicator_em(symbol=symbol,
                                                                                                indicator="单季报")
      # 添加股票代码列
      stock_financial_us_analysis_indicator_em_df['symbol'] = symbol
      # 左边添加date列:使用REPORT_DATE列数据填充
      stock_financial_us_analysis_indicator_em_df['date'] = stock_financial_us_analysis_indicator_em_df['REPORT_DATE']

      # 保存到CSV文件
      stock_financial_us_analysis_indicator_em_df.to_csv(f'{fnd_dir}/{symbol}.csv', index=False)

      # 合并到总数据中
      all_stocks_data = pd.concat([all_stocks_data, stock_financial_us_analysis_indicator_em_df], ignore_index=True)
      print(f"************************* 已获取 {symbol} 的数据 *************************")
    except Exception as e:
      print(f"获取 {symbol} 数据时出错: {e}")
    finally:
      time.sleep(3)

  # 将所有数据保存到数据库中
  try:
    # 先删除已存在的表，避免数据重复
    conn_.execute('DROP TABLE IF EXISTS financial_data')
    # 保存新的数据
    all_stocks_data.to_sql(table_name, conn_, if_exists='replace', index=False)
    print(f"所有股票数据已保存到数据库 {BASE_DIR}/{db_name}.db 中的 {table_name} 表")
  except Exception as e:
    print(f"保存数据到数据库时出错: {e}")

def get_code_by_symbol(symbol):
  csv_path = f"{BASE_DIR}/{SYMBOL_MAP_FILE}"
  # 读取CSV文件
  df = pd.read_csv(csv_path)
  # 根据symbol查找对应的symbol_code
  result = df[df['symbol'] == symbol]
  if not result.empty:
    print(f"找到 {symbol} 对应的代码: {result.iloc[0]['symbol_code']}")
    return result.iloc[0]['symbol_code']
  else:
    print(f"--- 未找到 {symbol} 对应的代码 ---")
    return None

def get_symbol_map(symbols, stock_us_spot_em_file):
    # 读取stock_us_spot_em_file CSV文件
    spot_df = pd.read_csv(f'{BASE_DIR}/spot/{stock_us_spot_em_file}')
    output_file = SYMBOL_MAP_FILE

    # 创建一个空的DataFrame来存储结果
    result_data = pd.DataFrame()

    # 遍历每个symbol
    for symbol in symbols:
        # 筛选出"代码"列中以symbol结尾的行
        filtered_df = spot_df[spot_df['代码'].str.endswith(f".{symbol}", na=False)].copy()

        # 如果找到了匹配的行
        if not filtered_df.empty:
            # 添加symbol列
            filtered_df.loc[:, 'symbol'] = symbol
            # 重命名"代码"列为symbol_code
            filtered_df = filtered_df.rename(columns={'代码': 'symbol_code'})

            # 合并到结果数据中
            result_data = pd.concat([result_data, filtered_df[['symbol', 'symbol_code']]], ignore_index=True)
        else:
          print(f"***** {symbol}找不到对应的code *****")
    # 保存到新的CSV文件
    # os.makedirs(f'{BASE_DIR}/symbol_map', exist_ok=True)
    result_data.to_csv(f'{BASE_DIR}/{output_file}', index=False)
    print(f"符号映射已保存到 {BASE_DIR}/{output_file}")

def get_pv_data(conn_, symbols_):
  db_name = 'pv'
  table_name = 'pv_data'
  csv_dir = f'{BASE_DIR}/pv'

  os.makedirs(csv_dir, exist_ok=True)

  # 创建一个空的DataFrame来存储所有股票数据
  all_stocks_data = pd.DataFrame()
  # 获取所有股票的财务数据
  for symbol in symbols_:
    try:
      print(f"************************* 开始获取 {symbol} 的数据 *************************")
      symbol_code = get_code_by_symbol(symbol)
      stock_df = ak.stock_us_hist(symbol=symbol_code, period="daily", start_date="20200101", end_date="20250823",
                                          adjust="qfq")
      print(stock_df[:10])
      # 添加股票代码列
      stock_df['symbol'] = symbol
      # 使用映射替换df header，保留原header
      column_mapping = {
          '日期': 'date',
          '开盘': 'open',
          '收盘': 'close',
          '最高': 'high',
          '最低': 'low',
          '成交量': 'volume',
          '成交额': 'turnover',
          '振幅': 'amplitude',
          '涨跌幅': 'change_percent',
          '涨跌额': 'change_amount',
          '换手率': 'turnover_rate'
      }
      # 仅映射存在的列
      stock_df.columns = [column_mapping.get(col, col) for col in stock_df.columns]

      # 保存到CSV文件
      stock_df.to_csv(f'{csv_dir}/{symbol}.csv', index=False)

      # 合并到总数据中
      all_stocks_data = pd.concat([all_stocks_data, stock_df], ignore_index=True)
      print(f"************************* 已获取 {symbol} 的数据 *************************")
      # exit()
    except Exception as e:
      print(f"获取 {symbol} 数据时出错: {e}")
    finally:
      time.sleep(3)

  # 将所有数据保存到数据库中
  try:
    # 先删除已存在的表，避免数据重复
    conn_.execute('DROP TABLE IF EXISTS financial_data')
    # 保存新的数据
    all_stocks_data.to_sql(table_name, conn_, if_exists='replace', index=False)
    print(f"所有股票数据已保存到数据库 {BASE_DIR}/{db_name}.db 中的 {table_name} 表")
  except Exception as e:
    print(f"保存数据到数据库时出错: {e}")

def get_stock_us_spot_em_df(file_name):
  # db_name = 'pv'
  # table_name = 'pv_data'
  csv_dir = f'{BASE_DIR}/spot'

  os.makedirs(csv_dir, exist_ok=True)
  stock_us_spot_em_df = ak.stock_us_spot_em()

  print(stock_us_spot_em_df[:10])

  # 保存到CSV文件
  stock_us_spot_em_df.to_csv(f'{csv_dir}/{file_name}', index=False)

if __name__ == '__main__':
  print(ak.__version__)

  # 创建fin-data目录（如果不存在）
  os.makedirs(BASE_DIR, exist_ok=True)
  # fnd目录（如果不存在）

  # 创建SQLite数据库连接
  conn = sqlite3.connect('fin-data/stocks_financial_data.db')
  print("已连接到SQLite数据库")


  # 股票列表 - 可以根据需要扩展
  # symbols = ["NVDA", "TSLA", "AAPL", "GOOGL", "MSFT"]  # 示例股票列表
  symbols = ["NVDA", "MSFT", "AAPL", "GOOGL", "META", "AVGO", "TSLA", "BRK.A", "JPM", "WMT", "V", "LLY", "ORCL", "MA",
             "NFLX", "XOM", "JNJ", "COST", "HD", "PLTR", "ABBV", "PG", "BAC", "CVX", "KO", "TMUS", "GE", "UNH", "AMD",
             "PM", "CSCO", "WFC", "CRM", "MS", "ABT", "LIN", "IBM", "GS", "MCD", "AXP", "MRK", "DIS", "BX", "RTX", "T",
             "PEP", "CAT", "UBER", "TMO", "VZ", "BLK", "TXN", "BKNG", "INTU", "SHOP", "NOW", "C", "BA", "SCHW", "ISRG",
             "QCOM", "SPGI", "ANET", "GEV", "ACN", "AMGN", "BSX", "NEE", "ADBE", "TJX", "DHR", "SYK", "APP", "LOW",
             "PFE", "PGR", "GILD", "COF", "HON", "SPOT", "ETN", "UNP", "DE", "APH", "MU", "AMAT", "LRCX", "KKR",
             "CMCSA", "ADP", "ADI", "PANW", "MELI", "COP", "MDT", "NKE", "KLAC", "MO", "SNPS"]  # 示例股票列表
  print(len(symbols))


  stock_us_spot_em_file = "stock_us_spot_em.csv"
  # get_stock_us_spot_em_df(stock_us_spot_em_file)
  # get_symbol_map(symbols, stock_us_spot_em_file)
  # for symbol in symbols:
  #   get_code_by_symbol(symbol)

  # get_fnd_data(conn, symbols)
  get_pv_data(conn, symbols)

  # 关闭数据库连接
  conn.close()
