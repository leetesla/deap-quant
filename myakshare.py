import time

import akshare as ak
import os
import pandas as pd
import sqlite3

BASE_DIR = 'fin-data'

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

def get_symbol_map(symbols):
  pass

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

      stock_df = ak.stock_us_hist(symbol=symbol, period="daily", start_date="20200101", end_date="20250823",
                                          adjust="qfq")
      print(stock_df[:10])
      exit()
      # 添加股票代码列
      stock_df['symbol'] = symbol
      # 左边添加date列:使用REPORT_DATE列数据填充
      stock_df['date'] = stock_df['REPORT_DATE']

      # 保存到CSV文件
      stock_df.to_csv(f'{csv_dir}/{symbol}.csv', index=False)

      # 合并到总数据中
      all_stocks_data = pd.concat([all_stocks_data, stock_df], ignore_index=True)
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

def get_name():
  # db_name = 'pv'
  # table_name = 'pv_data'
  csv_dir = f'{BASE_DIR}/spot'

  os.makedirs(csv_dir, exist_ok=True)
  stock_us_spot_em_df = ak.stock_us_spot_em()

  print(stock_us_spot_em_df[:10])

  # 保存到CSV文件
  stock_us_spot_em_df.to_csv(f'{csv_dir}/stock_us_spot_em.csv', index=False)

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

  spots = ["105.NVDA", "105.META", "", "", ""]
  # get_fnd_data(conn, symbols)
  # get_pv_data(conn, symbols)
  # get_name()
  get_symbol_map(symbols)
  # 关闭数据库连接
  conn.close()


