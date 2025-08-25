import pandas as pd
import os

def merge_price_and_fundamental_data(price_csv, data_dir = 'fin-data'):
  """
  将量价数据和基本面数据进行合并，并保存合并后的数据到文件。

  参数:
  data_dir (str): 数据文件所在的目录。
  price_csv (str): 量价数据的 CSV 文件名。

  返回:
  None
  """
  # data_dir = 'fin-data'
  # price_csv = 'AAPL-1.csv'

  # fundamental_csv = price_csv.replace('.csv', '-f.csv')
  # output_csv = price_csv.replace('.csv', '-merged.csv')
  fundamental_csv = price_csv
  output_csv = price_csv

  price_csv_path = f'{data_dir}/pv/{price_csv}'
  fundamental_csv_path = f'{data_dir}/fnd/{fundamental_csv}'
  output_csv_path = f'{data_dir}/merged/{output_csv}'

  # 读取量价数据
  price_df = pd.read_csv(price_csv_path, parse_dates=['date'])
  price_df.sort_values('date', inplace=True)

  # 读取基本面数据
  fundamental_df = pd.read_csv(fundamental_csv_path, parse_dates=['REPORT_DATE'])
  fundamental_df.sort_values('REPORT_DATE', inplace=True)

  # 只保留需要的列（示例，可根据需要调整）
  fundamental_cols = [
      'REPORT_DATE', 'OPERATE_INCOME', 'GROSS_PROFIT',
      'PARENT_HOLDER_NETPROFIT', 'BASIC_EPS', 'ROE_AVG', 'ROA'
  ]
  fundamental_df = fundamental_df[fundamental_cols]

  # 将基本面数据按报告日期排序，并去重（确保每个报告日期只有一条）
  fundamental_df = fundamental_df.drop_duplicates(subset='REPORT_DATE')

  # 合并：使用 merge_asof 将每个交易日期匹配到最近一期已发布的基本面数据
  merged_df = pd.merge_asof(
      price_df,
      fundamental_df,
      left_on='date',
      right_on='REPORT_DATE',
      direction='backward'  # 取最近一期已发布的财报
  )

  # 可选：将 REPORT_DATE 列重命名为 last_financial_report_date
  merged_df.rename(columns={'REPORT_DATE': 'last_financial_report_date'}, inplace=True)

  # 查看合并结果
  # print(merged_df.head(10))

  # 保存合并后的数据
  merged_df.to_csv(output_csv_path, index=False)

def merge_to_merge(to_merge = 'to-merge.csv', data_dir = 'fin-data'):
    # 读取待合并的股票代码列表
    to_merge_df = pd.read_csv(f'{data_dir}/{to_merge}')

    # 遍历每个股票代码
    for symbol in to_merge_df['Symbol']:
        # 构造量价数据文件路径
        price_csv_file = f'{symbol}.csv'
        price_csv_path = f'{data_dir}/pv/{price_csv_file}'

        # 检查文件是否存在
        if os.path.exists(price_csv_path):
            print(f"找到文件 {price_csv_file}，开始合并数据...")
            merge_price_and_fundamental_data(price_csv_file, data_dir)
        else:
            print(f"警告: 未找到文件 {price_csv_file} in '{data_dir}/pv/'，跳过处理。")


def clean_data(data_dir ='fin-data'):
  # 去掉data_merged_dir中前期last_financial_report_date为空的数据
  data_merged_dir = f'{data_dir}/merged'
  data_prepared_dir = f'{data_dir}/prepared'
  # 遍历合并后的数据目录中的所有CSV文件
  for filename in os.listdir(data_merged_dir):
    if filename.endswith('.csv'):
      file_path = os.path.join(data_merged_dir, filename)

      # 读取CSV文件
      df = pd.read_csv(file_path, parse_dates=['last_financial_report_date'])

      # 找到第一个last_financial_report_date非空的索引
      first_valid_index = df['last_financial_report_date'].first_valid_index()

      # 如果存在非空值，则删除前面的数据
      if first_valid_index is not None:
        df_cleaned = df.iloc[first_valid_index:].copy()

        # 保存清理后的数据到prepared目录
        prepared_file_path = os.path.join(data_prepared_dir, filename)
        df_cleaned.to_csv(prepared_file_path, index=False)
        print(f"已清理文件 {filename}，删除了前 {first_valid_index} 行空数据")
      else:
        print(f"警告: 文件 {filename} 中所有last_financial_report_date都为空")


if __name__ == '__main__':
  merge_to_merge()
  clean_data()
