import pandas as pd

price_csv_path = 'fin-data/AAPL-1.csv'
fundamental_csv_path = 'fin-data/AAPL-1基本面数据.csv'
output_file = ''
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
print(merged_df.head(10))

# 保存合并后的数据
merged_df.to_csv('fin-data/AAPL_merged.csv', index=False)
