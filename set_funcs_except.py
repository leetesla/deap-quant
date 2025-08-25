import pandas as pd

from funcs.funcs_except import CSV_PATH_FUNCS_ALL, CSV_PATH_FUNCS_EXCEPT

# 读取CSV文件
df = pd.read_csv(CSV_PATH_FUNCS_ALL, header=None, names=['function'])

# 筛选以#开头的行，并移除开头的#符号
df_filtered = df[df['function'].str.startswith('#')].copy()
df_filtered['function'] = df_filtered['function'].str[1:]

# 输出结果
print("处理后的结果:")
print(df_filtered['function'].to_string(index=False))

# 保存处理后的结果到新文件
df_filtered.to_csv(CSV_PATH_FUNCS_EXCEPT, index=False, header=False)
print(f"\n结果已保存到 {CSV_PATH_FUNCS_EXCEPT}")
