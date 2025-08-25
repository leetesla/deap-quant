import pandas as pd

# 读取CSV文件
df = pd.read_csv('funcs-all.csv', header=None, names=['function'])

# 筛选以#开头的行，并移除开头的#符号
df_filtered = df[df['function'].str.startswith('#')].copy()
df_filtered['function'] = df_filtered['function'].str[1:]

# 输出结果
print("处理后的结果:")
print(df_filtered['function'].to_string(index=False))

# 保存处理后的结果到新文件
df_filtered.to_csv('funcs-except.csv', index=False, header=False)
print("\n结果已保存到 funcs-except.csv")
