from datafeed.expr_functions import unary_funcs
from datafeed import ts_rolling_funcs
import pandas as pd

from funcs.funcs_except import CSV_PATH_FUNCS_ALL, CSV_PATH_FUNCS_EXCEPT, in_funcs_except

# 读取CSV文件
df_except = pd.read_csv(CSV_PATH_FUNCS_EXCEPT, header=None, names=['function'])

for func in unary_funcs:
  print(func)

print('**************************************')
# add_unary_rolling_ops(pset) 用下面的代码代替

# 创建一个列表来存储所有函数
all_funcs = []

for func in unary_funcs:
  # 检查func是否在df_except中
  if in_funcs_except(func):
    all_funcs.append('#' + func)
  else:
    all_funcs.append(func)

for func in ts_rolling_funcs:
  # print(type(func))
  # 检查func是否在df_except中
  if func in df_except['function'].values:
    all_funcs.append('#' + func)
  else:
    all_funcs.append(func)

# 保存到funcs-all.csv
with open(CSV_PATH_FUNCS_ALL, 'w') as f:
  for func in all_funcs:
    f.write(func + '\n')
