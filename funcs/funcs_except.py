import pandas as pd
def get_funcs_except():
  # 读取CSV文件
  df_except = pd.read_csv('funcs-except.csv', header=None, names=['function'])
  return df_except


def in_funcs_except(func):
  # 获取例外函数列表
  df_except = get_funcs_except()
  # 检查func是否在例外函数列表中
  return func in df_except['function'].values


if __name__ == '__main__':
  i = in_funcs_except('inv')
  print(i)

  i = in_funcs_except('ts_sum')
  print(i)
