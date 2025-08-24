import json
import os

from dotenv import load_dotenv

def my_load_env():
  # 加载环境配置，比如ENV=prod or dev
  # 可通过命令行参数override,比如 ENV=prod uv run main.py

  # 加载共享配置
  load_dotenv('env/.env')

  # 根据环境加载特定配置
  env_file = 'env/.prod.env' if os.getenv('ENV') == 'prod' else 'env/.dev.env'
  load_dotenv(env_file, override=True)

my_load_env()
print(os.getenv('REDIS_HOST'))
# API_BASE_URL = 'https://api.worldquantbrain.com'
API_BASE_URL = os.getenv('API_BASE_URL')


def get_ts_days():
  ts_days = []
  if os.getenv("TS_DAYS"):
    ts_days = json.loads(os.getenv("TS_DAYS"))

  return ts_days


REDIS_KEY_ALPHA_PREFIX = os.getenv('REDIS_KEY_ALPHA_PREFIX')


REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
REMOVE_DUPLICATE = True if os.getenv("REMOVE_DUPLICATE", "NO") == "YES" else False

if __name__ == '__main__':
  # print(os.getenv("ENV"))
  # print(os.getenv("T1"))
  # print(os.getenv("PASSWORD"))
  print(os.getenv("T1") == "")

