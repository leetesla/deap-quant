from redis import asyncio as aioredis
from redis import Redis

from myconfig import REDIS_HOST, REDIS_PASSWORD
from myredis.redis_key import KEY_ALPHA_EXPR_ALL

redis_client=None

async def create_redis_connection(host=REDIS_HOST, port=6379, db=0):
  """
  创建并返回一个 Redis 连接实例。

  :param host: Redis 服务器地址，默认为 'localhost'
  :param port: Redis 服务端口，默认为 6379
  :param db: 数据库编号，默认为 0
  :return: 返回一个 Redis 连接对象
  """
  try:
    # client = await aioredis.from_url(f"redis://{REDIS_PASSWORD}@{REDIS_HOST}:{port}/0 redis://{host}:{port}", password=REDIS_PASSWORD, decode_responses=True)
    client = await aioredis.Redis(host=REDIS_HOST, password=REDIS_PASSWORD, db=0, decode_responses=True)
    # 测试连接
    await client.ping()  # 如果连接失败会抛出异常
    return client
  except aioredis.ConnectionError as e:
    print(f"连接 Redis 失败: {e}")
    return None

async def create_redis_client(force = False):
  global redis_client  # 声明使用全局变量
  if redis_client is None or force:  # 如果未初始化，则创建连接
    # print("--------------------- create_redis_client: redis_client is None ---------------------")
    redis_client = await create_redis_connection()
  # else:
  #   print("++++++++++++++++++++++++ redis_client not None ++++++++++++++++++++++++ ")
  return redis_client



def create_sync_redis_client(db=0):
    """
    创建并返回一个同步的 Redis 连接实例。

    :param host: Redis 服务器地址，默认为配置中的地址
    :param port: Redis 服务端口，默认为 6379
    :param db: 数据库编号，默认为 0
    :return: 返回一个同步的 Redis 连接对象
    """
    try:
        client = Redis(host=REDIS_HOST, port=6379, password=REDIS_PASSWORD, db=db, decode_responses=True)
        # 测试连接
        client.ping()  # 如果连接失败会抛出异常
        return client
    except Exception as e:
        print(f"连接 Redis 失败: {e}")
        return None
async def add_elements_to_set(client, set_key, elements):
  """
  将一组元素添加到 Redis 中指定的集合里。

  :param client: Redis 连接对象
  :param set_key: 在 Redis 中存储集合的键名
  :param elements: 要添加到集合中的元素列表或单个元素
  :return: 成功添加的元素数量
  """
  if not client:
    print("Redis 连接无效")
    return 0

  # 确保 elements 是列表
  if not isinstance(elements, list):
    elements = [elements]  # 如果不是列表，则将其转换为列表

  # 调试信息
  # for element in elements:
  #   print(f"元素类型: {type(element)}, 元素值: {element}")

  return await client.sadd(set_key, *elements)

async def check_element_in_set(client, set_key, element):
  """
  检查指定的元素是否存在于 Redis 的集合中。

  :param client: Redis 连接对象
  :param set_key: 在 Redis 中存储集合的键名
  :param element: 要检查的元素
  :return: 如果元素存在则返回 True，否则返回 False
  """
  if not client:
    print("Redis 连接无效")
    return False
  return await client.sismember(set_key, element)

async def check_duplicate_alpha_expr(client, set_key, element):
  """
  检查指定的元素是否存在于 Redis 的集合中。

  :param client: Redis 连接对象
  :param set_key: 在 Redis 中存储集合的键名
  :param element: 要检查的元素
  :return: 如果元素存在则返回 True，否则返回 False
  """
  if not client:
    print("Redis 连接无效")
    return False
  return await client.sismember(set_key, element)


if __name__ == '__main__':
  c = create_sync_redis_client()
  print(c.llen(KEY_ALPHA_EXPR_ALL))
  c.close()
