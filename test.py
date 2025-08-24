from myredis import create_sync_redis_client, KEY_ALPHA_EXPR_ALL

if __name__ == '__main__':
  c = create_sync_redis_client()
  print(c.llen(KEY_ALPHA_EXPR_ALL))
  c.close()
