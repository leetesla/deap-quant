# KEY_ALPHA_EXPR_ALL_LIST
from myredis import create_sync_redis_client
from myredis.redis_key import KEY_ALPHA_EXPR_ALL_LIST
import csv
import os
data_dir = 'data/redis_export'

if __name__ == '__main__':
    c = create_sync_redis_client()
    # 获取list长度
    list_length = c.llen(KEY_ALPHA_EXPR_ALL_LIST)
    print(list_length)

    # 导出为csv文件，header使用KEY_ALPHA_EXPR_ALL_LIST并替换:为-
    header = KEY_ALPHA_EXPR_ALL_LIST.replace(':', '-')

    # 获取list中的所有元素
    list_data = c.lrange(KEY_ALPHA_EXPR_ALL_LIST, 0, -1)

    # 确保data_dir目录存在，如果不存在则创建
    os.makedirs(data_dir, exist_ok=True)

    # 写入csv文件
    with open(f'{data_dir}/{header}.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([header])  # 写入header
        for item in list_data:
            writer.writerow([item.decode('utf-8') if isinstance(item, bytes) else item])  # 写入数据行

    c.close()