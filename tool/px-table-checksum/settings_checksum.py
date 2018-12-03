#coding:utf-8
from settings_cs_tables import DB_SOURCE, DB_TARGET

# sql队列 redis地址
REDIS_INFO = "10.10.10.1:6379"

# checksum结果数据存放位置
DB_CHECKSUM = {'db_host': '10.10.10.1',
           'db_port': 3306,
           'db_user': 'root',
           'db_pass': '123456',
           'db_charset': 'utf8',
           'db_name': 'test'}

# 是否直接在数据库计算CRC32
CALC_CRC32_DB = True if DB_SOURCE['db_charset'] == DB_TARGET['db_charset'] else False

# REDIS队列生产端连接池连接数量，REDIS sql队列数量
# 根据每次比较的表数量调整。2,2 适合3-5个表同时比较
REDIS_POOL_CNT, REDIS_QUEUE_CNT = (2, 2)

# 每次从源库取得计算数据行的大小
CHUNK_SIZE = 2000

# 只从checksum表比较（不从目标库和源库拉取计算）
# 0:不比较，只计算； 1:计算并比较，2:只比较，不计算
DO_COMPARE = 1

# 生成修复SQL
GEN_DATAFIX = False

# 程序自动在目标库运行修复SQL (warning!)
RUN_DATAFIX = False
