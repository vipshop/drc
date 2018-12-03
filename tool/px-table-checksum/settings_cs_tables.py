
db_host_src = '10.10.10.1'
db_host_dst = '10.10.10.2'

TABLES_CHECK = {"test": ["table1"]}

DB_SOURCE = {'db_host': db_host_src,
           'db_port': 3306,
           'db_user': 'root',
           'db_pass': '123456',
           'db_charset': 'utf8mb4',
           'result_charset': 'utf-8'}

DB_TARGET = {'db_host': db_host_dst,
           'db_port': 3306,
           'db_user': 'root',
           'db_pass': '123456',
           'db_charset': 'utf8mb4',
           'result_charset': 'utf-8'}


DB_ID_CS = '1'
