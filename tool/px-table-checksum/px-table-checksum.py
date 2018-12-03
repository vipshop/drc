# -*- coding=utf-8 -*-
#!/usr/bin/python
"""
Author:     seanlook
Contact:    seanlook7@gmail http://seanlook.com
Date:       2016-11-02 released
"""

import MySQLdb
from MySQLdb.constants import FIELD_TYPE
from MySQLdb.converters import conversions
from zlib import crc32
from settings_cs_tables import DB_SOURCE, DB_TARGET, TABLES_CHECK, DB_ID_CS
import threading
import sys
import os
import time
import random
from hotqueue import HotQueue
from settings_checksum import REDIS_INFO, DB_CHECKSUM, CALC_CRC32_DB, REDIS_QUEUE_CNT, REDIS_POOL_CNT, CHUNK_SIZE
from settings_checksum import GEN_DATAFIX, RUN_DATAFIX, DO_COMPARE


# 数据库的字符集类型，映射到python的类型。
CHARSET_MAPPING = {"latin1": "latin-1",
                   "utf8": "utf-8",
                   "utf8mb4": "utf-8"}

# 要检查的比数量，用于通知消费队列什么时候结束
TABLES_CHECK_COUNT = 0
for t in TABLES_CHECK.values():
    TABLES_CHECK_COUNT += len(t)

"""
redis队列工具类
in_queues是源数据库写队列的客户端连接池。使用时随机获取一个
outcheck是目标库线程消费检查sql队列。注意不要与python原生Queue混淆
"""
class QueueHelper(object):
    def __init__(self, in_cnt, out_cnt):
        self.in_cnt = in_cnt
        self.out_cnt = out_cnt

	random.seed(os.getpid())
        time_uniq = random.randint(0, 10000)
        checksum_queue_key_prefix = "checksum-%d-" % time_uniq
        redis_host, redis_port = REDIS_INFO.split(":")

        # 生产者客户端连接池，根据同时比较的表数量而定
        self.in_queues = [HotQueue(checksum_queue_key_prefix + str(i),
                                   host=redis_host, port=redis_port, db=11) for i in range(in_cnt)]
        # 消费者取用队列，对应同等个数的后端消费者
        self.out_queues = [HotQueue(checksum_queue_key_prefix + str(i),
                                    host=redis_host, port=redis_port, db=11) for i in range(out_cnt)]

    # 随机挑选一个redis连接
    def get_in_client(self):
        in_clients = self.in_cnt - 1
        return self.in_queues[random.randint(0, in_clients)]

    def get_out_client(self):
        return self.out_queues

    def destroy_queue(self, queue):
        queue.clear()

queues = QueueHelper(REDIS_POOL_CNT, REDIS_QUEUE_CNT)

"""
建立数据库连接方法
注意数据库里日期0000-00-00使用MySQLdb取出后为None（不知道咋想的），所以连接时使用conv重写了字段映射类型（当做字符串）
"""
def get_dbconn(**dbconn_info):
    db_host = dbconn_info['db_host']
    db_user = dbconn_info['db_user']
    db_pass = dbconn_info['db_pass']
    db_port = dbconn_info['db_port']
    db_name = dbconn_info.get('db_name', None)
    db_charset = dbconn_info.get('db_charset', 'latin1')

    myconv = {FIELD_TYPE.TIMESTAMP: str, FIELD_TYPE.DATETIME: str}
    myconv = conversions.copy()
    del myconv[FIELD_TYPE.TIMESTAMP]
    del myconv[FIELD_TYPE.DATETIME]

    ### print "Connect to [%s:%d] using %s" % (db_host, db_port, db_charset)

    conn = None
    try:
        conn = MySQLdb.connect(host=db_host, user=db_user, passwd=db_pass, port=db_port, charset=db_charset,
                               connect_timeout=5, conv=myconv)  # use_unicode=False,
    except MySQLdb.Error, e:
        print "Error %d: %s" % (e.args[0], e.args[1])
        sys.exit(-1)

    if db_name is not None:
        conn.select_db(db_name)
    #conn.autocommit(False)

    return conn

"""
处理表的核心类，批次计算源库表的checksum结果，存入t_checksum表
"""
class CalcTbl(object):
    def __init__(self, db_conn, db_conn_checksum=None, dbconn_info={}):
        self.db_conn = db_conn
        self.db_conn_cs = db_conn_checksum
        if len(dbconn_info) != 0:
            self.result_charset = CHARSET_MAPPING[dbconn_info['db_charset']]

    def close_conn(self):
        try:
            self.db_conn.close()
            self.db_conn_cs.close()
        finally:
            print "db conection closed."

    # 获取表上的主键或唯一键：show index from t1
    def get_uniq_key(self, table_name):
        cur = self.db_conn.cursor()
        sql_keys = "show index from " + table_name + " where Key_name='PRIMARY'"

        #print sql_keys

        cur.execute(sql_keys)
        res = cur.fetchall()

        #print res

        cur.close()

        if len(res) == 0:
            global TABLES_CHECK_COUNT
            sql_keys = "show index from " + table_name + " where Non_unique=0"
            cur = self.db_conn.cursor()
            cur.execute(sql_keys)
            res = cur.fetchall()
            cur.close()

            if len(res) == 0:
                print "Warning: No PRIMARY or UNIQUE key found in ", table_name
                TABLES_CHECK_COUNT -= 1
                sys.exit(-1)

        t_uniq = [col[4] for col in res]

        ### print "Using [", t_uniq, "] as the unique column for ", table_name

        return t_uniq

    # 获取表上的所有字段名，拼接查询、修复sql都会用到
    def get_cols(self, table_name):
        global TABLES_CHECK_COUNT
        ts, tn = table_name.split(".")
        cur = self.db_conn.cursor()
        sql_cols = "select GROUP_CONCAT(CONCAT('`', COLUMN_NAME, '`')) from information_schema.COLUMNS where table_name =%s" + \
            " and table_schema=%s"
        param = (tn, ts)

        cur.execute(sql_cols, param)
        res = cur.fetchall()
        if res[0][0] is not None:
            return ",".join(res[0])
        else:
            print "Error: table %s does not exist" % table_name
            TABLES_CHECK_COUNT -= 1
            sys.exit(-1)


    # 用于在找出不同行，结构与make_chunk_sql，select_chunk类似
    def get_chunk_rows(self, table_name, start_key, end_key):
        t_cols = self.get_cols(table_name)
        t_uniq_keys = self.get_uniq_key(table_name)

        t_uniq_key_com = ",".join(t_uniq_keys)
        t_uniq_key_order = " asc,".join(t_uniq_keys) + " asc"

        t_uniq_filter_list_min = []
        for wf_cnt in range(0, len(t_uniq_keys)):
            t_uniq_filter_or = ("(" + t_uniq_keys[wf_cnt] + " > %s ")
            for wf_cnt2 in range(0, wf_cnt):
                t_uniq_filter_or += "and " + t_uniq_keys[wf_cnt2] + " = %s "
            t_uniq_filter_or += ")"

            t_uniq_filter_list_min.append(t_uniq_filter_or)

        t_uniq_filter_min = " OR ".join(t_uniq_filter_list_min)

        t_uniq_filter_list_max = []
        len_uniq_keys = len(t_uniq_keys)
        for wf_cnt in range(0, len_uniq_keys):
            if wf_cnt == len_uniq_keys - 1:  # 前闭后开区间 (min,max]
                t_uniq_filter_or = ("(" + t_uniq_keys[wf_cnt] + " <= %s ")
            else:
                t_uniq_filter_or = ("(" + t_uniq_keys[wf_cnt] + " < %s ")
            for wf_cnt2 in range(0, wf_cnt):
                t_uniq_filter_or += "and " + t_uniq_keys[wf_cnt2] + " = %s "
            t_uniq_filter_or += ")"

            t_uniq_filter_list_max.append(t_uniq_filter_or)

        t_uniq_filter_max = " OR ".join(t_uniq_filter_list_max)

        t_uniq_filter = "(%s) AND (%s)" % (t_uniq_filter_min, t_uniq_filter_max)

        sql_plain_rows = "select concat_ws('-'," + t_uniq_key_com + "), " + t_cols + " from " + table_name + \
                         " where " + t_uniq_filter + " order by " + t_uniq_key_order
        t_start_value = start_key.split('-')
        start_values = ()
        for wf_v in range(len(t_start_value)):
            start_values += (t_start_value[wf_v],) + tuple(t_start_value[0:wf_v])

        t_end_value = end_key.split('-')
        end_values = ()
        for wf_v in range(len(t_end_value)):
            end_values += (t_end_value[wf_v],) + tuple(t_end_value[0:wf_v])

        # print "diff sql: ", sql_plain_rows
        # print "diff param: ", start_values + end_values
        chunk_crc32_rows = []

        cur = self.db_conn.cursor()
        cur.execute(sql_plain_rows, start_values + end_values)

        res = cur.fetchall()
        for row in res:
            #print row[1:]
            row_str = self.conv_tuple_encode(0, row[1:])

            # row_str = "#".join([r.encode(self.result_charset) for r in row[1:]])
            row_crc32 = crc32(row_str)

            row_list = [row[0]]
            row_list.append(str(row_crc32))
            row_list.append(row_str)
            #print row_list
            chunk_crc32_rows.append(row_list)

        return chunk_crc32_rows

    # 在比较具体不同行用到，为了简化代码将数字变成字符串来拼接。NULL也要做特殊替换
    def conv_tuple_encode(self, tuple_str=0, *row):
        row_str = ""
        for rst in row[0]:
            # print "unicodetype:", type(rst)
            if isinstance(rst, unicode):
                rst = rst.encode(self.result_charset)
                # print rst
            else:
                rst = str(rst)
            row_str += rst + "#"

        if tuple_str == 1:
            row_list = []
            for rst in row[0]:
                if isinstance(rst, unicode):
                    rst = rst.encode(self.result_charset)
                elif rst is None:
                    rst = "[[NULL]]"  # handle NULL columns
                else:
                    rst = str(rst)
                row_list.append(rst)
            row_str = tuple(row_list)

        return row_str

    # 生剩源库检查chunk的sql，起始点用%s代替
    def make_chunk_sql(self, table_name, chunk_size=2000):
        t_cols = self.get_cols(table_name)
        t_uniq_keys = self.get_uniq_key(table_name)

        t_uniq_key_com = ",".join(t_uniq_keys)
        t_uniq_key_order = " asc,".join(t_uniq_keys) + " asc"
        # t_uniq_start_pair = dict(zip(t_uniq_keys, start_key))

        # 组合主键或索引，参考pt-table-checksum的语句
        t_uniq_filter_list = []
        for wf_cnt in range(0, len(t_uniq_keys)):
            t_uniq_filter_or = ("(" + t_uniq_keys[wf_cnt] + " > %s ")
            for wf_cnt2 in range(0, wf_cnt):
                t_uniq_filter_or += "and " + t_uniq_keys[wf_cnt2] + " = %s "
            t_uniq_filter_or += ")"

            t_uniq_filter_list.append(t_uniq_filter_or)

        t_uniq_filter = " OR ".join(t_uniq_filter_list)

        # 根据是否在db计算，生产不同的sql，所以在select_chunk反复也要区分处理
        if CALC_CRC32_DB:
            print "Caculate crc32 in db instead of program.(save net traffic, but make more db load)"
            sql_plain_rows = "select concat_ws('-'," + t_uniq_key_com + "), CRC32( concat_ws('#', " + t_cols + ") ) from " + table_name + \
                             " where " + t_uniq_filter + " order by " + t_uniq_key_order + " limit %d" % CHUNK_SIZE
        else:
            print "Caculate crc32 in program instead of db.(this program need more memory and more db net traffic, but convert charset)"
            sql_plain_rows = "select concat_ws('-'," + t_uniq_key_com + "), concat_ws('#', " + t_cols + ") from " + table_name + \
                             " where " + t_uniq_filter + " order by " + t_uniq_key_order + " limit %d" % CHUNK_SIZE
        # print sql_plain_rows

        return len(t_uniq_keys), sql_plain_rows

    # 计算一个chunk的crc32值
    # 输入拼装好的sql，传入界定chunk的参数  # out_rows用在后面找到具体行的不同 已用 get_chunk_rows方法单独处理，废
    def select_chunk(self, sql_chunkraw, start_key, out_rows=0):
        cur = self.db_conn.cursor()

        t_start_value = start_key.split('-')
        wf_values = ()
        for wf_v in range(len(t_start_value)):
            wf_values += (t_start_value[wf_v],) + tuple(t_start_value[0:wf_v])

        param = wf_values
        rows_count = 0
        # print sql_chunkraw
        # print param

        try:
            cur.execute(sql_chunkraw, tuple(param))
            rows_count = cur.rowcount
        except MySQLdb.Error, e:
            print "Error %d: %s !" % (e.args[0], e.args[1])

        if rows_count > 0:
            res = cur.fetchall()

            max_id = res[-1][0]  # 该chunk最后一行，即范围上限，用于下个chunk起点
            rows_crc32 = ""
            rows_id = ""

            for row in res:
                rows_id += str(row[0]) + ","  # row_id

                if CALC_CRC32_DB:
                    rows_crc32 += str(row[1]) + ","
                else:
                    #print row
                    if isinstance(row[1], unicode):
                        row_str = row[1].encode(self.result_charset)
                    else:
                        row_str = row[1]
                    # print rows_id, row_str
                    rows_crc32 += str(crc32(row_str)) + ","  # python计算每行的crc32

            # print max_id, rows_crc32
            if out_rows == 1:
                return rows_id.split(","), rows_crc32.split(",")
            else:
                return max_id, crc32(rows_crc32)  # chunk_crc32
        else:
            # 已完成所有chunk，或者有异常
            return -1, -1

    # 计算结果写入checksum结果表
    def write_output(self, *res_cs_cols):
        conn = self.db_conn_cs

        try:
            cur = conn.cursor()

            sqlstr = "insert into t_checksum(f_dbid,f_table_name,f_chunk_no,f_schema_name,f_min_id,f_max_id,f_chunk_crc32) " + \
                     "values(%s,%s,%s,%s,%s,%s,%s)"
            param = res_cs_cols  # (dbid, chunk_no, schema_name, table_name, max_id, chunk_crc32)

            cur.execute(sqlstr, param)
            conn.commit()

        except MySQLdb.Error, e:
            print "Error %d: %s" % (e.args[0], e.args[1])
            print "write checksum data to t_checksum FAIL."
            conn.rollback()
        finally:
            cur.close()


# 源库、目标库检查一致性开始的入口
class CheckSum(object):
    def __init__(self, st_name, dbconn_cs_info, dbconn_info=None):
        try:
            self.dbconn_checksum = get_dbconn(**dbconn_cs_info)
        except MySQLdb.Error, e:
            try:
                print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            except IndexError:
                print "MySQL Error: %s" % str(e)
            finally:
                sys.exit(-1)

        if dbconn_info is not None:
            self.dbconn_info = dbconn_info
            self.dbconn = get_dbconn(**dbconn_info)
            self.dbid = dbconn_info['db_host']+":"+str(dbconn_info['db_port'])
        if st_name != '':
            self.st_name = st_name

    def close_conn(self):
        try:
            self.dbconn_checksum.close()
            self.dbconn.close()
        finally:
            print "db conection closed."

    # 开始前清除老数据
    def before_checksum(self):
        cur = self.dbconn_checksum.cursor()

        print "Before checksum: create table if not exists t_checksum"
        sqlstr = """
            CREATE TABLE IF NOT EXISTS t_checksum (
              f_dbid varchar(80) NOT NULL,
              f_table_name varchar(50) NOT NULL,
              f_chunk_no int(11) NOT NULL,
              f_create_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
              f_schema_name varchar(30) DEFAULT NULL,
              f_min_id varchar(50) NOT NULL,
              f_max_id varchar(50) NOT NULL,
              f_chunk_crc32 varchar(20) DEFAULT NULL,
              PRIMARY KEY (f_dbid,f_table_name,f_chunk_no,f_create_time),
              KEY idx_tbname_maxid (f_table_name,f_max_id),
              KEY idx_chunkno (f_chunk_no)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8
            """
        cur.execute(sqlstr)

        print "Before checksum: delele old data from t_checksum if exists for table: ", self.st_name
        sqlstr = "DELETE FROM t_checksum WHERE f_table_name=%s AND f_schema_name=%s"
        schema_name, table_name = self.st_name.split(".")
        param = (table_name, schema_name)

        cur.execute(sqlstr, param)
        cur.close()

    # 在源source实例上执行检查
    def do_checksum(self):

        self.before_checksum()

        schema_name, table_name = self.st_name.split('.')
        dt_name = self.st_name

        mycheck = CalcTbl(self.dbconn, self.dbconn_checksum, self.dbconn_info)

        param_num, sql_chunkraw = mycheck.make_chunk_sql(dt_name)
        print "Caculating checksums: ", self.dbid, dt_name

        start_key = "-".join(['0'] * param_num)
        chunk_no = 1

        while True:
            # 批次计算crc32
            max_id, chunk_crc32 = mycheck.select_chunk(sql_chunkraw, start_key)  # , self.dbconn_info['result_charset'])

            if max_id != -1:
                res_cs_cols = (self.dbid, table_name, chunk_no, schema_name, start_key, max_id, chunk_crc32)
                # print "SOURCE: ", sql_chunkraw
                # print "SOURCE:", res_cs_cols
                mycheck.write_output(*res_cs_cols)

                # 写入t_checksum表后，将刚执行过才select chunk sql放入队列
                queues.get_in_client().put([dt_name, sql_chunkraw, start_key, chunk_no])

                start_key = max_id
                chunk_no += 1
            else:
                print "源实例", self.dbid, dt_name, " 计算checksum结束！"
                queues.get_in_client().put([-1, -1, -1, -1])  # chunk_no=-1通知队列结束
                break

        self.close_conn()

    # 检查目标库，从队列取sql
    def do_checksum_target(self, sql_queue):
        global TABLES_CHECK_COUNT
        dbid = DB_TARGET['db_host'] + ":" + str(DB_TARGET['db_port'])

        dbconn = get_dbconn(**DB_TARGET)
        dbconn_checksum = get_dbconn(**DB_CHECKSUM)

        tbl_check = CalcTbl(dbconn, dbconn_checksum, DB_TARGET)

        for item in sql_queue.consume():

            chunk_no = item[3]
            if chunk_no == -1:
                TABLES_CHECK_COUNT -= 1  # 全局变量，控制消费线程退出

                if TABLES_CHECK_COUNT <= 0:  # 通知其它队列结束
                    print "消费sql %d 退出！！" % TABLES_CHECK_COUNT
                    print
                    for sql_queue_other in queues.get_out_client():
                        sql_queue_other.put([-1, -1, -1, -1])
                    sql_queue.clear()
                    sys.exit(0)
            else:
                schema_name, table_name = item[0].split(".")
                start_key = item[2]
                try:
                    max_id, chunk_crc32 = tbl_check.select_chunk(item[1], start_key)

                    res_cs_cols = (dbid, table_name, chunk_no, schema_name, start_key, max_id, chunk_crc32)
                    # print "TARGET:", res_cs_cols
                    tbl_check.write_output(*res_cs_cols)

                except MySQLdb.Error, e:
                    print "Error %d: %s" % (e.args[0], e.args[1])


# 从t_checksum表里拿数据比较，修复。针对单个表
class Compare(object):
    def __init__(self, st_name, **dbconn_cs_info):
        self.dbconn_checksum = get_dbconn(**dbconn_cs_info)
        self.st_name = st_name

    def close_conn(self):
        try:
            self.dbconn_checksum.close()
        finally:
            print "db conection closed."

    def do_compare(self):
        schema_name, table_name = self.st_name.split(".")

        conn = self.dbconn_checksum
        cur = conn.cursor()

        sql_vs_raw = "SELECT t1.f_chunk_no,t1.f_min_id,t1.f_max_id,t1.f_chunk_crc32,t2.f_min_id,t2.f_max_id,t2.f_chunk_crc32 " + \
                     "FROM t_checksum t1, t_checksum t2 WHERE " + \
            "t1.f_table_name = t2.f_table_name AND " + \
            "t1.f_chunk_no = t2.f_chunk_no AND " + \
            "t1.f_schema_name = t2.f_schema_name AND " + \
            "t1.f_dbid != t2.f_dbid AND " + \
            "t1.f_chunk_crc32 > t2.f_chunk_crc32 AND " + \
            "t1.f_schema_name = %s and t1.f_table_name = %s "
        # print sql_vs_raw

        param = (schema_name, table_name)
        cur.execute(sql_vs_raw, param)
        res = cur.fetchall()

        cur.close()
        # conn.close()

        if len(res) > 0:
            print "表 %s.%s 数据不一致chunk数：%d " % (schema_name, table_name, len(res))
            print "-" * 80
            chunk_rows_fix = {"DELETE": [], "INSERT_UPDATE": []}
            for row in res:
                # 两边f_min_id肯定是一样的
                # chunk_no, min_id, max_id_src, max_id_tgt \
                diff_chunk = (row[0], row[1], row[2], row[5])
                print
                print u"该chunk [%s] 存在行内容不一致, CRC32: src(%s) rgt(%s)" % (row[0], row[3], row[6])

                data_fix = self.get_diffs(self.st_name, *diff_chunk)
                chunk_rows_fix['DELETE'].extend(data_fix[0])
                chunk_rows_fix['INSERT_UPDATE'].extend(data_fix[1])

            if GEN_DATAFIX:
                print
                print "-" * 80
                print "正在生成在目标库的修复SQL..."
                fixfile = self.generate_fixsql(self.st_name, **chunk_rows_fix)

                if RUN_DATAFIX:
                    print "-" * 80
                    run_confirm = str(raw_input("Run data fix <%s> in target DB <%s %s> confirm [N/y]:"
                                                % (fixfile, DB_TARGET['db_host'], DB_TARGET['db_port'])))
                    if run_confirm.lower() in ('y', 'yes'):
                        print "正在目标库(%s:%s)进行数据修复表(%s)" % (DB_TARGET['db_host'], DB_TARGET['db_port'], self.st_name)
                        self.run_fixsql(fixfile)
                    else:
                        print "没有执行目标库数据修复"

        else:
            cur = conn.cursor()
            sql_table_exists = "select 1 from t_checksum t1 where t1.f_schema_name = %s and t1.f_table_name = %s"
            cur.execute(sql_table_exists, param)
            if cur.rowcount == 0:
                print "没有找到表 %s.%s 的checksum数据" % (schema_name, table_name)
            else:
                print "表 %s.%s 数据一致" % (schema_name, table_name)
            cur.close()

        conn.close()

    # 根据chunk号，去源库和目标库获取不一致的具体行
    # 每个chunk起点一定是相同的，但最大的记录不一定，所以这里要取得源库和目标库改chunk最后一行的较大的那条，来作为比较的范围
    def get_diffs(self, schema_table_name, *diff_chunk):
        #my_conv = {MySQLdb.constants.FIELD_TYPE.LONG: str}
        print "去源库和目标库获取chunk[%d]不一致行：" % diff_chunk[0]
        dbconn_source = get_dbconn(**DB_SOURCE)
        dbconn_target = get_dbconn(**DB_TARGET)

        mycalc_src = CalcTbl(dbconn_source, dbconn_info=DB_SOURCE)
        mycalc_tgt = CalcTbl(dbconn_target, dbconn_info=DB_TARGET)

        chunk_no, min_id, max_id_src, max_id_tgt = diff_chunk
        # max_id_real = max(max_id_src, max_id_tgt)
        max_id_real = self.get_realmax_id(max_id_src, max_id_tgt)

        # print "max_id_real:", max_id_real

        chunk_crc32_rows_src = mycalc_src.get_chunk_rows(schema_table_name, min_id, max_id_real)
        chunk_crc32_rows_tgt = mycalc_tgt.get_chunk_rows(schema_table_name, min_id, max_id_real)

        chunk_crc32_src = set([row[0]+":"+row[1] for row in chunk_crc32_rows_src])
        chunk_crc32_tgt = set([row[0]+":"+row[1] for row in chunk_crc32_rows_tgt])

        rows_inserted = [rowid.split(":")[0] for rowid in (chunk_crc32_src - chunk_crc32_tgt)]  # inserted or updated
        rows_deleted2 = [rowid.split(":")[0] for rowid in (chunk_crc32_tgt - chunk_crc32_src)]  # deleted or updated

        # print "XXXXXX", chunk_crc32_tgt

        rows_updated = set(rows_inserted) & set(rows_inserted)
        rows_deleted = set(rows_deleted2) - rows_updated

        # fix_dict = {"DELETE": rows_deleted, "INSERT_UPDATE": list(rows_inserted)}
        # 使用replace into语法处理 insert,update两种dml
        data_fix = list(rows_deleted), rows_inserted

        if len(rows_inserted) + len(rows_deleted) == 0:
            print "表%s 上chunk %d 数据已一致" % (schema_table_name, chunk_no)
        else:
            print "  TO insert or update: ", data_fix[1]
            print "  TO delete: ", data_fix[0]

        # print "data_fix: ", data_fix
        return data_fix

    # 获取较大的主键（数字主键和字符主键比较方法不一样）
    def get_realmax_id(self, max_id1, max_id2):

        if max_id1 == max_id2:
            return max_id1

        maxid1_list = max_id1.split('-')
        maxid2_list = max_id2.split('-')
        pos_max = 0

        for id1, id2 in zip(maxid1_list, maxid2_list):
            try:
                id1_long = long(id1)
                id2_long = long(id2)
                if id1_long > id2_long:
                    pos_max = max_id1
                elif id1_long < id2_long:
                    pos_max = max_id2
                else:
                    pos_max = 0
            except ValueError:
                if id1 > id2:
                    pos_max = max_id1
                elif id1 < id2:
                    pos_max = max_id2
                else:
                    pos_max = 0
            if pos_max != 0:
                break

        return pos_max

    # 根据找出的不同行的主键，生成从源库到目标库的修复sql
    def generate_fixsql(self, schema_table_name, **tbl_key):
        time_unique = int(time.time())
        datafix_file = "datafix-%s-%d.sql" % (schema_table_name.split(".")[1], time_unique)
        data_fix_sql = open(datafix_file, "a")

        tbl_ops = CalcTbl(get_dbconn(**DB_SOURCE), dbconn_info=DB_SOURCE)
        wf_keys = tbl_ops.get_uniq_key(schema_table_name)
        # t_cols = tbl_ops.get_cols(schema_table_name)

        wf_keys_str = "='%s' and ".join(wf_keys) + "='%s'"

        for dml, params in tbl_key.items():

            if dml == 'DELETE':
                for k in params:
                    wf_values = k.split('-')
                    sql_str_raw = "delete from " + schema_table_name + " where " + wf_keys_str + ";"
                    sql_str = sql_str_raw % tuple(wf_values)
                    #print
                    #print "DELETE ", sql_str, tuple(wf_values)
                    data_fix_sql.write(sql_str + "\n")
            if dml == "INSERT_UPDATE":
                dbconn = get_dbconn(**DB_SOURCE)

                for k in params:
                    wf_values = k.split('-')
                    sql_str_src = "select * from " + schema_table_name + " where " + wf_keys_str % tuple(wf_values)

                    cur = dbconn.cursor()
                    cur.execute(sql_str_src)
                    if cur.rowcount == 1:
                        res = cur.fetchone()
                        res2 = tbl_ops.conv_tuple_encode(1, res)
                        # print "fix field: ", res
                        # print "fix field2: ", res2
                    else:
                        print
                        print "Error: exactly one line expected."
                        print sql_str_src
                    cur.close()

                    # sql_str_raw = "insert into " + schema_table_name + "(" + t_cols + ") values('%s') on duplicate key update;"
                    sql_str_raw = "REPLACE into " + schema_table_name + " values('%s');"
                    sql_str = sql_str_raw % "', '".join(res2)
                    sql_str = sql_str.replace("\'[[NULL]]\'", "NULL")
                    #print sql_str
                    data_fix_sql.write(sql_str + "\n")

                dbconn.close()

        data_fix_sql.close()
        print "修复数据SQL文件：%s" % datafix_file

        return datafix_file

    # 在目标库运行修复sql
    def run_fixsql(self, fixfile):
        dbconn = get_dbconn(**DB_TARGET)
        cur = dbconn.cursor()

        f = open(fixfile, 'r')
        line_sql = f.readline()
        while line_sql:
            cur.execute(line_sql)
            line_sql = f.readline()
        dbconn.commit()

        f.close()

# 目标库消费 检查sql 的多线程
class outThread(threading.Thread):
    def __init__(self, sql_queue):
        threading.Thread.__init__(self)
        self.sql_queue = sql_queue

    def run(self):
        # 消费线程不关心队列里是哪个表的sql
        outcheck = CheckSum(st_name='', dbconn_cs_info=DB_CHECKSUM, dbconn_info=DB_TARGET)
        outcheck.do_checksum_target(self.sql_queue)

## 源库根据表数据量同时检查的多线程
class myThread(threading.Thread):
    def __init__(self, threadID, schema_name, table_name, **dbconn_info):
        threading.Thread.__init__(self)
        # self.threadID = threadID
        self.name = threadID
        self.schema_name = schema_name
        self.table_name = table_name
        self.dbconn_info = dbconn_info

    def run(self):
        dbid = self.dbconn_info['db_host'] + ":" + str(self.dbconn_info['db_port'])
        st_name = self.schema_name + "." + self.table_name
        print "Starting checksum thread for table: %s (%s)" % (st_name, dbid)

        checksum = CheckSum(st_name, DB_CHECKSUM, self.dbconn_info)
        checksum.do_checksum()

        print "Checksum thread ended for table: %s (%s) " % (st_name, dbid)


if __name__ == '__main__':
    
    reload(sys)
    sys.setdefaultencoding('utf-8')
    # 多线程并行计算checksum
    if DO_COMPARE in (1, 0):
        # 执行过的sql存放两个redis队列，各自对应一个线程来消费
        for out_queue in queues.get_out_client():
            thread_outcheck = outThread(out_queue)
            thread_outcheck.start()

        TID_no = 0
        for s_name, t_names in TABLES_CHECK.items():
            for t_name in t_names:
                thread_checksum = myThread(DB_ID_CS+"-"+str(TID_no), s_name, t_name, **DB_SOURCE)
                thread_checksum.start()
                TID_no += 1
                time.sleep(0.6)

    # 串行比较
    if DO_COMPARE in (1, 2):
        if DO_COMPARE == 1:
            while TABLES_CHECK_COUNT >= 0:
                time.sleep(2)

        for s_names, t_namess in TABLES_CHECK.items():
            for t_n in t_namess:
                print "#" * 80
                print "Start compare chunk's crc32 for table: [ %s.%s ]" % (s_names, t_n)
                tb_cmp = Compare(s_names + "." + t_n, **DB_CHECKSUM)
                tb_cmp.do_compare()

                print ""
                print "#" * 80
