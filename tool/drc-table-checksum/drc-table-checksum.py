#!/usr/bin/python
# -*- coding=utf-8 -*-

# Copyright 2018 seanlook. All rights reserved.
#
# Copyright 2018 vip.com.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# See the License for the specific language governing permissions and
# limitations under the License.


import MySQLdb
from MySQLdb.constants import FIELD_TYPE
from MySQLdb.converters import conversions
from settings_cs_tables import SRC_CONN_INFO, DEST_CONN_INFO
import time
import sys
import argparse


# 建立数据库连接
def get_db_conn(**db_conn_info):
    db_host = db_conn_info['db_host']
    db_user = db_conn_info['db_user']
    db_pass = db_conn_info['db_pass']
    db_port = db_conn_info['db_port']
    db_name = db_conn_info.get('db_name', None)
    db_charset = db_conn_info.get('db_charset', 'utf8')

    # 注意数据库里日期0000 - 00 - 00
    # 使用MySQLdb取出后为None，所以连接时使用conv重写了字段映射类型（当做字符串）
    myconv = {FIELD_TYPE.TIMESTAMP: str, FIELD_TYPE.DATETIME: str}
    myconv = conversions.copy()
    del myconv[FIELD_TYPE.TIMESTAMP]
    del myconv[FIELD_TYPE.DATETIME]

    print "Connect to [%s:%d] using %s" % (db_host, db_port, db_charset)

    try:
        conn = MySQLdb.connect(host=db_host, user=db_user, passwd=db_pass, port=db_port, charset=db_charset,
                               connect_timeout=5, conv=myconv)
    except MySQLdb.Error as e:
        print "Error %d: %s" % (e.args[0], e.args[1])
        sys.exit(-1)

    if db_name is not None:
        conn.select_db(db_name)
    # 设置binlog format 为statement格式
    set_binlog_format(conn)
    return conn


# 设置binlog format 为statement
def set_binlog_format(conn):
    cur = conn.cursor()
    sql = "SET SESSION binlog_format = 'STATEMENT'"
    try:
        cur.execute(sql)
        conn.commit()
        cur.close()
    except MySQLdb.Error as e:
        print "set_binlog_format: MySQL Error: %s, rollback" % str(e)
        conn.rollback()
        cur.close()
        sys.exit(-1)

# 计算源库表的checksum结果，存入checksum表
class CheckTable(object):
    def __init__(self, db_checksum_table, db_target_table, chunk_size, db_conn):
        self.db_conn = db_conn
        self.db_checksum_table = db_checksum_table
        self.db_target_table = db_target_table
        self.chunk_size = chunk_size

    def close_conn(self):
        try:
            self.db_conn.close()
        finally:
            print "db conection closed."

    # 获取表上的主键
    def get_primary_key(self):
        target_schema, target_table = self.db_target_table.split(".")
        cur = self.db_conn.cursor()
        sql = "show index from `{0}`.`{1}` where Key_name='PRIMARY'".format(target_schema, target_table)
        print sql
        cur.execute(sql)
        res = cur.fetchall()
        cur.close()

        if len(res) == 0:
            print "Warning: No PRIMARY or UNIQUE key found in ", self.db_target_table
            sys.exit(-1)

        primary_key = res[0][4]
        print "Using [", primary_key, "] as the unique column for ", self.db_target_table
        return primary_key

    # 获取表上的所有列名
    def get_cols(self):
        target_schema, target_table = self.db_target_table.split(".")
        cur = self.db_conn.cursor()
        sql_cols = "select GROUP_CONCAT(COLUMN_NAME) from information_schema.COLUMNS where table_name ='{0}'" + \
            " and table_schema='{1}'"
        sql_cols = sql_cols.format(target_table, target_schema)

        cur.execute(sql_cols)
        res = cur.fetchall()
        cur.close()
        if res[0][0] is not None:
            return ",".join(res[0])
        else:
            print "Error: table %s does not exist" % self.db_target_table
            sys.exit(-1)

    def get_is_nullable_cols(self):
        target_schema, target_table = self.db_target_table.split(".")
        cur = self.db_conn.cursor()
        sql = "select COLUMN_NAME from information_schema.COLUMNS where table_name = '{0}'" + \
                    " and table_schema = '{1}' and IS_NULLABLE = 'yes'"
        sql = sql.format(target_table, target_schema)
        cur.execute(sql)
        res = cur.fetchall()
        cur.close()
        if len(res):
            result = []
            for r in res:
                result.append("ISNULL(%s)"%r)
            return ",".join(result)
        else:
            return None

    def get_init_lower_boundary(self, primary_key):
        target_schema, target_table = self.db_target_table.split(".")
        cur = self.db_conn.cursor()
        sql = "SELECT `{0}` FROM `{1}`.`{2}` FORCE INDEX (`PRIMARY`) " \
              "WHERE `{3}` IS NOT NULL ORDER BY `{4}` LIMIT 1"
        sql = sql.format(primary_key, target_schema, target_table, primary_key, primary_key)
        cur.execute(sql)
        res = cur.fetchone()
        cur.close()
        if res is not None:
            return res[0]
        else:
            print "Error: table %s does not exist rows" % self.db_target_table
            sys.exit(-1)

    # 获取本次的upper和下一次的lower
    def get_upper_lower_boundary(self, primary_key, init_lower_boundary):
        target_schema, target_table = self.db_target_table.split(".")
        cur = self.db_conn.cursor()

        sql = "SELECT `{0}` FROM `{1}`.`{2}` FORCE INDEX (`PRIMARY`) " \
              "WHERE ((`{0}` >= '{3}')) ORDER BY `{0}` LIMIT {4}, 2"
        sql = sql.format(primary_key, target_schema, target_table, init_lower_boundary, self.chunk_size - 1)
        cur.execute(sql)
        res = cur.fetchall()
        cur.close()

        if len(res) == 0:
            # 没有数据了
            return None, None
        elif len(res) == 1:
            # 只有一条数据
            return res[0][0], None
        elif len(res) == 2:
            # 包含本次upper和下一次lower boundary
            return res[0][0], res[1][0]

    def get_max_boundary(self, primary_key):
        target_schema, target_table = self.db_target_table.split(".")
        cur = self.db_conn.cursor()

        sql = "SELECT `{0}` FROM `{1}`.`{2}` FORCE INDEX (`PRIMARY`) " \
              "WHERE `{0}` IS NOT NULL ORDER BY `{0}` desc LIMIT 1"
        sql = sql.format(primary_key, target_schema, target_table)
        cur.execute(sql)
        res = cur.fetchone()
        cur.close()

        # todo 调试为空时的case
        if res is not None:
            return res[0]

    def generate_checksum_sql(self, chunk, current_lower, current_upper, pk):
        target_schema, target_table = self.db_target_table.split(".")
        checksum_schema, checksum_table = self.db_checksum_table.split(".")

        all_cols = self.get_cols()
        is_nullable_cols = self.get_is_nullable_cols()

        if current_lower is None and current_upper is not None:
            condition = " WHERE ((`{0}` > '{1}'))".format(pk, current_upper)
            current_lower = current_upper
            current_upper = 'NULL'
        elif current_lower is not None and current_upper is None:
            condition = " WHERE ((`{0}` < '{1}'))".format(pk, current_lower)
            current_upper = current_lower
            current_lower = 'NULL'
        elif current_lower is not None and current_upper is not None:
            condition = " WHERE ((`{0}` >= '{1}')) AND ((`{0}` <= '{2}'))".format(pk, current_lower, current_upper)

        if is_nullable_cols is None:
            sql ="REPLACE INTO `{0}`.`{1}` (db, tbl, chunk, chunk_index, lower_boundary, upper_boundary, this_cnt, this_crc) " + \
                 "SELECT '{2}', '{3}', '{4}', 'PRIMARY', '{5}', '{6}', COUNT(*) AS cnt, " + \
                 "COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS('#', {7})) AS UNSIGNED)), 10, 16)), 0) AS crc " + \
                 "FROM `{8}`.`{9}` FORCE INDEX(`PRIMARY`)"
            sql = sql.format(checksum_schema, checksum_table, target_schema, target_table, chunk,
                              current_lower, current_upper, all_cols, target_schema, target_table, pk)
        else:
            sql = "REPLACE INTO `{0}`.`{1}` (db, tbl, chunk, chunk_index, lower_boundary, upper_boundary, this_cnt, this_crc) " + \
                  "SELECT '{2}', '{3}', '{4}', 'PRIMARY', '{5}', '{6}', COUNT(*) AS cnt, " + \
                  "COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS('#', {7}, CONCAT({8}))) AS UNSIGNED)), 10, 16)), 0) AS crc " + \
                  "FROM `{9}`.`{10}` FORCE INDEX(`PRIMARY`)"
            sql = sql.format(checksum_schema, checksum_table, target_schema, target_table,
                              chunk, current_lower, current_upper, all_cols, is_nullable_cols,
                              target_schema, target_table, pk)

        sql = sql + condition
        return sql
# 操作checksum表


class CheckSum(object):
    def __init__(self, db_checksum_table, db_check_table,
                 chunk_size, lower_boundary, upper_boundary, src_conn_info):
        self.checksum_table_name = db_checksum_table
        self.db_target_table = db_check_table
        self.chunk_size = chunk_size
        self.lower_boundary = lower_boundary
        self.upper_boundary = upper_boundary

        try:
            self.db_conn = get_db_conn(**src_conn_info)
        except MySQLdb.Error as e:
            try:
                print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            except IndexError:
                print "MySQL Error: %s" % str(e)
            finally:
                sys.exit(-1)

    def close_conn(self):
        try:
            self.db_conn.close()
        finally:
            print "db conection closed."

    # 更新checksum表结果
    def update_crc32_time(self, chunk_time, chunk):
        checksum_schema, checksum_table = self.checksum_table_name.split(".")
        target_schema, target_table = self.db_target_table.split(".")
        cur = self.db_conn.cursor()

        # 查询this_crc 和 this_cnt
        crc_sql = "SELECT this_crc, this_cnt FROM `{0}`.`{1}` " + \
                       "WHERE db = '{2}' AND tbl = '{3}' AND chunk = '{4}'"
        crc_sql = crc_sql.format(checksum_schema, checksum_table, target_schema, target_table, chunk)
        cur.execute(crc_sql)
        res = cur.fetchone()
        if res is not None:
            this_crc = res[0]
            this_cnt = res[1]
        else:
            print "Error: table %s does not exist rows" % self.checksum_table_name
            sys.exit(-1)

        # 更新master_crc 和master_cnt
        sql = "UPDATE `{0}`.`{1}` SET chunk_time = '{2:.3f}', master_crc = '{3}', master_cnt = '{4}' " + \
              "WHERE db = '{5}' AND tbl = '{6}' AND chunk = '{7}'"
        sql = sql.format(checksum_schema, checksum_table, chunk_time,
                         this_crc, this_cnt, target_schema, target_table, chunk)
        try:
            cur.execute(sql)
            self.db_conn.commit()
            cur.close()
        except MySQLdb.Error as e:
            print "update_crc32_time: MySQL Error: %s, rollback" % str(e)
            self.db_conn.rollback()
            cur.close()
            sys.exit(-1)

    # 在Master实例上执行检查
    def do_check(self):
        check_table = CheckTable(self.checksum_table_name, self.db_target_table, self.chunk_size, self.db_conn)

        chunk_num = 0
        stop_check = False
        init_lower = self.lower_boundary
        pk = check_table.get_primary_key()
        while stop_check is False:
            # 未指定开始位置，则从表中最小位置开始
            if init_lower == -1:
                current_lower = check_table.get_init_lower_boundary(pk)
                min_lower = current_lower
            else:
                current_lower = init_lower
            # 计算本次chunk的上限，和下一个chunk的上限
            current_upper, next_lower = check_table.get_upper_lower_boundary(pk, current_lower)
            if current_upper is not None and next_lower is None:
                max_upper = current_upper
                stop_check = True
            elif current_upper is None and next_lower is None:
                current_upper = check_table.get_max_boundary(pk)
                max_upper = current_upper
                stop_check = True
            else:
                init_lower = next_lower
            # 只计算一段范围内的chunk
            if self.upper_boundary != -1 and self.upper_boundary <= current_upper:
                current_upper = self.upper_boundary
                stop_check = True
            # 生成SQL
            sql = check_table.generate_checksum_sql(chunk_num, current_lower, current_upper, pk)
            self.exec_sql(chunk_num, sql)
            chunk_num = chunk_num + 1

        # 对于不指定范围的检查，需要添加两个额外的chunk
        if self.lower_boundary == -1:
            sql = check_table.generate_checksum_sql(chunk_num, min_lower, None, pk)
            self.exec_sql(chunk_num, sql)
            chunk_num = chunk_num + 1
        if self.upper_boundary == -1:
            sql = check_table.generate_checksum_sql(chunk_num, None, max_upper, pk)
            self.exec_sql(chunk_num, sql)
            chunk_num = chunk_num + 1
        # 返回chunk的个数
        return chunk_num

    def exec_sql(self, chunk_num, sql):
        print sql
        cur = self.db_conn.cursor()
        start_time = time.time()
        try:
            cur.execute(sql)
            self.db_conn.commit()
        except MySQLdb.Error as e:
            print "do_check: MySQL Error: %s, rollback" % str(e)
            self.db_conn.rollback()
            cur.close()
            sys.exit(-1)
        end_time = time.time()
        cur.close()
        self.update_crc32_time((end_time - start_time), chunk_num)

# 根据slave上的checksum表对比数据


class Compare(object):
    def __init__(self, db_checksum_table, db_target_table, chunk_count, dest_conn_info):
        self.db_conn = get_db_conn(**dest_conn_info)
        self.db_checksum_table = db_checksum_table
        self.db_target_table = db_target_table
        self.chunk_count = chunk_count

    def close_conn(self):
        try:
            self.db_conn.close()
        finally:
            print "db conection closed."

    def wait_until_max_chunk(self):
        checksum_schema, checksum_table = self.db_checksum_table.split(".")
        current_chunk_count = 0

        # 检查checksum表是否存在？
        while current_chunk_count < self.chunk_count:
            # 等待dest db 数据同步
            time.sleep(2)
            cur = self.db_conn.cursor()
            sql = "select count(*) from `%s`.`%s`" % (checksum_schema, checksum_table)
            cur.execute(sql)
            res = cur.fetchone()
            self.db_conn.commit()
            print "wating for chunk sync,the count of chunk is:%s" % res
            if res is not None:
                current_chunk_count = res[0]
            else:
                print "wait_until_max_chunk:not exit max_chunk_index"
                sys.exit(-1)
            cur.close()

    # 检查check表，判断是否一致
    def do_compare(self):
        checksum_schema, checksum_table = self.db_checksum_table.split(".")
        target_schema, target_table = self.db_target_table.split(".")
        cur = self.db_conn.cursor()

        sql = "SELECT CONCAT(db, '.', tbl) AS `table`, chunk, chunk_index," + \
            "lower_boundary, upper_boundary, COALESCE(this_cnt-master_cnt, 0) AS cnt_diff," + \
            "COALESCE(this_crc <> master_crc OR ISNULL(master_crc) <> ISNULL(this_crc), 0) AS crc_diff," + \
            "this_cnt, master_cnt, this_crc, master_crc FROM `{0}`.`{1}`" + \
            "WHERE (master_cnt <> this_cnt OR master_crc <> this_crc OR ISNULL(master_crc) <> ISNULL(this_crc))" + \
            "AND (db='{2}' AND tbl='{3}')"
        sql = sql.format(checksum_schema, checksum_table, target_schema, target_table)
        cur.execute(sql)
        res = cur.fetchall()
        cur.close()

        if len(res) > 0:
            print "表 %s.%s 数据不一致chunk数：%d " % (target_schema, target_table, len(res))
            diff_range = dict()
            for r in res:
                result = "table:'{0}', chunk:{1}, chunk_index:'{2}', lower_boundary:'{3}', upper_boundary:'{4}',\
cnt_diff:{5}, crc_diff:{6}, this_cnt:{7}, master_cnt:{8}, this_crc:'{9}', master_crc:'{10}'"
                result = result.format(r[0], int(r[1]), r[2], r[3], r[4], int(r[5]),
                                       int(r[6]), int(r[7]), int(r[8]), r[9], r[10])
                diff_range[self.db_target_table] = [r[3], r[4]]
                print "-" * 80
                print result
                print "-" * 80
            return diff_range
        else:
            print "表 %s.%s 数据一致" % (target_schema, target_table)
            return None


def process_table(db_target_table, result_table, chunk_size,
                  lower_boundary, upper_boundary):

    checksum = CheckSum(result_table, db_target_table,
                        chunk_size, lower_boundary, upper_boundary, SRC_CONN_INFO)

    print "generate crc32 for table: %s" % db_target_table
    chunk_count = checksum.do_check()
    checksum.close_conn()

    print "check table: %s" % db_target_table
    compare = Compare(result_table, db_target_table, chunk_count, DEST_CONN_INFO)
    compare.wait_until_max_chunk()
    diff_range = compare.do_compare()
    compare.close_conn()
    return diff_range


def init_result_table(table_name, db_conn_info):
    conn = get_db_conn(**db_conn_info)
    cur = conn.cursor()
    print "create table:%s" % table_name
    create_table_sql = """
                CREATE TABLE IF NOT EXISTS {} (
                    db CHAR(64) NOT NULL,
                    tbl CHAR(64) NOT NULL,
                    chunk INT NOT NULL,
                    chunk_time FLOAT NULL,
                    chunk_index VARCHAR(200) NULL,
                    lower_boundary TEXT NULL,
                    upper_boundary TEXT NULL,
                    this_crc CHAR(40) NOT NULL,
                    this_cnt INT NOT NULL,
                    master_crc CHAR(40) NULL,
                    master_cnt INT NULL,
                    ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    PRIMARY KEY (db,tbl,chunk),
                    INDEX ts_db_tbl(ts,db,tbl)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """.format(table_name)
    try:
        cur.execute(create_table_sql)
    except MySQLdb.Error as e:
        print "create checksum table error:%s " % str(e)
        conn.close()
        sys.exit(-1)

    print "delete old data from %s if exists in table" % table_name
    delete_result_sql = "DELETE FROM `{0}`.`{1}`"
    checksum_schema, checksum_table = table_name.split(".")
    delete_result_sql = delete_result_sql.format(checksum_schema, checksum_table)
    try:
        cur.execute(delete_result_sql)
        conn.commit()
        cur.close()
    except MySQLdb.Error as e:
        print "clear_checksum_table: MySQL Error: %s, rollback" % str(e)
        conn.rollback()
        cur.close()
        conn.close()
        sys.exit(-1)

    conn.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="checksum args description")
    parser.add_argument("--target_table", type=str, required=True, help="the target table:db_name.table_name")
    parser.add_argument("--checksum_table", type=str, required=True, help="the checksum table:db_name.table_name")
    parser.add_argument("--chunk_size", type=int, default=1000, help="chunk size")
    parser.add_argument("--lower_boundary", type=int, default=-1, help="the lower boundary to check")
    parser.add_argument("--upper_boundary", type=int, default=-1, help="the upper boundary to check")
    args = parser.parse_args()

    db_target_table = args.target_table
    db_checksum_table = args.checksum_table
    chunk_size = args.chunk_size
    lower_boundary = args.lower_boundary
    upper_boundary = args.upper_boundary

    # 初始化结果表，包括:checksum和diff
    init_result_table(db_checksum_table, SRC_CONN_INFO)

    diff_range = process_table(db_target_table, db_checksum_table, chunk_size,
                               lower_boundary, upper_boundary)

    if diff_range is None:
        sys.exit(0)
    else:
        sys.exit(1)
