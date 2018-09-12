#!/usr/bin/python
# -*- coding: utf-8 -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 0000-00-00, modification: 0000-00-00"

###########
# imports #
###########
import os
import sys
import traceback
import cx_Oracle


#########
# class #
#########
class PrdOracleConfig(object):
    host = '172.18.200.143'
    host_list = ['172.18.200.143', '172.18.200.144']
    user = 'STTAAPP'
    pd = 'sttasvc!@'
    port = 2525
    service_name = 'PUSTTA'
    reconnect_interval = 10
    con_db = 'prd'


class DevOracleConfig(object):
    host = '172.18.217.146'
    host_list = ['172.18.217.146']
    user = 'STTAADM'
    pd = 'sttasvc!@'
    port = 1525
    sid = 'DUSTTA'
    reconnect_interval = 10
    con_db = 'dev'


class Oracle(object):
    def __init__(self, conf, host_reverse=False, failover=False):
        import cx_Oracle
        self.conf = conf
        os.environ["NLS_LANG"] = ".AL32UTF8"
        if failover:
            self.dsn_tns = '(DESCRIPTION = (ADDRESS_LIST= (FAILOVER = on)(LOAD_BALANCE = off)'
            if host_reverse:
                self.conf.host_list.reverse()
            for host in self.conf.host_list:
                self.dsn_tns += '(ADDRESS= (PROTOCOL = TCP)(HOST = {0})(PORT = {1}))'.format(host, self.conf.port)
            if self.conf.con_db == 'dev':
                self.dsn_tns += ')(CONNECT_DATA=(SID={0})))'.format(self.conf.sid)
            else:
                self.dsn_tns += ')(CONNECT_DATA=(SERVICE_NAME={0})))'.format(self.conf.service_name)
        else:
            if self.conf.con_db == 'dev':
                self.dsn_tns = cx_Oracle.makedsn(
                    self.conf.host,
                    self.conf.port,
                    sid=self.conf.sid
                )
            else:
                self.dsn_tns = cx_Oracle.makedsn(
                    self.conf.host,
                    self.conf.port,
                    service_name=self.conf.service_name
                )
        self.conn = cx_Oracle.connect(
            self.conf.user,
            self.conf.pd,
            self.dsn_tns
        )
        self.cursor = self.conn.cursor()

    def disconnect(self):
        try:
            self.cursor.close()
            self.conn.close()
        except Exception:
            raise Exception(traceback.format_exc())

    def insert_brand_keyword(self, values_list):
        query = """
            INSERT INTO TB_TADC_BRAND_DIC
            (
                BRANDNAME,
                CREATE_DT,
                MODIFY_DT
            )
            VALUES
            (
                :1,
                SYSDATE,
                SYSDATE
            )
        """
        self.cursor.executemany(query, values_list)
        if self.cursor.rowcount > 0:
            self.conn.commit()
        else:
            self.conn.rollback()

    def delete_brand_keyword(self):
        try:
            query = """
                DELETE FROM
                    TB_TADC_BRAND_DIC
            """
            self.cursor.execute(query, )
            if self.cursor.rowcount > 0:
                self.conn.commit()
                return True
            else:
                self.conn.rollback()
                return False
        except Exception:
            self.conn.rollback()
            raise Exception(traceback.format_exc())

    def insert_emotion_keyword(self, values_list):
        query = """
            INSERT INTO TB_TADC_SENTI_KEYWORD
            (
                SENTI_KEYWORD,
                SENTI_LEVEL,
                CREATE_DT,
                MODIFY_DT
            )
            VALUES
            (
                :1,
                :2,
                SYSDATE,
                SYSDATE
            )
        """
        self.cursor.executemany(query, values_list)
        if self.cursor.rowcount > 0:
            self.conn.commit()
        else:
            self.conn.rollback()

    def delete_emotion_keyword(self):
        try:
            query = """
                DELETE FROM
                    TB_TADC_SENTI_KEYWORD
            """
            self.cursor.execute(query, )
            if self.cursor.rowcount > 0:
                self.conn.commit()
                return True
            else:
                self.conn.rollback()
                return False
        except Exception:
            self.conn.rollback()
            raise Exception(traceback.format_exc())

    def insert_stopword_keyword(self, values_list):
        query = """
            INSERT INTO TB_TADC_DEL_KEYWORD
            (
                DEL_KEYWORD,
                CREATE_DT,
                MODIFY_DT
            )
            VALUES
            (
                :1,
                SYSDATE,
                SYSDATE
            )
        """
        self.cursor.executemany(query, values_list)
        if self.cursor.rowcount > 0:
            self.conn.commit()
        else:
            self.conn.rollback()

    def delete_stopword_keyword(self):
        try:
            query = """
                DELETE FROM
                    TB_TADC_DEL_KEYWORD
            """
            self.cursor.execute(query, )
            if self.cursor.rowcount > 0:
                self.conn.commit()
                return True
            else:
                self.conn.rollback()
                return False
        except Exception:
            self.conn.rollback()
            raise Exception(traceback.format_exc())


########
# main #
########
def main(target_file_name, con_db):
    """
    This program that insert db
    :param      target_file_name:       Target file name
    :param      con_db:                 PRD or DEV
    """
    if not con_db.lower().strip() in ['prd', 'dev']:
        print "[ERROR] Choose dev or prd"
        sys.exit(1)
    try:
        if con_db.lower().strip() == 'dev':
            oracle = Oracle(DevOracleConfig)
        else:
            oracle = Oracle(PrdOracleConfig)
    except Exception:
        exc_info = traceback.format_exc()
        print exc_info
        print "[ERROR] Can't connect DB"
        sys.exit(1)
    target_db = os.path.splitext(os.path.basename(target_file_name))[0]
    if target_db.lower() == 'brand':
        if os.path.exists(target_file_name):
            keyword_dict = dict()
            target_file = open(target_file_name)
            for line in target_file:
                line = line.strip()
                line_list = line.split('\t')
                if len(line) < 1:
                    continue
                keyword = line_list[0]
                keyword_dict[keyword] = 1
            target_file.close()
            bind_list = list()
            for keyword in keyword_dict.keys():
                bind_list.append((keyword,))
            oracle.delete_brand_keyword()
            oracle.insert_brand_keyword(bind_list)
        else:
            print "[ERROR] Not existed {0}".format(target_file_name)
            sys.exit(1)
    elif target_db.lower() == 'emotion':
        if os.path.exists(target_file_name):
            keyword_dict = dict()
            target_file = open(target_file_name)
            for line in target_file:
                line = line.strip()
                line_list = line.split('\t')
                if len(line_list) != 2:
                    continue
                if len(line) < 1:
                    continue
                keyword, level = line_list
                keyword_dict[keyword] = level
            target_file.close()
            bind_list = list()
            for keyword, level in keyword_dict.items():
                bind_list.append((keyword, level))
            oracle.delete_emotion_keyword()
            oracle.insert_emotion_keyword(bind_list)
        else:
            print "[ERROR] Not existed {0}".format(target_file_name)
            sys.exit(1)
    elif target_db.lower() == 'stopword':
        if os.path.exists(target_file_name):
            keyword_dict = dict()
            target_file = open(target_file_name)
            for line in target_file:
                line = line.strip()
                line_list = line.split('\t')
                if len(line) < 1:
                    continue
                keyword = line_list[0]
                keyword_dict[keyword] = 1
            target_file.close()
            bind_list = list()
            for keyword in keyword_dict.keys():
                bind_list.append((keyword,))
            oracle.delete_stopword_keyword()
            oracle.insert_stopword_keyword(bind_list)
        else:
            print "[ERROR] Not existed {0}".format(target_file_name)
            sys.exit(1)
    else:
        print "[ERROR] Wrong target file name.[brand.txt, emotion.txt, stopword.txt]"


if __name__ == '__main__':
    if len(sys.argv) == 3:
        main(sys.argv[1], sys.argv[2])
    else:
        print "usage : python {0} [Input target file] [PRD or DEV]".format(sys.argv[0])
        print "ex) python {0} brand.txt dev".format(sys.argv[0])
        sys.exit(1)
