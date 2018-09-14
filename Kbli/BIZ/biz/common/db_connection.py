#!/usr/bin/python
# -*- coding: utf-8 -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-05-07, modification: 0000-00-00"

###########
# imports #
###########
import os
import sys
import cx_Oracle
import traceback
sys.path.append(os.path.join(os.getenv('MAUM_ROOT'), 'lib/python'))
from common.config import Config
from common.openssl import decrypt_string


#########
# class #
#########
class Oracle(object):
    def __init__(self):
        os.environ["NLS_LANG"] = ".AL32UTF8"
        self.conf = Config()
        self.conf.init('biz.conf')
        self.dsn_tns = self.conf.get('oracle.dsn').strip()
        passwd = decrypt_string(self.conf.get('oracle.passwd'))
        self.conn = cx_Oracle.connect(
            self.conf.get('oracle.user'),
            passwd,
            self.dsn_tns
        )
        self.cursor = self.conn.cursor()

    def disconnect(self):
        try:
            self.cursor.close()
            self.conn.close()
        except Exception:
            raise Exception(traceback.format_exc())
