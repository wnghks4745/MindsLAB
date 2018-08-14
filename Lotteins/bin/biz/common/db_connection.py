#!/usr/bin/python
# -*- coding: utf-8 -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-05-07, modification: 0000-00-00"

###########
# imports #
###########
import os
import cx_Oracle
import traceback
from common.config import Config

#########
# class #
#########
class Oracle(object):
    def __init__(self):
        os.environ["NLS_LANG"] = ".AL32UTF8"
        self.conf = Config()
        self.conf.init('biz.conf')
        self.dsn_tns = cx_Oracle.makedsn(
            self.conf.get('oracle.host'),
            self.conf.get('oracle.port'),
            sid=self.conf.get('oracle.sid')
        )
        self.conn = cx_Oracle.connect(
            self.conf.get('oracle.user'),
            self.conf.get('oracle.passwd'),
            self.dsn_tns
        )
        self.cursor = self.conn.cursor()

    def disconnect(self):
        try:
            self.cursor.close()
            self.conn.close()
        except Exception:
            raise Exception(traceback.format_exc())
