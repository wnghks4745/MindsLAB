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
import grpc
import logging
import traceback
import cx_Oracle
from biz.client import dnn_client
from biz.common import biz_worker
from biz.common import util, db_connection
sys.path.append(os.path.join(os.getenv('MAUM_ROOT'), "lib/python"))
from maum.biz.proc import nlp_pb2
from maum.biz.common import common_pb2


#########
# class #
#########
class DnnProc(biz_worker.BizWorker):
    def __init__(self, index, frontend_port, backend_port, logq, dnn_addr):
        biz_worker.BizWorker.__init__(self, frontend_port, backend_port, logq)
        self.index = index
        self.dnn_addr = dnn_addr

    def initialize(self):
        """ callback """
        pass

    def do_work(self, frames):
        header, body = frames
        hdr = common_pb2.Header()
        hdr.ParseFromString(header)
        nlp_result = nlp_pb2.nlpResultDetail()
        nlp_result.ParseFromString(body)
        self.log(logging.INFO, "Message (pipeline_event_id:{0}) received".format(hdr.pipeline_event_id))
        client = dnn_client.ClassifierClient(self.dnn_addr)
        oracle = db_connection.Oracle()
        try:
            hdr.status_id = util.ProcStatus.PS_INPROGRESS
            util.insert_proc_result(oracle, self.logger, hdr)
            img_name, img_lang = hdr.model_params.split('-')
            if client.get_server(img_name, img_lang):
                for document in nlp_result.documentList:
                    nlp_id = document.nlp_result_id
                    nlp_result = document.sentence
                    summary = client.get_class(nlp_result)
                    summary.c1_top = summary.c1_top.replace("'", "\'")
                    query = """
                    INSERT INTO TA_DNN_RESULT_TB
                    (
                        NLP_RESULT_ID,
                        DNN_CLASSIFY_CODE,
                        PROBABILITY,
                        CREATED_DTM,
                        UPDATED_DTM,
                        CREATOR_ID,
                        UPDATOR_ID
                    )
                    VALUES
                    (
                        :1, :2, :3,
                        SYSDATE, SYSDATE,
                        :4, :5
                    )
                    """
                    bind = (
                        nlp_id,
                        summary.c1_top,
                        summary.probability_top,
                        hdr.creator_id,
                        hdr.creator_id,
                    )
                    oracle.cursor.execute(query, bind)
                    oracle.conn.commit()
                    self.log(logging.DEBUG, "DNN result is {0}".format(summary))
                    hdr.status_id = util.ProcStatus.PS_COMPLETED
                    util.insert_proc_result(oracle, self.logger, hdr)
            else:
                self.log(logging.ERROR, "Classifier Service unavailable")
                hdr.status_id = util.ProcStatus.PS_FAILED
                util.insert_proc_result(oracle, self.logger, hdr)
            oracle.disconnect()
        except grpc.RpcError as e:
            hdr.status_id = util.ProcStatus.PS_FAILED
            util.insert_proc_result(oracle, self.logger, hdr)
            self.log(logging.ERROR, e)
        except Exception:
            hdr.status_id = util.ProcStatus.PS_FAILED
            util.insert_proc_result(oracle, self.logger, hdr)
            self.log(logging.ERROR, traceback.format_exc())
