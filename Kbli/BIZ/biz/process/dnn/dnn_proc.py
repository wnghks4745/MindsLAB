#!/usr/bin/python
# -*- coding: utf-8 -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-06-19, modification: 0000-00-00"

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
                detect_cnt = 0
                call_date = '' if hdr.call_metadata.call_date == 'None' else hdr.call_metadata.call_date
                for document in nlp_result.documentList:
                    nlp_id = document.nlp_result_id
                    nlp_result = document.sentence
                    summary = client.get_class(nlp_result)
                    summary.c1_top = summary.c1_top.replace("'", "\'")
                    if summary.c1_top == 'voc':
                        detect_cnt += 1
                    self.log(logging.DEBUG, "NLP_ID[{0}] DNN result is {1}".format(nlp_id, summary.c1_top))
                dnn_classify_code = 'voc' if detect_cnt > 0 else 'normal'
                query = """
                    INSERT INTO TA_DNN_RESULT_TB
                    (
                        CALL_DATE,
                        CALL_ID,
                        SPEAKER_CODE,
                        DNN_CLASSIFY_CODE,
                        DETECT_CNT,
                        CREATED_DTM,
                        UPDATED_DTM,
                        CREATOR_ID,
                        UPDATOR_ID
                    )
                    VALUES
                    (
                        TO_DATE(:1, 'YYYY/MM/DD'),
                        :2, :3, :4, :5, SYSDATE,
                        SYSDATE, :6, :7
                    )
                """
                bind = (
                    call_date,
                    hdr.call_id,
                    hdr.call_metadata.speaker_code,
                    dnn_classify_code,
                    detect_cnt,
                    hdr.creator_id,
                    hdr.creator_id,
                )
                oracle.cursor.execute(query, bind)
                oracle.conn.commit()
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
