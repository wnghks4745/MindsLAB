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
import json
import logging
import traceback
import cx_Oracle
from google.protobuf import json_format
from biz.client import nlp_client
from biz.common import util, db_connection, biz_worker
sys.path.append(os.path.join(os.getenv('MAUM_ROOT'), "lib/python"))
from common.config import Config
from maum.biz.common import common_pb2
from maum.biz.proc import stt_pb2, nlp_pb2
from maum.brain.nlp import nlp_pb2 as brain_nlp_pb2


#########
# class #
#########
class NlpProc(biz_worker.BizWorker):
    def __init__(self, index, frontend_port, backend_port, logq):
        biz_worker.BizWorker.__init__(self, frontend_port, backend_port, logq)
        self.index = index

    def initialize(self):
        """callback"""
        pass

    def do_work(self, frames):
        header, body = frames
        hdr = common_pb2.Header()
        oracle = db_connection.Oracle()
        try:
            hdr.ParseFromString(header)
            # 통 문장 분석 시
            # stt_result = stt_pb2.SttResult()
            # 다중 문장 분석 시
            stt_result = stt_pb2.SttResultDetail()
            stt_result.ParseFromString(body)
            client = nlp_client.NlpClient()
            hdr.status_id = util.ProcStatus.PS_INPROGRESS
            oracle = db_connection.Oracle()
            util.insert_proc_result(oracle, self.logger, hdr)
            level1, level2 = hdr.model_params.split('_')
            rc = client.get_provider()
            if rc is not None:
                hdr.status_id = util.ProcStatus.PS_FAILED
                util.insert_proc_result(oracle, self.logger, hdr)
                self.log(logging.ERROR, rc)
                return
            nlp_result_list = nlp_pb2.nlpResultDetail()
            nlp_document = brain_nlp_pb2.Document()
            sentence_cnt = 0
            for sentence in stt_result.sentence:
                sentence_cnt += 1
                try:
                    result = client.analyze(sentence.sentence, int(level1), int(level2))
                except Exception:
                    try:
                        result = client.analyze(sentence.sentence, int(level1), int(level2))
                    except Exception:
                        try:
                            result = client.analyze(sentence.sentence, int(level1), int(level2))
                        except Exception:
                            self.log(logging.ERROR, "REC_ID = {0}, RFILE_NAME = {1}".format(
                                hdr.call_metadata.record_key, hdr.call_metadata.file_name))
                            self.log(logging.ERROR, traceback.format_exc())
                            continue
                nlp_result_id = record_result(oracle, hdr, sentence, result)
                nlp_result_list.documentList.add(
                    nlp_result_id=nlp_result_id,
                    sequence=sentence.sequence,
                    sentence=sentence.sentence,
                    document=result[2],
                    start_time=sentence.start_time,
                    end_time=sentence.end_time,
                    stt_result_detail_id=sentence.stt_result_detail_id
                )
                try:
                    record_result_detail(oracle, hdr, result, nlp_result_id)
                except Exception:
                    continue
            self.log(logging.INFO, "{0} NLP Process Success".format(sentence_cnt))
            hdr.status_id = util.ProcStatus.PS_COMPLETED
            util.insert_proc_result(oracle, self.logger, hdr)
            new_body = nlp_result_list.SerializeToString()
            self.sendto_router_ex(hdr, new_body)
        except grpc.RpcError:
            hdr.status_id = util.ProcStatus.PS_FAILED
            util.insert_proc_result(oracle, self.logger, hdr)
            self.log(logging.ERROR, "REC_ID = {0}, RFILE_NAME = {1}".format(
                hdr.call_metadata.record_key, hdr.call_metadata.file_name))
            self.log(logging.ERROR, traceback.format_exc())
        except Exception:
            hdr.status_id = util.ProcStatus.PS_FAILED
            util.insert_proc_result(oracle, self.logger, hdr)
            self.log(logging.ERROR, traceback.format_exc())
        finally:
            oracle.disconnect()


#######
# def #
#######
def record_result(oracle, hdr, sentence, result):
    """
    NLP record result
    :param          oracle:         DB
    :param          hdr:            Header
    :param          sentence:       Sentence
    :param          result:         NLP result
    :return:                        NLP result id
    """
    call_date = '' if hdr.call_metadata.call_date == 'None' else hdr.call_metadata.call_date
    my_seq = oracle.cursor.var(cx_Oracle.NUMBER)
    query = """
    INSERT INTO TA_NLP_RESULT_TB
    (
        PIPELINE_EVENT_ID,
        STT_RESULT_DETAIL_ID,
        TICKET_ID,
        NLP_SENTENCE,
        CREATED_DTM,
        UPDATED_DTM,
        CREATOR_ID,
        UPDATOR_ID,
        CALL_DATE
    )
    VALUES(
        :1, :2, :3,
        :4, SYSDATE,
        SYSDATE, :5, :6,
        TO_DATE(:7, 'YYYY-MM-DD')
    )
    RETURNING NLP_RESULT_ID INTO :9
    """
    bind = (
        hdr.pipeline_event_id,
        sentence.stt_result_detail_id,
        hdr.ticket_id,
        result[0].replace("'", "\'"),
        hdr.creator_id,
        hdr.creator_id,
        call_date,
        my_seq,
    )
    oracle.cursor.execute(query, bind)
    nlp_result_id = my_seq.getvalue()
    nlp_result_id = int(nlp_result_id)
    oracle.conn.commit()
    return nlp_result_id


def record_result_detail(oracle, hdr, result, nlp_result_id):
    """
    NLP record detail result
    :param          oracle:                 DB
    :param          hdr:                    Header
    :param          result:                 NLP result
    :param          nlp_result_id:          NLP result id
    """
    keywords = list()
    result_json = json.loads(result[1])
    call_date = '' if hdr.call_metadata.call_date == 'None' else hdr.call_metadata.call_date
    morp_seq = 1
    target_morphs_list = ['MAG', 'NNG', 'NNP', 'NP', 'VA', 'VV']
    for morp in result_json['sentences'][0]['morps']:
        if morp['type'].upper() in target_morphs_list:
            if len(morp['lemma']) < 2:
                continue
            keywords.append(
                (
                    nlp_result_id, 'NC0001', morp['type'].upper(), morp_seq, morp['lemma'], hdr.creator_id,
                    hdr.creator_id, call_date
                )
            )
            morp_seq += 1
    query = """
    INSERT INTO TA_NLP_RESULT_KEYWORD_TB
    (
        NLP_RESULT_ID,
        RESULT_TYPE_CODE,
        KEYWORD_CLASSIFY_CODE,
        KEYWORD_SEQUENCE,
        WORD,
        CREATED_DTM,
        UPDATED_DTM,
        CREATOR_ID,
        UPDATOR_ID,
        CALL_DATE
    )
    VALUES
    (
        :1, :2, :3,
        :4, :5, SYSDATE,
        SYSDATE, :6, :7,
        TO_DATE(:8, 'YYYY/MM/DD')
    )
    """
    oracle.cursor.executemany(query, keywords)
    oracle.conn.commit()
