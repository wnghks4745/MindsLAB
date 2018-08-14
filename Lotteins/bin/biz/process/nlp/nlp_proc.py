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
                result = client.analyze(sentence.sentence, int(level1), int(level2))
                nlp_result_id = record_result(oracle, hdr, sentence, result)
                nlp_result_list.documentList.add(
                    nlp_result_id=nlp_result_id,
                    sentence=sentence.sentence,
                    document=result[2]
                )
                record_result_detail(oracle, hdr, result, nlp_result_id)
            print "{0} NLP Process Success".format(sentence_cnt)
            self.log(logging.INFO, "{0} NLP Process Success".format(sentence_cnt))
            # Insert CS_PERFECT_SALES_CALL_LIST_TB
            if hdr.call_metadata.speaker_code  == 'ST0002':
                if hdr.call_metadata.business_dcd in ['CJ0016', 'CJ0017']:
                    insert_perfect_sales_tb(oracle, hdr)
            hdr.status_id = util.ProcStatus.PS_COMPLETED
            util.insert_proc_result(oracle, self.logger, hdr)
            new_body = nlp_result_list.SerializeToString()
            self.sendto_router_ex(hdr, new_body)
        except grpc.RpcError as e:
            print 'grpc.RpcError: {0}'.format(e)
            hdr.status_id = util.ProcStatus.PS_FAILED
            util.insert_proc_result(oracle, self.logger, hdr)
            self.log(logging.ERROR, e)
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
    my_seq = oracle.cursor.var(cx_Oracle.NUMBER)
    query = """
    INSERT INTO TA_NLP_RESULT_TB
    (
        PIPELINE_EVENT_ID,
        STT_RESULT_DETAIL_ID,
        TICKET_ID,
        NLP_SENTENCE,
        NLP_RESULT_JSON,
        CREATED_DTM,
        UPDATED_DTM,
        CREATOR_ID,
        UPDATOR_ID
    )
    VALUES(
        :1, :2, :3,
        :4, :5, SYSDATE,
        SYSDATE, :6, :7
    )
    RETURNING NLP_RESULT_ID INTO :8
    """
    bind = (
        hdr.pipeline_event_id,
        sentence.stt_result_detail_id,
        hdr.ticket_id,
        result[0].replace("'", "\'"),
        result[1].replace("'", "\'"),
        hdr.creator_id,
        hdr.creator_id,
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
    morp_seq = 1
    target_morphs_list = ['MAG', 'NNG', 'NNP', 'NP', 'VA', 'VV']
    for morp in result_json['sentences'][0]['morps']:
        if morp['type'].upper() in target_morphs_list:
            if len(morp['lemma']) < 2:
                continue
            keywords.append(
                (
                    nlp_result_id, 'NC0001', morp['type'].upper(), morp_seq, morp['lemma'], hdr.creator_id,
                    hdr.creator_id
                )
            )
            morp_seq += 1
#    ne_seq = 1
#    for ne in result_json['sentences'][0]['nes']:
#        keywords.append(
#            (
#                nlp_result_id, 'NC0002', ne['type'].upper(), ne_seq, ne['text'], hdr.creator_id, hdr.creator_id
#            )
#        )
#        ne_seq += 1
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
        UPDATOR_ID
    )
    VALUES
    (
        :1, :2, :3,
        :4, :5, SYSDATE,
        SYSDATE, :6, :7
    )
    """
    oracle.cursor.executemany(query, keywords)
    oracle.conn.commit()


def insert_perfect_sales_tb(oracle, hdr):
    """
    Insert CS_PERFECT_SALES_CALL_LIST_TB
    :param          oracle:         DB
    :param          hdr:            Header
    """
    query = """
        INSERT INTO CS_PERFECT_SALES_CALL_LIST_TB
        (
            CALL_ID,
            TICKET_ID,
            CALL_DATE,
            FILE_NAME,
            RUSER_ID,
            CU_ID,
            CU_NAME_HASH,
            PROCESSED_YN,
            CREATED_DTM,
            UPDATED_DTM,
            CREATOR_ID, 
            UPDATOR_ID
        )
        VALUES
        (
            :1, :2, TO_DATE(:3, 'YYYY-MM-DD'),
            :4, :5, :6, :7, 'N', SYSDATE,
            SYSDATE, 'CS_REC', 'CS_REC'
        ) 
    """
    bind = (
        hdr.call_id,
        hdr.ticket_id,
        hdr.call_metadata.call_date,
        hdr.call_metadata.file_name,
        hdr.call_metadata.ruser_id,
        hdr.call_metadata.cu_id,
        hdr.call_metadata.cu_name_hash,
    )
    oracle.cursor.execute(query, bind)
