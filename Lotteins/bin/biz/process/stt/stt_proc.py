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
import sox
import logging
import datetime
import cx_Oracle
import traceback
from biz.client import stt_client
from biz.common import util, db_connection, biz_worker
sys.path.append(os.path.join(os.getenv('MAUM_ROOT'), "lib/python"))
from maum.biz.common import common_pb2
from maum.biz.proc import stt_pb2


#########
# class #
#########
class SttProc(biz_worker.BizWorker):
    def __init__(self, index, frontend_port, backend_port, logq, stt_addr):
        biz_worker.BizWorker.__init__(self, frontend_port, backend_port, logq)
        self.index = index
        self.complete_cnt = 0
        self.stt_addr = stt_addr

    def insert_stt_result_detail(self, oracle, hdr, stt_result_id, result):
        try:
            seq = 1
            rt_values = list()
            for segment in result.segments:
                my_seq = oracle.cursor.var(cx_Oracle.NUMBER)
                query = """
                INSERT INTO STT_RESULT_DETAIL_TB
                (
                    STT_RESULT_ID,
                    SPEAKER_CODE,
                    SENTENCE_ID,
                    SENTENCE,
                    START_TIME,
                    END_TIME,
                    CREATED_DTM,
                    UPDATED_DTM,
                    CREATOR_ID,
                    UPDATOR_ID
                )
                VALUES
                (
                    :1, :2, :3, :4, 
                    :5, :6, SYSDATE,
                    SYSDATE, :7, :8
                )
                RETURNING STT_RESULT_DETAIL_ID INTO :9
                """
                bind = (
                    stt_result_id,
                    hdr.call_metadata.speaker_code,
                    seq,
                    segment.txt,
                    segment.start,
                    segment.end,
                    hdr.creator_id,
                    hdr.creator_id,
                    my_seq,
                )
                oracle.cursor.execute(query, bind)
                stt_result_detail_id = my_seq.getvalue()
                stt_result_detail_id = int(stt_result_detail_id)
                rt_values.append((stt_result_detail_id, seq, segment.txt))
                seq += 1
            oracle.conn.commit()
            return rt_values
        except Exception:
            self.log(logging.ERROR, traceback.format_exc())

    def do_work(self, frames):
        print 'stt receive frames ({0})'.format(frames)
        header, body = frames
        hdr = common_pb2.Header()
        hdr.ParseFromString(header)
        audio = common_pb2.Audio()
        audio.ParseFromString(body)
        print 'Received request {0}, {1}'.format(hdr, audio)
        client = stt_client.SttClient(self.stt_addr)
        img_name, img_lang, img_sample_rate = hdr.model_params.split('-')
        hdr.status_id = util.ProcStatus.PS_INPROGRESS
        oracle = db_connection.Oracle()
        util.insert_proc_result(oracle, self.logger, hdr)
        if client.get_servers(_name=img_name, _lang=img_lang, _sample_rate=int(img_sample_rate)):
            pcm_filename = audio.filename
            if pcm_filename.endswith('wav'):
                # 화자 분리
                # https://www.nesono.com/node/275
                # sox infile.wav outfile.l.wav remix 1
                # wav 파일 pcm 파일로 전환, sox 에서는 raw 파일이 pcm 으로 취급
                pcm_filename = audio.filename[:-3] + 'raw'
                tfm = sox.Transformer()
                # 같은 파일이 다른 모델로 동시에 처리될 때
                if os.path.exists(pcm_filename):
                    pass
                else:
                    tfm.build(audio.filename, pcm_filename)
            result = client.detail_recognize(pcm_filename)
            if pcm_filename != audio.filename:
                print pcm_filename, audio.filename
                # TODO : CS 후처리
                # os.remove(pcm_filename)
            self.complete_cnt += 1
            # STT_RESULT 테이블에 기록
            stt_result_id = insert_stt_result(oracle, hdr, result)
            stt_result_detail = self.insert_stt_result_detail(oracle, hdr, stt_result_id, result)
            # FLOW_RESULT 테이블에 기록
            hdr.status_id = util.ProcStatus.PS_COMPLETED
            hdr.stt_result_id = stt_result_id
            util.insert_proc_result(oracle, self.logger, hdr)
            # 다음 처리할 프로세스로 데이타 전달
            self.log(logging.DEBUG, "stt result is {0}".format(result.txt))
            # self.sendto_router(info.flow_set, info.flow_id, info.call_id, result)
            # 여러문장 동시 전송
            stt_result = stt_pb2.SttResultDetail()
            stt_result.stt_result_id = stt_result_id
            for sentence in stt_result_detail:
                print sentence
                stt_result.sentence.add(stt_result_detail_id=sentence[0], sequence=sentence[1], sentence=sentence[2])
            # 전체 문장 한꺼번에 전송
            # stt_result = stt_pb2.SttResult()
            # stt_result.result = result.txt
            print stt_result.SerializeToString()
            new_body = stt_result.SerializeToString()
            self.sendto_router_ex(hdr, new_body)
        else:
            self.log(logging.DEBUG, 'CS Service unavailable')
            hdr.status_id = util.ProcStatus.PS_FAILED
            util.insert_proc_result(oracle, self.logger, hdr)
        oracle.disconnect()


#######
# def #
#######
def insert_stt_result(oracle, hdr, result):
    """
    Insert stt result
    :param          oracle:         Oracle
    :param          hdr:            Header
    :param          result:         CS result
    :return:                        CS result id
    """
    my_seq = oracle.cursor.var(cx_Oracle.NUMBER)
    query = """
    INSERT INTO STT_RESULT_TB
    (
        PIPELINE_EVENT_ID,
        TICKET_ID,
        CALL_ID,
        SPEAKER_CODE,
        MLF,
        CREATED_DTM,
        UPDATED_DTM,
        CREATOR_ID,
        UPDATOR_ID
    )
    VALUES
    (
        :1, :2, :3, :4, :5,
        SYSDATE, SYSDATE, :6, :7
    )
    RETURNING STT_RESULT_ID INTO :8
    """
    bind = (
        hdr.pipeline_event_id,
        hdr.ticket_id,
        hdr.call_metadata.call_id,
        hdr.call_metadata.speaker_code,
        result.txt.replace("'", "\'"),
        hdr.creator_id,
        hdr.creator_id,
        my_seq,
    )
    oracle.cursor.execute(query, bind)
    stt_result_id = my_seq.getvalue()
    stt_result_id = int(stt_result_id)
    oracle.conn.commit()
    return stt_result_id