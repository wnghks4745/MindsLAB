#!/usr/bin/python
# -*- coding:utf-8 -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-05-07, modification: 0000-00-00"

###########
# imports #
###########
import os
import sys
import zmq
import time
import traceback
import cx_Oracle
from biz.common import util, db_connection, logger
sys.path.append(os.path.join(os.getenv('MAUM_ROOT'), 'lib/python'))
from common.config import Config
from maum.biz.common import common_pb2
from maum.biz.proc import stt_pb2

###########
# options #
###########
reload(sys)
sys.setdefaultencoding('utf-8')

#########
# class #
#########
class DbCollector(object):
    def __init__(self):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUSH)
        self.logger = logger.get_timed_rotating_logger(
            logger_name='cs_db_collector',
            file_name=os.path.join(os.getenv('MAUM_ROOT'), 'logs/cs_db_collector_process.log'),
            backup_count=5,
            log_level='debug'
        )
        self.conf = Config()
        self.conf.init('biz.conf')
        self.pipeline_info = util.create_pipeline_info()
        self.tree_map = self.pipeline_info['tree']
        self.select_cnt = 1000

    def create_common_header(self, **kwargs):
        try:
            meta_info = kwargs.get('meta_info')
            header = common_pb2.Header()
            header.ticket_id = kwargs.get('ticket_id')
            header.call_id = kwargs.get('call_id')
            header.pipeline_id = kwargs.get('pipeline_id')
            header.pipeline_event_id = kwargs.get('pipeline_event_id')
            header.router_id = kwargs.get('router_id')
            header.proc_id = kwargs.get('proc_id')
            header.status_id = util.ProcStatus.PS_COMPLETED
            header.creator_id = meta_info[24] if meta_info[24] else 'None'
            header.call_metadata.call_id = kwargs.get('call_id')
            header.call_metadata.call_date = meta_info[0].strftime('%Y-%m-%d') if meta_info[0] else 'None'
            header.call_metadata.project_code = meta_info[1]
            header.call_metadata.file_name = meta_info[2]
            header.call_metadata.call_type_code = meta_info[3]
            header.call_metadata.contract_no = meta_info[4] if meta_info[4] else 'None'
            header.call_metadata.record_key = meta_info[5] if meta_info[5] else 'None'
            header.call_metadata.start_time = str(meta_info[6])
            header.call_metadata.end_time = str(meta_info[7])
            header.call_metadata.duration = meta_info[8]
            header.call_metadata.cti_call_id = meta_info[9] if meta_info[9] else 'None'
            header.call_metadata.ruser_id = meta_info[10] if meta_info[10] else 'None'
            header.call_metadata.ruser_name = meta_info[11] if meta_info[11] else 'None'
            header.call_metadata.ruser_number = meta_info[12]
            header.call_metadata.cu_id = meta_info[13] if meta_info[13] else 'None'
            header.call_metadata.cu_name = meta_info[14] if meta_info[14] else 'None'
            header.call_metadata.cu_name_hash = meta_info[15] if meta_info[15] else 'None'
            header.call_metadata.cu_number = meta_info[16] if meta_info[16] else 'None'
            header.call_metadata.in_call_number = meta_info[17]
            header.call_metadata.biz_cd = meta_info[18]
            header.call_metadata.chn_tp = meta_info[19]
            header.call_metadata.file_sprt = meta_info[20]
            header.call_metadata.rec_ext = meta_info[21]
            header.call_metadata.creator_id = meta_info[24] if meta_info[24] else 'None'
            header.call_metadata.updator_id = meta_info[25]
            header.call_metadata.speaker_code = kwargs.get('speaker_code')
            header.call_metadata.business_dcd = meta_info[26] if meta_info[26] else 'None'
            return header
        except Exception:
            self.logger.error(traceback.format_exc())
            raise Exception(traceback.format_exc())

    def send_to_router(self, header, body):
        self.socket.send_multipart([header, body])
        time.sleep(0.2)

    def do_cs_job(self):
        oracle = db_connection.Oracle()
        # Select CS target list
        cs_list = get_cs_list(oracle, self.select_cnt)
        for item in cs_list:
            call_id = ""
            (rfile_name,) = item
            try:
                # Get call meta information
                meta_info_temp = get_cs_meta_info(oracle, rfile_name)
                if not meta_info_temp:
                    self.logger.error(
                        "Can't select CS meta information. [RFILE_NAME = {0}]".format(rfile_name))
                    continue
                stt_spch_sped_rx = meta_info_temp[28]
                stt_spch_sped_tx = meta_info_temp[29]
                meta_info = meta_info_temp[:28]
                # Insert meta information
                call_id = create_call_id(oracle, meta_info)
                print '[START] CALL_ID : {0}, RFILE_NAME : {1}'.format(call_id, rfile_name)
                self.logger.info('[START] CALL_ID : {0}, RFILE_NAME : {1}'.format(call_id, rfile_name))
                # Get stt result
                stt_result = get_cs_stt_result(oracle, rfile_name)
                for stt_data in stt_result:
                    # Create ticket id
                    ticket_id = create_ticket_id(oracle, call_id, 'CS_REC', 'CS_REC')
                    for pipeline_id, tree in self.tree_map.iteritems():
                        root = tree.get_node(tree.root)
                        if root.data[9] == 'PT0006':
                            # Create pipeline_event_id
                            pipeline_event_id = create_pipeline_event_id(
                                oracle, pipeline_id, ticket_id, meta_info[24], meta_info[25])
                            if stt_data[4] == 'ST0001':
                                sc = 'C'
                            elif stt_data[4] == 'ST0002':
                                sc = 'A'
                            else:
                                sc = 'M'
                            speed = stt_spch_sped_rx if sc == 'C' else stt_spch_sped_tx
                            # Insert stt result
                            stt_result_id = insert_stt_result(
                                oracle, stt_data, call_id, ticket_id, pipeline_event_id, speed)
                            # Get stt detail
                            stt_detail_list = get_cs_stt_detail(oracle, rfile_name, sc)
                            # Insert stt_detail_list
                            stt_result_detail = insert_stt_detail(oracle, stt_result_id, stt_detail_list)
                            # Create message
                            hdr = self.create_common_header(
                                pipeline_id=pipeline_id,
                                router_id=root.identifier,
                                proc_id=root.data[1],
                                call_id=call_id,
                                ticket_id=ticket_id,
                                pipeline_event_id=pipeline_event_id,
                                meta_info=meta_info,
                                speaker_code=stt_data[4]
                            )
                            hdr.stt_result_id = stt_result_id
                            stt_result = stt_pb2.SttResultDetail()
                            stt_result.stt_result_id = stt_result_id
                            for sentence in stt_result_detail:
                                stt_result.sentence.add(
                                    stt_result_detail_id=sentence[0],
                                    sequence=sentence[1],
                                    sentence=sentence[2]
                                )
                            self.send_to_router(hdr.SerializeToString(), stt_result.SerializeToString())
                # Update TRANSFER_YN status
                update_cs_meta_trans_status(oracle, rfile_name, 'Y')
                oracle.conn.commit()
                print "[E N D] CALL_ID : {0}, RFILE_NAME : {1}".format(call_id, rfile_name)
                self.logger.info("[E N D] CALL_ID : {0}, RFILE_NAME : {1}".format(call_id, rfile_name))
            except Exception:
                oracle.conn.rollback()
                # Update TRANSFER_YN status
                update_cs_meta_trans_status(oracle, rfile_name, 'F')
                oracle.conn.commit()
                self.logger.error(traceback.format_exc())
                self.logger.error("[ERROR] CALL_ID : {0}, RFILE_NAME : {1}".format(call_id, rfile_name))
                continue
        oracle.disconnect()

    def run(self):
        self.logger.info('[START] CS DB Collector Process started')
        # Prepare context and sockets
        self.socket.connect("tcp://localhost:{0}".format(self.conf.get('router.pull.port')))
        # Main loop
        try:
            while True:
                # Process CS
                self.do_cs_job()
        except KeyboardInterrupt:
            self.logger.info('DB Collector stopped by Interrupt')
        except Exception:
            self.logger.error(traceback.format_exc())
            print traceback.format_exc()
        finally:
            self.logger.info('[E N D] DB Collector Process Stopped...')


#######
# def #
#######
def insert_stt_detail(oracle, stt_result_id, stt_detail_list):
    """
    Insert CS result detail
    :param          oracle:                 DB
    :param          stt_result_id:          STT_RESULT_ID
    :param          stt_detail_list:        CS detail result list
    :return:                                CS detail result for router
    """
    sc = 0
    rt_values = list()
    for stt_detail in stt_detail_list:
        sc += 1
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
                SPEED,
                SILENCE_YN,
                CREATED_DTM,
                UPDATED_DTM,
                CREATOR_ID, 
                UPDATOR_ID,
                SILENCE_TIME
            )
            VALUES
            (
                :1, :2, :3, :4, :5, :6, :7,
                :8, :9, :10, :11, :12, :13
            ) 
            RETURNING STT_RESULT_DETAIL_ID INTO :14
        """
        bind = (
            stt_result_id,
            stt_detail[0],
            sc,
            stt_detail[2],
            stt_detail[3],
            stt_detail[4],
            stt_detail[5],
            stt_detail[6],
            stt_detail[7],
            stt_detail[8],
            stt_detail[9],
            stt_detail[10],
            stt_detail[11],
            my_seq,
        )
        oracle.cursor.execute(query, bind)
        stt_result_detail_id = my_seq.getvalue()
        stt_result_detail_id = int(stt_result_detail_id)
        rt_values.append((stt_result_detail_id, sc, stt_detail[2]))
    return rt_values


def insert_stt_result(oracle, stt_data, call_id, ticket_id, pipeline_event_id, speed):
    """
    Insert CS result
    :param          oracle:                     DB
    :param          stt_data:                   CS result
    :param          call_id:                    CALL_ID
    :param          ticket_id:                  TICKET_ID
    :param          pipeline_event_id:          PIPELINE_EVENT_ID
    :param          speed:                      SPEECH SPEED
    :return:                                    STT_RESULT_ID
    """
    my_seq = oracle.cursor.var(cx_Oracle.NUMBER)
    query = """
        INSERT INTO STT_RESULT_TB
        (
            CREATED_DTM,
            UPDATED_DTM,
            CREATOR_ID,
            UPDATOR_ID,
            SPEAKER_CODE,
            CALL_ID,
            TICKET_ID,
            PIPELINE_EVENT_ID,
            SPEED
        )
        VALUES
        (
            :1, :2, :3, :4,
            :5, :6, :7, :8, :9
        ) 
        RETURNING STT_RESULT_ID INTO :10
    """
    bind = stt_data + (call_id, str(ticket_id), pipeline_event_id, speed, my_seq,)
    oracle.cursor.execute(query, bind)
    stt_result_id = my_seq.getvalue()
    stt_result_id = int(stt_result_id)
    return stt_result_id


def create_pipeline_event_id(oracle, pipeline_id, ticket_id, creator_id, updator_id):
    """
    Create PIPELINE_EVENT_ID
    :param          oracle:                 DB
    :param          pipeline_id:            PIPELINE_ID
    :param          ticket_id:              TICKET_ID
    :param          creator_id:             CREATOR_ID
    :param          updator_id:             UPDATOR_ID
    :return:                                PIPELINE_EVENT_ID
    """
    my_seq = oracle.cursor.var(cx_Oracle.NUMBER)
    query = """
        INSERT INTO PL_PIPELINE_EVENT_TB
        (
            PIPELINE_ID,
            TICKET_ID,
            CREATED_DTM,
            UPDATED_DTM,
            CREATOR_ID,
            UPDATOR_ID
        )
        VALUES
        (
            :1, :2, SYSDATE,
            SYSDATE, :3, :4
        )
        RETURNING PIPELINE_EVENT_ID INTO :5 
    """
    bind = (
        pipeline_id,
        ticket_id,
        creator_id,
        updator_id,
        my_seq,
    )
    oracle.cursor.execute(query, bind)
    pipeline_event_id = my_seq.getvalue()
    pipeline_event_id = int(pipeline_event_id)
    return pipeline_event_id


def create_ticket_id(oracle, call_id, creator_id, updator_id):
    """
    Create TICKET_ID
    :param          oracle:             DB
    :param          call_id:            CALL_ID
    :param          creator_id:         CREATOR_ID
    :param          updator_id:         UPDATOR_ID
    :return:                            TICKET_ID
    """
    my_seq = oracle.cursor.var(cx_Oracle.NUMBER)
    query = """
        INSERT INTO CM_TICKET_TB
        (
            CALL_ID,
            COLLECT_ID,
            CUSTOM_ID,
            AGENT_ID,
            CLIENT_ID,
            CREATED_DTM, 
            UPDATED_DTM,
            CREATOR_ID,
            UPDATOR_ID
        )
        VALUES
        (
            :1, 0, 0, NULL, NULL,
            SYSDATE, SYSDATE, :2, :3
        )
        RETURNING TICKET_ID INTO :4
    """
    bind = (
        call_id,
        creator_id,
        updator_id,
        my_seq,
    )
    oracle.cursor.execute(query, bind)
    ticket_id = my_seq.getvalue()
    ticket_id = int(ticket_id)
    return ticket_id


def create_call_id(oracle, meta_info):
    """
    Create CALL_ID
    :param          oracle:             DB
    :param          meta_info:          Meta information
    :return:                            CALL_ID
    """
    my_seq = oracle.cursor.var(cx_Oracle.NUMBER)
    query = """
        INSERT INTO CM_CALL_META_TB
        (
            CALL_DATE,
            PROJECT_CODE,
            FILE_NAME,
            CALL_TYPE_CODE,
            CONTRACT_NO,
            RECORD_KEY,
            START_TIME,
            END_TIME,
            DURATION,
            CTI_CALL_ID,
            RUSER_ID,
            RUSER_NAME, 
            RUSER_NUMBER,
            CU_ID,
            CU_NAME,
            CU_NAME_HASH,
            CU_NUMBER,
            IN_CALL_NUMBER,
            BIZ_CD,
            CHN_TP,
            FILE_SPRT,
            REC_EXT,
            CREATED_DTM,
            UPDATED_DTM,
            CREATOR_ID,
            UPDATOR_ID,
            BUSINESS_DCD,
            RUSER_USERGROUPCD
        )
        VALUES
        (
            TO_DATE(:1, 'YYYY/MM/DD'), :2, :3, :4, :5, :6, :7,
            :8, :9, :10, :11, :12, :13,
            :14, :15, :16, :17, :18, :19,
            :20, :21, :22, :23, :24, :25,
            :26, :27, :28
        )
        RETURNING CALL_ID INTO :29
    """
    call_date = meta_info[0].strftime('%Y-%m-%d') if meta_info[0] else ''
    bind = (call_date,) + meta_info[1:] + (my_seq,)
    oracle.cursor.execute(query, bind)
    call_id = int(my_seq.getvalue())
    return call_id


def update_cs_meta_trans_status(oracle, rfile_name, status):
    """
    Update CS meta TRANSFER_YN status
    :param      oracle:             DB
    :param      rfile_name:         RFILE_NAME
    :param      status:             Status
    """
    query = """
        UPDATE
            TB_CS_STT_RCDG_INFO
        SET
            TRANSFER_YN = :1
        WHERE
            RFILE_NAME = :2
    """
    bind = (status, rfile_name,)
    oracle.cursor.execute(query, bind)


def get_cs_stt_detail(oracle, rfile_name, sc):
    """
    Get CS stt detail
    :param          oracle:             DB
    :param          rfile_name:         RFILE_NAME
    :param          sc:                 STT_SNTC_SPRK_DCD
    :return:                            CS result detail
    """
    query = """
        SELECT
            DECODE(STT_SNTC_SPKR_DCD, 'A', 'ST0002', 'C', 'ST0001', 'M', 'ST0003'),
            STT_SNTC_LIN_NO,
            STT_SNTC_CONT,
            (SUBSTR(STT_SNTC_STTM, 1, 2) * 60 * 60 + SUBSTR(STT_SNTC_STTM, 3, 2) * 60 + SUBSTR(STT_SNTC_STTM, 5, 2)) * 100,
            (SUBSTR(STT_SNTC_ENDTM, 1, 2) * 60 * 60 + SUBSTR(STT_SNTC_ENDTM, 3, 2) * 60 + SUBSTR(STT_SNTC_ENDTM, 5, 2)) * 100,
            STT_SNTC_SPCH_SPED,
            SILENCE_YN,
            RGST_DTM,
            LST_CHG_DTM,
            RGST_PGM_ID,
            LST_CHG_PGM_ID,
            SILENCE_TIME
        FROM
            TB_CS_STT_RST
        WHERE 1=1
            AND RFILE_NAME = :1
            AND STT_SNTC_SPKR_DCD = :2
        ORDER BY
            STT_SNTC_LIN_NO
    """
    bind = (rfile_name, sc,)
    oracle.cursor.execute(query, bind)
    rows = oracle.cursor.fetchall()
    return rows


def get_cs_stt_result(oracle, rfile_name):
    """
    Get CS CS result
    :param          oracle:             DB
    :param          rfile_name:         RFILE_NAME
    :return:                            CS result
    """
    query = """
        SELECT
            MAX(RGST_DTM),
            MAX(LST_CHG_DTM),
            MAX(RGST_PGM_ID),
            MAX(LST_CHG_PGM_ID),
            DECODE(STT_SNTC_SPKR_DCD, 'A', 'ST0002', 'C', 'ST0001', 'M', 'ST0003')
        FROM
            TB_CS_STT_RST
        WHERE
            RFILE_NAME = :1
        GROUP BY
            STT_SNTC_SPKR_DCD
    """
    bind = (rfile_name,)
    oracle.cursor.execute(query, bind)
    rows = oracle.cursor.fetchall()
    if rows is bool:
        return list()
    if not rows:
        return list()
    return rows


def get_cs_meta_info(oracle, rfile_name):
    """
    Get CS meta information
    :param          oracle:             DB
    :param          rfile_name:         RFILE_NAME
    :return:                            CS meta information
    """
    query = """
        SELECT
            TO_DATE(SUBSTR(RFILE_NAME, 0, 14), 'YYYYMMDDHH24MISS'),
            'PC0001',
            RFILE_NAME,
            TRIM(CONCAT('CT000', CALL_TYPE)),
            NULL,
            NULL,
            CALL_START_TIME,
            CALL_END_TIME,
            EXTRACT(DAY FROM (CALL_END_TIME - CALL_START_TIME)) * 24*60*60 +
            EXTRACT(HOUR FROM (CALL_END_TIME - CALL_START_TIME)) * 60*60 +
            EXTRACT(MINUTE FROM (CALL_END_TIME - CALL_START_TIME)) * 60 +
            EXTRACT(SECOND FROM (CALL_END_TIME - CALL_START_TIME)),
            CTI_CALL_ID,
            RUSER_ID,
            RUSER_NAME,
            RUSER_NUMBER,
            CU_ID,
            CU_NAME,
            CU_NAME_HASH,
            CU_NUMBER,
            IN_CALL_NUMBER,
            BIZ_CD,
            CHN_TP,
            FILE_SPRT,
            REC_EXT,
            RGST_DTM,
            LST_CHG_DTM,
            RGST_PGM_ID,
            LST_CHG_PGM_ID,
            (
                SELECT
                    C.FULL_CODE
                FROM
                    GROUP_INFO_CS_USER_TB B
                INNER JOIN
                    CM_CD_DETAIL_TB C
                ON 
                    C.META_CODE = B.JOBCD
                    AND C.USE_YN = 'Y'
                WHERE 1=1
                    AND SUBSTR(B.USER_ID, 1, 7) LIKE CONCAT('%', RUSER_ID)
                    AND B.USERNAME = RUSER_NAME
            ) AS BUSINESS_DCD,
            (
                SELECT
                    C.FULL_CODE
                FROM
                    GROUP_INFO_CS_USER_TB B
                INNER JOIN
                    CM_CD_DETAIL_TB C
                ON 
                    C.META_CODE = B.USERGROUPCD
                    AND C.USE_YN = 'Y'
                WHERE 1=1
                    AND SUBSTR(B.USER_ID, 1, 7) LIKE CONCAT('%', RUSER_ID)
                    AND B.USERNAME = RUSER_NAME
            ) AS RUSER_USERGROUPCD,
            STT_SPCH_SPED_RX,
            STT_SPCH_SPED_TX
        FROM 
            TB_CS_STT_RCDG_INFO 
        WHERE 
            RFILE_NAME = :1
    """
    bind = (rfile_name,)
    oracle.cursor.execute(query, bind)
    rows = oracle.cursor.fetchone()
    if rows is bool:
        return False
    if not rows:
        return False
    return rows


def get_cs_list(oracle, cnt):
    """
    Get CS list
    :param          oracle:         DB
    :param          cnt:            Select count
    :return:                        Target CS list
    """
    query = """
        SELECT
            RFILE_NAME
        FROM
            (
                SELECT
                    DISTINCT RFILE_NAME,
                    MAX(LST_CHG_DTM) AS DT
                FROM 
                    (
                        SELECT
                            A.RFILE_NAME,
                            A.LST_CHG_DTM
                        FROM 
                            TB_CS_STT_RCDG_INFO A,
                            TB_CS_STT_RST B
                        WHERE 1=1 
                            AND A.RFILE_NAME = B.RFILE_NAME
                            AND A.TRANSFER_YN != 'Y'
                            AND B.LST_CHG_DTM <= TO_TIMESTAMP(TO_CHAR(SYSDATE, 'YYYYMMDDHH24MISS'), 'YYYYMMDDHH24MISS') - INTERVAL '5' MINUTE
                            AND ROWNUM < 1000000
                        ORDER BY B.LST_CHG_DTM
                    )
                GROUP BY
                    RFILE_NAME
                ORDER BY 
                    DT
            )
        WHERE ROWNUM <= :1
    """
    query = """
        SELECT
            RFILE_NAME
        FROM
            (
                SELECT
                    DISTINCT RFILE_NAME,
                    MAX(LST_CHG_DTM) AS DT
                FROM 
                    (
                        SELECT
                            A.RFILE_NAME,
                            A.LST_CHG_DTM
                        FROM 
                            TB_CS_STT_RCDG_INFO A,
                            TB_CS_STT_RST B
                        WHERE 1=1 
                            AND A.RFILE_NAME = B.RFILE_NAME
                            AND A.TRANSFER_YN != 'Y'
                            AND
                                (
                                    A.CALL_START_TIME
                                    BETWEEN
                                        TO_TIMESTAMP('20180502090000', 'YYYYMMDDHH24MISS')
                                    AND
                                        TO_TIMESTAMP('20180502110000', 'YYYYMMDDHH24MISS')
                                )
                            AND ROWNUM < 1000000
                        ORDER BY 
                            B.LST_CHG_DTM
                    )
                GROUP BY
                    RFILE_NAME
                ORDER BY 
                    DT
            )
        WHERE ROWNUM <= :1
    """
    bind = (cnt, )
    oracle.cursor.execute(query, bind)
    rows = oracle.cursor.fetchall()
    if rows is bool:
        return list()
    if not rows:
        return list()
    return rows


def main():
    """
    This is a program that collector process
    """
    try:
        db_collector = DbCollector()
        db_collector.run()
    except Exception:
        print traceback.format_exc()


if __name__ == '__main__':
    main()
