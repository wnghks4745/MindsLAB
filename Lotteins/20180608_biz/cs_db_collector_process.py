#!/usr/bin/python
# -*- coding:utf-8 -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-05-21, modification: 0000-00-00"

###########
# imports #
###########
import os
import sys
import zmq
import time
import traceback
import cx_Oracle
from datetime import datetime
from biz.common import util, db_connection, logger
sys.path.append(os.path.join(os.getenv('MAUM_ROOT'), 'lib/python'))
from common.config import Config
from maum.biz.proc import stt_pb2
from maum.biz.common import common_pb2

###########
# options #
###########
reload(sys)
sys.setdefaultencoding('utf-8')


#############
# constants #
#############
CREATOR_ID = 'CS_COL'


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
        self.select_cnt = 10000

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
            header.creator_id = CREATOR_ID
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
            header.call_metadata.updator_id = meta_info[25] if meta_info[25] else 'None'
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
        # Extract holiday list
        holiday_list = select_holiday(oracle)
        call_id = ''
        rec_id = ''
        rfile_name = ''
        for item in cs_list:
            try:
                st = datetime.now()
                (rec_id, rfile_name,) = item
                # Get call meta information
                meta_info, stt_spch_sped_rx, stt_spch_sped_tx = get_cs_meta_info(oracle, rfile_name, rec_id)
                if not meta_info:
                    self.logger.error(
                        "Can't select CS meta information. [REC_ID = {0}, RFILE_NAME = {0}]".format(
                            rec_id, rfile_name))
                    continue
                call_date = meta_info[0].strftime('%Y-%m-%d') if meta_info[0] else ''
                start_time = meta_info[6]
                ruser_id = meta_info[10]
                # Extract TEAMCD
                team_cd = select_code_from_cm_call_meta_tb(oracle, ruser_id)
                meta_info += team_cd if team_cd else ('',)
                # Extract OFFICE_HOUR
                if call_date.replace("-", "") in holiday_list:
                    office_hour = 'OH0003' if 9 <= start_time.hour < 18 else 'OH0004'
                elif datetime.strptime(call_date, '%Y-%m-%d').weekday() in [5, 6]:
                    office_hour = 'OH0003' if 9 <= start_time.hour < 18 else 'OH0004'
                else:
                    office_hour = 'OH0001' if 9 <= start_time.hour < 18 else 'OH0002'
                meta_info += (office_hour, )
                # Create CALL_ID
                call_id = create_call_id(oracle, meta_info)
                # Get stt result
                stt_result = get_cs_stt_result(oracle, rfile_name, rec_id)
                for stt_data in stt_result:
                    # Create ticket id
                    ticket_id = create_ticket_id(oracle, call_date, call_id)
                    for pipeline_id, tree in self.tree_map.iteritems():
                        root = tree.get_node(tree.root)
                        if root.data[9] == 'PT0006':
                            # Create pipeline_event_id
                            pipeline_event_id = create_pipeline_event_id(oracle, call_date, pipeline_id, ticket_id)
                            if stt_data[4] == 'ST0001':
                                sc = 'C'
                            elif stt_data[4] == 'ST0002':
                                sc = 'A'
                            else:
                                sc = 'M'
                            speed = stt_spch_sped_rx if sc == 'C' else stt_spch_sped_tx
                            # Insert stt result
                            stt_result_id = insert_stt_result(
                                oracle, call_date, stt_data, call_id, ticket_id, pipeline_event_id, speed)
                            # Get stt detail
                            stt_detail_list = get_cs_stt_detail(oracle, rfile_name, sc, rec_id)
                            # Insert stt_detail_list
                            stt_result_detail = insert_stt_detail(oracle, call_date, stt_result_id, stt_detail_list)
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
                                    sentence=sentence[2],
                                    start_time=sentence[3],
                                    end_time=sentence[4]
                                )
                            self.send_to_router(hdr.SerializeToString(), stt_result.SerializeToString())
                # Update TRANSFER_YN status
                update_cs_meta_trans_status(oracle, rfile_name, 'Y', rec_id)
                oracle.conn.commit()
                self.logger.info(
                    "[SUCCESS] CALL_ID : {0}, REC_ID : {1}, RFILE_NAME : {2}, REQUIRED TIME = {3}".format(
                        call_id, rec_id, rfile_name, str(datetime.now() - st)))
            except Exception:
                oracle.conn.rollback()
                # Update TRANSFER_YN status
                update_cs_meta_trans_status(oracle, rfile_name, 'F', rec_id)
                oracle.conn.commit()
                self.logger.error(traceback.format_exc())
                self.logger.error(
                    "[ERROR] CALL_ID : {0}, REC_ID : {1}, RFILE_NAME : {2}".format(
                        call_id, rec_id, rfile_name))
                continue
        oracle.disconnect()

    def run(self):
        self.logger.info('[START] CS DB Collector Process started')
        # Prepare context and sockets
        self.socket.connect("tcp://localhost:{0}".format(self.conf.get('router.pull.port')))
        # Main loop
        flag = True
        while flag:
            # Process CS
            try:
                self.do_cs_job()
            except KeyboardInterrupt:
                self.logger.info('DB Collector stopped by Interrupt')
                flag = False
            except Exception:
                self.logger.error(traceback.format_exc())
        self.logger.info('[E N D] DB Collector Process Stopped...')


#######
# def #
#######
def select_holiday(oracle):
    """
    Get holiday list
    :param          oracle:             DB
    :return:                            Holiday List
    """
    query = """
        SELECT
            TO_CHAR(TO_DATE(HOLIDAY, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        FROM 
            HOLIDAY_CALENDAR_TB
    """
    oracle.cursor.execute(query, )
    result = oracle.cursor.fetchall()
    if result is bool:
        return list()
    if not result:
        return list()
    holiday_list = list()
    for item in result:
        holiday_list.append(item[0])
    return holiday_list


def insert_stt_detail(oracle, call_date, stt_result_id, stt_detail_list):
    """
    Insert STT result detail
    :param          oracle:                 DB
    :param          call_date:              CALL_DATE
    :param          stt_result_id:          STT_RESULT_ID
    :param          stt_detail_list:        STT detail result list
    :return:                                STT detail result for router
    """
    sc = 0
    rt_values = list()
    for stt_detail in stt_detail_list:
        sc += 1
        my_seq = oracle.cursor.var(cx_Oracle.NUMBER)
        query = """
            INSERT INTO STT_RESULT_DETAIL_TB
            (
                CALL_DATE,
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
                TO_DATE(:1, 'YYYY/MM/DD'),
                :2, :3, :4, :5, :6, :7, :8,
                :9, :10, :11, :12, :13, :14
            ) 
            RETURNING STT_RESULT_DETAIL_ID INTO :15
        """
        bind = (
            call_date,
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
        rt_values.append((stt_result_detail_id, sc, stt_detail[2], stt_detail[3], stt_detail[4]))
    return rt_values


def insert_stt_result(oracle, call_date, stt_data, call_id, ticket_id, pipeline_event_id, speed):
    """
    Insert STT result
    :param          oracle:                     DB
    :param          call_date:                  CALL_DATE
    :param          stt_data:                   STT result
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
            SPEED,
            CALL_DATE
        )
        VALUES
        (
            :1, :2, :3,
            :4, :5, :6,
            :7, :8, :9, 
            TO_DATE(:10, 'YYYY/MM/DD')
        ) 
        RETURNING STT_RESULT_ID INTO :11
    """
    bind = stt_data + (call_id, str(ticket_id), pipeline_event_id, speed, call_date, my_seq,)
    oracle.cursor.execute(query, bind)
    stt_result_id = my_seq.getvalue()
    stt_result_id = int(stt_result_id)
    return stt_result_id


def create_pipeline_event_id(oracle, call_date, pipeline_id, ticket_id):
    """
    Create PIPELINE_EVENT_ID
    :param          oracle:                 DB
    :param          call_date:              CALL_DATE
    :param          pipeline_id:            PIPELINE_ID
    :param          ticket_id:              TICKET_ID
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
            UPDATOR_ID,
            CALL_DATE
        )
        VALUES
        (
            :1, :2, SYSDATE,
            SYSDATE, :3, :4,
            TO_DATE(:5, 'YYYY/MM/DD')
        )
        RETURNING PIPELINE_EVENT_ID INTO :6 
    """
    bind = (
        pipeline_id,
        ticket_id,
        CREATOR_ID,
        CREATOR_ID,
        call_date,
        my_seq,
    )
    oracle.cursor.execute(query, bind)
    pipeline_event_id = my_seq.getvalue()
    pipeline_event_id = int(pipeline_event_id)
    return pipeline_event_id


def create_ticket_id(oracle, call_date, call_id):
    """
    Create TICKET_ID
    :param          oracle:             DB
    :param          call_date:          CALL_DATE
    :param          call_id:            CALL_ID
    :return:                            TICKET_ID
    """
    my_seq = oracle.cursor.var(cx_Oracle.NUMBER)
    query = """
        INSERT INTO CM_TICKET_TB
        (
            CALL_DATE,
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
            TO_DATE(:1, 'YYYY/MM/DD'),
            :2, 0, 0, NULL, NULL,
            SYSDATE, SYSDATE, :3, :4
        )
        RETURNING TICKET_ID INTO :5
    """
    bind = (
        call_date,
        call_id,
        CREATOR_ID,
        CREATOR_ID,
        my_seq,
    )
    oracle.cursor.execute(query, bind)
    ticket_id = my_seq.getvalue()
    ticket_id = int(ticket_id)
    return ticket_id


def select_code_from_cm_call_meta_tb(oracle, ruser_id):
    """
    Get CS meta information
    :param          oracle:             DB
    :param          ruser_id:           RUSER_ID
    :return:                           TEAMCD
    """
    query = """
        SELECT
            TEAMCD
        FROM 
            GROUP_INFO_CS_USER_TB 
        WHERE 1=1
            AND USER_ID LIKE CONCAT(SUBSTR(:1, 1, 7), '%')
    """
    bind = (ruser_id, )
    oracle.cursor.execute(query, bind)
    result = oracle.cursor.fetchone()
    if result is bool:
        return False
    if not result:
        return False
    return result


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
            RUSER_USERGROUPCD,
            POLI_NO,
            TEAMCD,
            OFFICE_HOUR
        )
        VALUES
        (
            TO_DATE(:1, 'YYYY/MM/DD'),
            :2, :3, :4, :5, :6, :7, :8,
            :9, :10, :11, :12, :13, :14,
            :15, :16, :17, :18, :19, :20,
            :21, :22, :23, :24, :25, :26,
            :27, :28, :29, :30, :31
        )
        RETURNING CALL_ID INTO :32
    """
    call_date = meta_info[0].strftime('%Y-%m-%d') if meta_info[0] else ''
    bind = (call_date,) + meta_info[1:] + (my_seq,)
    oracle.cursor.execute(query, bind)
    call_id = int(my_seq.getvalue())
    return call_id


def update_cs_meta_trans_status(oracle, rfile_name, status, rec_id):
    """
    Update CS meta TRANSFER_YN status
    :param      oracle:             DB
    :param      rfile_name:         RFILE_NAME
    :param      status:             Status
    :param      rec_id:             REC_ID
    """
    query = """
        UPDATE
            TB_CS_STT_RCDG_INFO
        SET
            TRANSFER_YN = :1
        WHERE 1=1
            AND RFILE_NAME = :2
            AND REC_ID = :3
    """
    bind = (status, rfile_name, rec_id)
    oracle.cursor.execute(query, bind)


def get_cs_stt_detail(oracle, rfile_name, sc, rec_id):
    """
    Get CS stt detail
    :param          oracle:             DB
    :param          rfile_name:         RFILE_NAME
    :param          sc:                 STT_SNTC_SPRK_DCD
    :param          rec_id:             REC_ID
    :return:                            STT result detail
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
            AND REC_ID = :3
        ORDER BY
            STT_SNTC_LIN_NO
    """
    bind = (
        rfile_name,
        sc,
        rec_id,
    )
    oracle.cursor.execute(query, bind)
    rows = oracle.cursor.fetchall()
    return rows


def get_cs_stt_result(oracle, rfile_name, rec_id):
    """
    Get CS STT result
    :param          oracle:             DB
    :param          rfile_name:         RFILE_NAME
    :param          rec_id:             REC_ID
    :return:                            STT result
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
        WHERE 1=1
            AND RFILE_NAME = :1
            AND REC_ID = :2
        GROUP BY
            STT_SNTC_SPKR_DCD
    """
    bind = (
        rfile_name,
        rec_id,
    )
    oracle.cursor.execute(query, bind)
    rows = oracle.cursor.fetchall()
    if rows is bool:
        return list()
    if not rows:
        return list()
    return rows


def get_cs_meta_info(oracle, rfile_name, rec_id):
    """
    Get CS meta information
    :param          oracle:             DB
    :param          rfile_name:         RFILE_NAME
    :param          rec_id:             REC_ID
    :return:                            CS meta information, Speech speed rx and tx
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
                    AND B.USER_ID LIKE CONCAT(SUBSTR(RUSER_ID, 1, 7), '%')
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
                    AND B.USER_ID LIKE CONCAT(SUBSTR(RUSER_ID, 1, 7), '%')
            ) AS RUSER_USERGROUPCD,
            POLI_NO,
            STT_SPCH_SPED_RX,
            STT_SPCH_SPED_TX
        FROM 
            TB_CS_STT_RCDG_INFO 
        WHERE 1=1
            AND RFILE_NAME = :1
            AND REC_ID = :2
    """
    bind = (rfile_name, rec_id,)
    oracle.cursor.execute(query, bind)
    rows = oracle.cursor.fetchone()
    if rows is bool:
        return False, False, False
    if not rows:
        return False, False, False
    return rows[:29], rows[29], rows[30]


def get_cs_list(oracle, cnt):
    """
    Get CS list
    :param          oracle:         DB
    :param          cnt:            Select count
    :return:                        Target CS list
    """
    query = """
        SELECT
            REC_ID,
            RFILE_NAME
        FROM
            (
                SELECT DISTINCT
                    REC_ID, 
                    RFILE_NAME,
                    MAX(LST_CHG_DTM) AS DT
                FROM 
                    (
                        SELECT
                            A.REC_ID,
                            A.RFILE_NAME,
                            A.LST_CHG_DTM
                        FROM 
                            TB_CS_STT_RCDG_INFO A,
                            TB_CS_STT_RST B
                        WHERE 1=1 
                            AND A.RFILE_NAME = B.RFILE_NAME
                            AND A.TRANSFER_YN != 'Y'
                            AND A.CS_STTA_PRGST_CD = '13'
                            AND B.LST_CHG_DTM <= TO_TIMESTAMP(TO_CHAR(SYSDATE, 'YYYYMMDDHH24MISS'), 'YYYYMMDDHH24MISS') - INTERVAL '5' MINUTE
                            AND ROWNUM < 1000000
                        ORDER BY B.LST_CHG_DTM
                    )
                GROUP BY
                    REC_ID,
                    RFILE_NAME
                ORDER BY 
                    DT
            )
        WHERE ROWNUM <= :1
    """
    query = """
        SELECT
            REC_ID,
            RFILE_NAME
        FROM
            (
                SELECT DISTINCT
                    REC_ID, 
                    RFILE_NAME,
                    MAX(LST_CHG_DTM) AS DT
                FROM 
                    (
                        SELECT
                            A.REC_ID,
                            A.RFILE_NAME,
                            A.LST_CHG_DTM
                        FROM 
                            TB_CS_STT_RCDG_INFO A,
                            TB_CS_STT_RST B
                        WHERE 1=1 
                            AND A.RUSER_ID != 'None'
                            AND A.RFILE_NAME = B.RFILE_NAME
                            AND A.TRANSFER_YN = 'N'
                            AND A.CS_STTA_PRGST_CD = '13'
                            AND A.CALL_START_TIME >= TO_TIMESTAMP('20180530000000', 'YYYYMMDDHH24MISS')
                            AND ROWNUM < 1000000
                        ORDER BY 
                            B.LST_CHG_DTM
                    )
                GROUP BY
                    REC_ID,
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
