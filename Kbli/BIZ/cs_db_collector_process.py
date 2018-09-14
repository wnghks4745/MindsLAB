#!/usr/bin/python
# -*- coding:utf-8 -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-06-27, modification: 0000-00-00"

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
CREATOR_ID = 'BIZ_COL'


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
            cntc_user_info = kwargs.get('cntc_user_info')
            header = common_pb2.Header()
            header.ticket_id = kwargs.get('ticket_id')
            header.call_id = kwargs.get('call_id')
            header.pipeline_id = kwargs.get('pipeline_id')
            header.pipeline_event_id = kwargs.get('pipeline_event_id')
            header.router_id = kwargs.get('router_id')
            header.proc_id = kwargs.get('proc_id')
            header.status_id = util.ProcStatus.PS_COMPLETED
            header.creator_id = CREATOR_ID
            header.call_metadata.call_date = meta_info[0].strftime('%Y-%m-%d') if meta_info[0] else 'None'
            header.call_metadata.project_code = meta_info[1]
            header.call_metadata.file_name = meta_info[2]
            header.call_metadata.call_type_code = meta_info[3]
            header.call_metadata.record_key = meta_info[4] if meta_info[4] else 'None'
            header.call_metadata.start_time = str(meta_info[5])
            header.call_metadata.end_time = str(meta_info[6])
            header.call_metadata.duration = meta_info[7]
            header.call_metadata.ruser_id = meta_info[8] if meta_info[8] else 'None'
            header.call_metadata.ruser_name = meta_info[9] if meta_info[9] else 'None'
            header.call_metadata.mktn_id = str(meta_info[10]) if meta_info[10] else 'None'
            header.call_metadata.org_c = str(meta_info[11]) if meta_info[11] else 'None'
            header.call_metadata.list_id = str(meta_info[12]) if meta_info[12] else 'None'
            header.call_metadata.cu_id = str(meta_info[13]) if meta_info[13] else 'None'
            header.call_metadata.cu_name = meta_info[14] if meta_info[14] else 'None'
            header.call_metadata.poly_no = meta_info[15] if meta_info[15] else 'None'
            header.call_metadata.cont_date = meta_info[16] if meta_info[16] else 'None'
            header.call_metadata.speaker_code = kwargs.get('speaker_code')
            header.call_metadata.cntc_user_depart_c = cntc_user_info[0] if cntc_user_info[0] else 'None'
            header.call_metadata.cntc_user_depart_nm = cntc_user_info[1] if cntc_user_info[1] else 'None'
            header.call_metadata.cntc_user_part_c = cntc_user_info[2] if cntc_user_info[2] else 'None'
            header.call_metadata.cntc_user_part_nm = cntc_user_info[3] if cntc_user_info[3] else 'None'
            header.call_metadata.ivr_serv_nm = cntc_user_info[4] if cntc_user_info[4] else 'None'
            for cntc_cls_dict in kwargs.get('cntc_cls_double_dict').values():
                header.cntc_cls_list.add(
                    cntc_lcls_c=cntc_cls_dict['cntc_lcls_c'],
                    cntc_lcls_nm=cntc_cls_dict['cntc_lcls_nm'],
                    cntc_md_clas_c=cntc_cls_dict['cntc_md_clas_c'],
                    cntc_md_clas_nm=cntc_cls_dict['cntc_md_clas_nm']
                )
            return header
        except Exception:
            self.logger.error(traceback.format_exc())
            raise Exception(traceback.format_exc())

    def send_to_router(self, header, body):
        self.socket.send_multipart([header, body])

    def do_cs_job(self):
        oracle = db_connection.Oracle()
        # Select CS target list
        self.logger.info("Get CS list")
        cs_list = get_cs_list(oracle, self.select_cnt)
        self.logger.info("Done get CS list")
        for rec_id, rfile_name in cs_list:
            st = datetime.now()
            call_id = ''
            try:
                # Get call meta information
                self.logger.info("Get call meta information")
                meta_info, stt_spch_sped_rx, stt_spch_sped_tx, cntc_cls_double_dict, cntc_user_info = get_cs_meta_info(oracle, rfile_name, rec_id)
                self.logger.info("Done get call meta information")
                if not meta_info:
                    self.logger.error(
                        "Can't select CS meta information. [REC_ID = {0}, RFILE_NAME = {1}]".format(
                        rec_id, rfile_name))
                    continue
                # Create CALL_ID
                call_id = create_call_id(oracle, meta_info, cntc_user_info)
                # Get STT result
                stt_result = get_cs_stt_result(oracle, rfile_name, rec_id)
                # Create CALL_DATE
                call_date = meta_info[0].strftime('%Y-%m-%d') if meta_info[0] else ''
                for stt_data in stt_result:
                    # Create ticket id
                    ticket_id = create_ticket_id(oracle, call_date, call_id)
                    for pipeline_id, tree in self.tree_map.iteritems():
                        root = tree.get_node(tree.root)
                        if root.data[9] == 'PT0006':
                            if stt_data[4] == 'ST0001':
                                sc = 'C'
                            elif stt_data[4] == 'ST0002':
                                sc = 'S'
                            else:
                                sc = 'M'
                            speed = stt_spch_sped_rx if sc == 'C' else stt_spch_sped_tx
                            # Create pipeline_event_id
                            pipeline_event_id = create_pipeline_event_id(oracle, call_date, pipeline_id, ticket_id)
                            # Insert stt result
                            stt_result_id = insert_stt_result(
                                oracle=oracle,
                                call_date=call_date,
                                stt_data=stt_data,
                                call_id=call_id,
                                ticket_id=ticket_id,
                                pipeline_event_id=pipeline_event_id,
                                speed=speed
                            )
                            # Get stt detail
                            stt_detail_list = get_cs_stt_detail(oracle, rfile_name, sc, rec_id)
                            # Insert stt_detail_list
                            stt_result_detail = insert_stt_detail(
                                oracle, call_id, call_date, stt_result_id, stt_detail_list)
                            # Create message
                            hdr = self.create_common_header(
                                pipeline_id=pipeline_id,
                                router_id=root.identifier,
                                proc_id=root.data[1],
                                call_id=call_id,
                                ticket_id=ticket_id,
                                pipeline_event_id=pipeline_event_id,
                                meta_info=meta_info,
                                speaker_code=stt_data[4],
                                cntc_user_info=cntc_user_info,
                                cntc_cls_double_dict=cntc_cls_double_dict
                            )
                            hdr.stt_result_id = stt_result_id
                            stt_result = stt_pb2.SttResultDetail()
                            stt_result.stt_result_id = stt_result_id
                            for item in stt_result_detail:
                                stt_result.sentence.add(
                                    stt_result_detail_id=item[0],
                                    sequence=item[1],
                                    sentence=item[2],
                                    start_time=item[3],
                                    end_time=item[4]
                                )
                            self.send_to_router(hdr.SerializeToString(), stt_result.SerializeToString())
                oracle.conn.commit()
                # Update TRANSFER_YN status
                update_cs_meta_trans_status(oracle, rfile_name, 'Y', rec_id)
                self.logger.info(
                    "[SUCCESS] CALL_DATE = {0}, CALL_ID : {1}, REC_ID : {2}, RFILE_NAME : {3},"
                    " REQUIRED TIME = {4}".format(call_date, call_id, rec_id, rfile_name, str(datetime.now() - st)))
            except Exception:
                oracle.conn.rollback()
                # Update TRANSFER_YN status
                update_cs_meta_trans_status(oracle, rfile_name, 'F', rec_id)
                oracle.conn.commit()
                self.logger.error(traceback.format_exc())
                self.logger.error(
                    "[ERROR] CALL_ID : {0}, REC_ID : {1}, RFILE_NAME : {2}".format(call_id, rec_id, rfile_name))
                continue
            time.sleep(1)
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
            CALL_META
        SET
            TRANSFER_YN = :1
        WHERE 1=1
            AND DOCUMENT_ID = :2
            AND REC_ID = :3
    """
    bind = (status, rfile_name, rec_id)
    oracle.cursor.execute(query, bind)
    oracle.conn.commit()


def insert_stt_detail(oracle, call_id, call_date, stt_result_id, stt_detail_list):
    """
    Insert STT result detail
    :param          oracle:                 DB
    :param          call_id:                CALL_ID
    :param          call_date:              CALL_DATE
    :param          stt_result_id:          STT_RESULT_ID
    :param          stt_detail_list:        STT detail result list
    :return:                                STT detail result for router
    """
    sentence_id = 0
    rt_values = list()
    for stt_detail in stt_detail_list:
        sentence_id += 1
        my_seq = oracle.cursor.var(cx_Oracle.NUMBER)
        query = """
            INSERT INTO STT_RESULT_DETAIL_TB
            (
                CALL_ID,
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
                :1, TO_DATE(:2, 'YYYY/MM/DD'),
                :3, :4, :5, :6, :7, :8, :9,
                :10, :11, :12, :13, :14, :15
            ) 
            RETURNING STT_RESULT_DETAIL_ID INTO :16
        """
        bind = (
            call_id,
            call_date,
            stt_result_id,
            stt_detail[0],
            sentence_id,
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
        rt_values.append((stt_result_detail_id, sentence_id, stt_detail[2], stt_detail[3], stt_detail[4]))
    return rt_values


def get_cs_stt_detail(oracle, rfile_name, sc, rec_id):
    """
    Get CS stt detail
    :param          oracle:             DB
    :param          rfile_name:         RFILE_NAME
    :param          sc:                 STT_SNTC_SPKR_DCD
    :param          rec_id:             REC_ID
    :return:                            STT result detail
    """
    query = """
        SELECT
            DECODE(STT_SNTC_SPKR_DCD, 'S', 'ST0002', 'C', 'ST0001', 'M', 'ST0003'),
            STT_SNTC_LIN_NO,
            STT_SNTC_CONT,
            (SUBSTR(STT_SNTC_STTM, 1, 2) * 60 * 60 + SUBSTR(STT_SNTC_STTM, 3, 2) * 60 + SUBSTR(STT_SNTC_STTM, 5, 2)) * 100,
            (SUBSTR(STT_SNTC_ENDTM, 1, 2) * 60 * 60 + SUBSTR(STT_SNTC_ENDTM, 3, 2) * 60 + SUBSTR(STT_SNTC_ENDTM, 5, 2)) * 100,
            STT_SNTC_SPCH_SPED,
            CASE 
                WHEN STT_SILENCE > 5 THEN 'Y'
                ELSE 'N'
            END AS SILENCE_YN,
            RGST_DTM,
            LST_CHG_DTM,
            RGST_PGM_ID,
            LST_CHG_PGM_ID,
            STT_SILENCE
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


def insert_stt_result(**kwargs):
    """
    Insert STT result
    :return:                                    STT_RESULT_ID
    """
    oracle = kwargs.get('oracle')
    call_date = kwargs.get('call_date')
    stt_data = kwargs.get('stt_data')
    call_id = kwargs.get('call_id')
    ticket_id = kwargs.get('ticket_id')
    pipeline_event_id = kwargs.get('pipeline_event_id')
    speed = kwargs.get('speed')
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
            DECODE(STT_SNTC_SPKR_DCD, 'S', 'ST0002', 'C', 'ST0001', 'M', 'ST0003')
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


def select_cntc_user_info(oracle, record_key):
    """
    Select CNTC_USER_DEPART_C, CNTC_USER_DEPART_NM, CNTC_USER_PART_C, CNTC_USER_PART_NM
    :param          oracle:             DB
    :param          record_key:         record_key
    :return:                            Result rows
    """
    dev = ''
    # dev = '@D_KBLUAT_ZREAD'
    query = """
        SELECT
            CNTC_USER_DEPART_C,
            (
                SELECT
                    CODE_NM
                FROM
                    ZCS.TCL_CO_CODE{0}
                WHERE 1=1
                    AND DOMAIN_CD = 'SS08'
                    AND CODE = CNTC_USER_DEPART_C
            ) AS CNTC_USER_DEPART_NM,
            CNTC_USER_PART_C,
            (
                SELECT
                    CODE_NM
                FROM
                    ZCS.TCL_CO_CODE{0}
                WHERE 1=1
                    AND DOMAIN_CD = 'SS09'
                    AND CODE = CNTC_USER_PART_C
            ) AS CNTC_USER_PART_NM,
            (
                SELECT
                    CODE_NM
                FROM
                    ZCS.TCL_CO_CODE{0}
                WHERE 1=1
                    AND DOMAIN_CD = 'CS27'
                    AND CODE = IVR_SERV_NO
            ) AS IVR_SERV_NM
        FROM
            ZCS.TCL_CS_CNTC_HIST{0} A
        WHERE 1=1
            AND A.REC_NO = :1
            AND A.CNTC_STRT_DATE = :2
    """.format(dev)
    bind = (
        record_key,
        record_key[8:16],
    )
    oracle.cursor.execute(query, bind)
    result = oracle.cursor.fetchone()
    if result is bool:
        return '', '', '', '', ''
    if not result:
        return '', '', '', '', ''
    return result


def create_call_id(oracle, meta_info, cntc_user_info):
    """
    Create CALL_ID
    :param          oracle:             DB
    :param          meta_info:          Meta information
    :param          cntc_user_info:     CNTC_USER information
    :return:                            CALL_ID
    """
    my_seq = oracle.cursor.var(cx_Oracle.NUMBER)
    call_date = meta_info[0].strftime('%Y-%m-%d') if meta_info[0] else ''
    query = """
        INSERT INTO CM_CALL_META_TB
        (
            CALL_DATE,
            PROJECT_CODE,
            FILE_NAME,
            CALL_TYPE_CODE,
            RECORD_KEY,
            START_TIME,
            END_TIME,
            DURATION,
            RUSER_ID,
            RUSER_NAME,
            MKTN_ID,
            LIST_ID,
            CU_ID,
            CU_NAME,
            POLY_NO,
            CONT_DATE,
            CNTC_USER_DEPART_C,
            CNTC_USER_DEPART_NM,
            CNTC_USER_PART_C,
            CNTC_USER_PART_NM,
            IVR_SERV_NM,
            CREATED_DTM,
            UPDATED_DTM,
            CREATOR_ID,
            UPDATOR_ID
        )
        VALUES
        (
            TO_DATE(:1, 'YYYY/MM/DD'), :2, :3, :4,
            :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15,
            TO_DATE(:16, 'YYYY/MM/DD'),
            :17, :18, :19, :20, :21, SYSDATE, SYSDATE, :22, :23
        )
        RETURNING CALL_ID INTO :24
    """
    bind = (call_date,) + meta_info[1:11] + meta_info[12:17] + cntc_user_info + (CREATOR_ID, CREATOR_ID, my_seq,)
    oracle.cursor.execute(query, bind)
    call_id = int(my_seq.getvalue())
    return call_id


def get_cs_meta_info(oracle, rfile_name, rec_id):
    """
    Get CS meta information
    :param          oracle:             DB
    :param          rfile_name:         RFILE_NAME
    :param          rec_id:             REC_ID
    :return:                            CS meta information, Speech speed rx and tx
    """
    dev = ''
    # dev = '@D_KBLUAT_ZREAD'
    query = """
        SELECT
            A.DOCUMENT_DT,
            'PC0001',
            A.DOCUMENT_ID,
            TRIM(CONCAT('CT000', A.CALL_TYPE + 1)),
            A.REC_ID,
            A.START_DTM,
            A.END_DTM,
            A.DURATION,
            B.CNTC_USID,
            C.USER_NM,
            B.MKTN_ID,
            B.ORG_C,
            B.LIST_ID,
            B.CL_CID,
            D.CNM,
            B.POLY_NO,
            (
                SELECT DISTINCT
                    CONT_DATE
                FROM
                    ZCS.TCL_CS_CNTC_HIST_SUB{0}
                WHERE 1=1
                    AND B.ORG_C = ORG_C
                    AND B.CNTC_HIST_ID = CNTC_HIST_ID
                    AND B.CL_CID = CL_CID
                    AND CONT_DATE IS NOT NULL
                    AND ROWNUM = 1
            ) AS CONT_DATE,
            A.STT_SPCH_SPED_RX,
            A.STT_SPCH_SPED_TX,
            B.CNTC_LCLS_C,
            (
                SELECT
                    CODE_NM
                FROM
                    ZCS.TCL_CO_CODE{0}
                WHERE 1=1
                    AND DOMAIN_CD = 'CS02'
                    AND CODE = B.CNTC_LCLS_C
            ) AS CNTC_LCLS_NM,
            B.CNTC_MD_CLAS_C,
            (
                SELECT
                    CODE_NM
                FROM
                    ZCS.TCL_CO_CODE{0}
                WHERE 1=1
                    AND DOMAIN_CD = 'CS02'
                    AND CODE = B.CNTC_MD_CLAS_C
            ) AS CNTC_MD_CLAS_NM,
            B.CNTC_USER_DEPART_C,
            (
                SELECT
                    CODE_NM
                FROM
                    ZCS.TCL_CO_CODE{0}
                WHERE 1=1
                    AND DOMAIN_CD = 'SS08'
                    AND CODE = B.CNTC_USER_DEPART_C
            ) AS CNTC_USER_DEPART_NM,
            B.CNTC_USER_PART_C,
            (
                SELECT
                    CODE_NM
                FROM
                    ZCS.TCL_CO_CODE{0}
                WHERE 1=1
                    AND DOMAIN_CD = 'SS09'
                    AND CODE = B.CNTC_USER_PART_C
            ) AS CNTC_USER_PART_NM,
            (
                SELECT
                    CODE_NM
                FROM
                    ZCS.TCL_CO_CODE{0}
                WHERE 1=1
                    AND DOMAIN_CD = 'CS27'
                    AND CODE = B.IVR_SERV_NO
            ) AS IVR_SERV_NM
        FROM
            CALL_META A
                LEFT OUTER JOIN ZCS.TCL_CS_CNTC_HIST{0} B
                    ON ( 1=1
                        AND A.REC_ID = B.REC_NO
                        AND B.CNTC_STRT_DATE = :1
                    )
                LEFT OUTER JOIN ZCS.TCL_USER{0} C
                    ON ( 1=1
                        AND B.CNTC_USID = C.USER_ID
                    )
                LEFT OUTER JOIN ZCS.TCL_CS_BASE{0} D
                    ON ( 1=1
                        AND B.ORG_C = D.ORG_C
                        AND B.CL_CID = D.CL_CID
                    )
        WHERE 1=1
            AND A.DOCUMENT_ID = :2
            AND A.REC_ID = :3
    """.format(dev)
    bind = (rec_id[8:16], rfile_name, rec_id,)
    oracle.cursor.execute(query, bind)
    rows = oracle.cursor.fetchall()
    if rows is bool:
        return False, False, False, dict(), tuple()
    if not rows:
        return False, False, False, dict(), tuple()
    cntc_cls_double_dict = dict()
    for result in rows:
        key = '{0}_{1}_{2}_{3}'.format(result[19], result[20], result[21], result[22])
        cntc_cls_dict = {
            'cntc_lcls_c': result[19],
            'cntc_lcls_nm': result[20],
            'cntc_md_clas_c': result[21],
            'cntc_md_clas_nm': result[22]
        }
        if key not in cntc_cls_double_dict:
            cntc_cls_double_dict[key] = cntc_cls_dict
    return rows[0][:17], rows[0][17], rows[0][18], cntc_cls_double_dict, rows[0][23:28]


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
                            A.DOCUMENT_ID AS RFILE_NAME,
                            B.LST_CHG_DTM
                        FROM 
                            CALL_META A,
                            TB_CS_STT_RST B
                        WHERE 1=1 
                            AND A.STT_PRGST_CD = '05'
                            AND A.PROJECT_CD = 'CS'
                            AND A.DOCUMENT_ID = B.RFILE_NAME
                            AND A.REC_ID = B.REC_ID
                            AND A.TRANSFER_YN != 'Y'
                            AND A.CALL_DT > TO_DATE('2018-07-31', 'YYYY-MM-DD')
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
