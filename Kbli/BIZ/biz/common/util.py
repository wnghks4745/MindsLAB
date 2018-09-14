#!/usr/bin/python
# -*- coding: utf-8 -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-05-07, modification: 0000-00-00"

###########
# imports #
###########
import traceback
import db_connection
from treelib import Tree


#########
# class #
#########
class ProcStatus(object):
    PS_COMPLETED = "PS0001"
    PS_WAITING = "PS0002"
    PS_PENDING = "PS0003"
    PS_INPROGRESS = "PS0004"
    PS_FAILED = "PS0005"
    PS_EMPTY = "PS0006"


#######
# def #
#######
def select_call_meta_data(file_name):
    """
    Select call meta data
    :param          file_name:      File name
    :return:                        Call meta data
    """
    oracle = db_connection.Oracle()
    sql = """
        SELECT
            CALL_ID,
            CALL_DATE,
            PROJECT_CODE,
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
            CREATOR_ID,
            UPDATOR_ID,
            BUSINESS_DCD
        FROM
            CM_CALL_META_TB
        WHERE
            FILE_NAME = :1
        ORDER BY 
            CALL_ID DESC
    """
    bind = (
        file_name,
    )
    oracle.cursor.execute(sql, bind)
    rows = oracle.cursor.fetchone()
    if rows is bool:
        return False
    if not rows:
        return False
    return rows


def insert_proc_result(oracle, logger, hdr):
    """
    Insert proc result
    :param          oracle:         Oracle
    :param          logger:         Logger
    :param          hdr:            Header
    """
    try:
        query = """
            MERGE INTO
                PL_PROC_STATUS_TB
            USING
                DUAL
            ON
            ( 1=1
                AND PIPELINE_ID = :1
                AND PIPELINE_EVENT_ID = :2
                AND ROUTER_ID = :3
            )
            WHEN MATCHED THEN
                UPDATE SET
                    PROC_STATUS_CODE = :4,
                    UPDATED_DTM = SYSDATE,
                    UPDATOR_ID = :5
            WHEN NOT MATCHED THEN
                INSERT
                (
                    PIPELINE_ID,
                    PIPELINE_EVENT_ID,
                    ROUTER_ID,
                    PROC_STATUS_CODE,
                    CREATED_DTM,
                    UPDATED_DTM,
                    CREATOR_ID,
                    UPDATOR_ID,
                    CALL_DATE
                )
                VALUES
                (
                    :6, :7, :8, :9,
                    SYSDATE, SYSDATE,
                    :10, :11, TO_DATE(:12, 'YYYY/MM/DD')
                )
        """
        call_date = '' if hdr.call_metadata.call_date == 'None' else hdr.call_metadata.call_date
        bind = (
            hdr.pipeline_id,
            hdr.pipeline_event_id,
            hdr.router_id,
            hdr.status_id,
            hdr.creator_id,
            hdr.pipeline_id,
            hdr.pipeline_event_id,
            hdr.router_id,
            hdr.status_id,
            hdr.creator_id,
            hdr.creator_id,
            call_date
        )
        oracle.cursor.execute(query, bind)
        oracle.conn.commit()
    except Exception:
        oracle.conn.rollback()
        logger.error(traceback.format_exc())


def create_pipeline_info():
    """
    Create pipe line info
    :return:            Pipe line information
    """
    oracle = db_connection.Oracle()
    try:
        # Tree: pipeline tree list, info: pipeline information, model: pipeline model, config: pipeline config
        pl_info = {'tree': dict(), 'info': dict(), 'model': dict(), 'config': dict()}
        # Create pipeline info map
        select_pipeline_query = """
            SELECT
                PIPELINE_ID,
                PIPELINE_NAME,
                PIPELINE_META
            FROM
                PL_PIPELINE_TB
            WHERE
                USE_YN = 'Y'
        """
        oracle.cursor.execute(select_pipeline_query)
        pipeline_rows = oracle.cursor.fetchall()
        # Insert pipeline information
        for pipeline_id, pipeline_name, pipeline_meta in pipeline_rows:
            pl = dict()
            pl['id'] = pipeline_id
            pl['name'] = pipeline_name
            pl['meta'] = pipeline_meta
            pl_info['info'][pipeline_id] = pl
        query = """
            SELECT
                pr.router_id AS router_id,
                pr.pipeline_id AS pipeline_id,
                pr.proc_id AS proc_id,
                pr.parent_id AS parent_id,
                pr.config_id AS config_id,
                pc.proc_name AS proc_name,
                pc.proc_meta AS proc_meta,
                conf.config_meta AS config_meta,
                mo.model_id AS model_id,
                mo.model_name AS model_name,
                mo.proc_type_code AS proc_type_code,
                mo.model_params AS model_params
            FROM
                PL_PIPELINE_ROUTER_TB pr
                    INNER JOIN PL_PROC_TB pc
                        ON ( 1=1
                            AND pr.proc_id = pc.proc_id
                        )
                    LEFT OUTER JOIN PL_PROC_CONFIG_TB conf
                        ON ( 1=1
                            AND pr.config_id = conf.config_id
                        )
                    LEFT OUTER JOIN PL_PROC_CONFIG_MODEL_REL_TB cmrel
                        ON ( 1=1
                            AND conf.config_id = cmrel.config_id
                            AND cmrel.id IN (
                                                SELECT
                                                    id
                                                FROM
                                                    (
                                                        SELECT
                                                            id
                                                        FROM
                                                            PL_PROC_CONFIG_MODEL_REL_TB sub_rel,
                                                            PL_PIPELINE_ROUTER_TB sub_pr
                                                                INNER JOIN PL_PROC_TB sub_pc
                                                                    ON ( 1=1
                                                                        AND sub_pr.proc_id = sub_pc.proc_id
                                                                    )
                                                                LEFT OUTER JOIN PL_PROC_CONFIG_TB sub_conf
                                                                    ON ( 1=1
                                                                        AND sub_pr.config_id = sub_conf.config_id
                                                                    )
                                                        WHERE 1=1
                                                            AND sub_rel.config_id = sub_conf.config_id
                                                        ORDER BY
                                                            ID ASC
                                                    )
                                            )
                        )
                    LEFT OUTER JOIN PL_MODEL_TB mo
                        ON ( 1=1
                            AND cmrel.model_id = mo.model_id
                        )
            ORDER BY
                pr.parent_id ASC,
                pr.router_id ASC
        """
        oracle.cursor.execute(query)
        rows = oracle.cursor.fetchall()
        for item in rows:
            router_id = item[0]
            pipeline_id = item[1]
            proc_id = item[2]
            parent_id = item[3]
            config_id = item[4]
            proc_name = item[5]
            proc_meta = item[6]
            config_meta = item[7]
            model_id = item[8]
            model_name = item[9]
            proc_type_code = item[10]
            model_params = item[11]
            if parent_id == 0:
                tree_obj = Tree()
                tree_obj.create_node(router_id, router_id, data=(
                    model_params, proc_id, proc_name, router_id, model_id, config_id, config_meta, proc_meta,
                    model_name, proc_type_code))
                pl_info['tree'][pipeline_id] = tree_obj
            else:
                pl_info['tree'][pipeline_id].create_node(
                    router_id, router_id, parent=parent_id, data=(
                        model_params, proc_id, proc_name, router_id, model_id,config_id, config_meta, proc_meta,
                        model_name, proc_type_code))
    except Exception:
        raise Exception(traceback.format_exc())
    return pl_info


def get_grpc_service_list(proc_type_code):
    """
    Get grpc service list
    :param          proc_type_code:         Proc type code
    :return:                                Remote list
    """
    remote_list = list()
    oracle = db_connection.Oracle()
    sql = """
        SELECT
            IP,
            PORT,
            PROCESS_COUNT
        FROM
            PL_SERVICE_SERVER_LIST_TB
        WHERE
            PROC_TYPE_CODE = :1
    """
    bind = (
        proc_type_code,
    )
    oracle.cursor.execute(sql, bind)
    rows = oracle.cursor.fetchall()
    for ip, port, process_count in rows:
        remote_list.append(("{0}:{1}".format(ip, port), process_count))
    oracle.disconnect()
    return remote_list


if __name__ == '__main__':
    pl_info = create_pipeline_info()
    for key, tree in pl_info['tree'].iteritems():
        tree.show()

