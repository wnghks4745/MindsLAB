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
import json
import time
import traceback
import cx_Oracle
from datetime import datetime, timedelta
from lib.iLogger import set_logger_period_of_time
from cfg.config import CONFIG, ORACLE_DB_CONFIG
sys.path.append(os.path.join(os.getenv('MAUM_ROOT'), "lib/python"))
import maum.brain.hmd.hmd_pb2 as hmd_pb2


###########
# options #
###########
reload(sys)
sys.setdefaultencoding("utf-8")

#############
# constants #
#############
ST = ""
DT = ""

#########
# class #
#########
class Oracle(object):
    def __init__(self, logger):
        self.logger = logger
        self.dsn_tns = cx_Oracle.makedsn(
            ORACLE_DB_CONFIG['host'],
            ORACLE_DB_CONFIG['port'],
            sid=ORACLE_DB_CONFIG['sid']
        )
        self.conn = cx_Oracle.connect(
            ORACLE_DB_CONFIG['user'],
            ORACLE_DB_CONFIG['passwd'],
            self.dsn_tns
        )
        self.cursor = self.conn.cursor()

    def disconnect(self):
        try:
            self.cursor.close()
            self.conn.close()
        except Exception:
            raise Exception(traceback.format_exc())

    def select_perfect_sales_call_list(self, call_date):
        query = """
            SELECT
                PROCESS_ID,
                CALL_ID,
                TICKET_ID,
                RUSER_ID,
                CU_ID,
                CU_NAME_HASH,
                FILE_NAME
            FROM
                CS_PERFECT_SALES_CALL_LIST_TB
            WHERE 1=1
                AND PROCESSED_YN = 'N'
                AND CALL_DATE = TO_DATE(:1, 'YYYY-MM-DD')
        """
        bind = (
            call_date,
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchall()
        if result is bool:
            return False
        if not result:
            return False
        return result

    def select_outbound_monitoring_task_tb(self, call_date, ruser_id, cu_id, cu_name_hash):
        query = """
            SELECT
                CODE
            FROM
                OUTBOUND_MONITORING_TASK_TB
            WHERE 1=1
                AND TASK_DATE = TO_DATE(:1, 'YYYY-MM-DD')
                AND RUSER_ID = :2
                AND CU_ID = :3
                AND CU_NAME_HASH = :4
        """
        bind = (
            call_date,
            ruser_id,
            cu_id,
            cu_name_hash,
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchone()
        if result is bool:
            return False
        if not result:
            return False
        return result[0]

    def select_full_code(self, meta_code):
        query = """
            SELECT
                FULL_CODE
            FROM
                CM_CD_DETAIL_TB
            WHERE 1=1
                AND META_CODE = :1
        """
        bind = (
            meta_code,
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchone()
        if result is bool:
            return False
        if not result:
            return False
        return result[0]

    def select_json_data(self, ticket_id):
        query = """
            SELECT
                NLP_RESULT_ID,
                STT_RESULT_DETAIL_ID,
                NLP_RESULT_JSON
            FROM
                TA_NLP_RESULT_TB
            WHERE 1=1
                AND TICKET_ID = :1
        """
        bind = (
            ticket_id,
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchall()
        if result is bool:
            return False
        if not result:
            return False
        return result

    def select_sentence(self, stt_result_detail_id):
        query = """
            SELECT
                SENTENCE
            FROM
                STT_RESULT_DETAIL_TB
            WHERE 1=1
                AND STT_RESULT_DETAIL_ID = :1
        """
        bind = (
            stt_result_detail_id,
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchone()
        if result is bool:
            return False
        if not result:
            return False
        return result[0]

    def select_meta_info(self, call_id, call_date):
        query = """
            SELECT
                PROJECT_CODE,
                BUSINESS_DCD
            FROM
                CM_CALL_META_TB
            WHERE 1=1
                AND CALL_ID = :1
                AND CALL_DATE = TO_DATE(:2, 'YYYY/MM/DD')
        """
        bind = (
            call_id,
            call_date,
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchone()
        if result is bool:
            return False
        if not result:
            return False
        return result[0]

    def insert_perfect_sales_hmd_result(self, hmd_result_list, call_id, call_date, business_dcd, project_code, full_code):
        overlap_check_dict = dict()
        for hmd_result, nlp_result_id in hmd_result_list:
            for cls in hmd_result.keys():
                key = "{0}_{1}".format(cls, nlp_result_id)
                if not key in overlap_check_dict:
                    overlap_check_dict[key] = 1
                else:
                    continue
                query = """
                INSERT INTO CS_PERFECT_SALES_TB
                (
                    CALL_ID,
                    CATEGORY,
                    CALL_DATE,
                    BUSINESS_DCD,
                    PROJECT_CODE,
                    NLP_RESULT_ID,
                    CREATED_DTM,
                    UPDATED_DTM,
                    CREATOR_ID,
                    UPDATOR_ID,
                    OB_MONITORING_TYPE
                )
                VALUES
                (
                    :1, :2, :3, TO_DATE(:4, 'YYYY/MM/DD'),
                    :5, :6, :7, SYSDATE, SYSDATE, :8, :9
                )
                """
                bind = (
                    call_id,
                    cls,
                    call_date,
                    business_dcd,
                    project_code,
                    nlp_result_id,
                    'CS_BATCH',
                    'CS_BATCH',
                    full_code,
                )
                self.cursor.execute(query, bind)
            self.conn.commit()

    def update_perfect_sales_processed_yn(self, process_id):
        query = """
            UPDATE
                CS_PERFECT_SALES_CALL_LIST_TB
            SET
                PROCESSED_YN = 'Y',
                UPDATED_DTM = SYSDATE
            WHERE
                PROCESS_ID = :1
        """
        bind = (
            process_id,
        )
        self.cursor.execute(query, bind)
        self.conn.commit()


#######
# def #
#######
def elapsed_time(sdate):
    """
    elapsed time
    :param          sdate:          date object
    :return                         Required time (type : datetime)
    """
    end_time = datetime.now()
    if not sdate or len(sdate) < 14:
        return 0, 0, 0, 0
    start_time = datetime(int(sdate[:4]), int(sdate[4:6]), int(sdate[6:8]),
                          int(sdate[8:10]), int(sdate[10:12]), int(sdate[12:14]))
    required_time = end_time - start_time
    return required_time


def connect_db(logger, db):
    """
    Connect database
    :param          logger:         Logger
    :param          db:             Database
    :return                         SQL Object
    """
    # Connect DB
    logger.debug('Connect {0} DB ...'.format(db))
    sql = False
    for cnt in range(1, 4):
        try:
            if db == 'Oracle':
                os.environ["NLS_LANG"] = ".AL32UTF8"
                sql = Oracle(logger)
            elif db == 'MsSQL':
                sql = MSSQL(logger)
            else:
                logger.error("Unknown DB [{0}]".format(db))
                return False
            logger.debug("Success connect {0} DB ...".format(db))
            break
        except Exception:
            exc_info = traceback.format_exc()
            logger.error(exc_info)
            if cnt < 3:
                logger.error("Fail connect {0} DB, retrying count = {1}".format(db, cnt))
            time.sleep(10)
            continue
    if not sql:
        return False
    return sql


def find_hmd(vec_key, sent, tmp_line, vec_space):
    """
    Find hmd
    :param      vec_key:
    :param      sent:
    :param      tmp_line:
    :param      vec_space:
    :return:
    """
    pos = 0
    not_line = tmp_line
    len_line = len(tmp_line)
    for i in range(len(vec_key)):
        if len(vec_key[i]) == 0:
            continue
        b_pos = True
        b_sub = False
        b_neg = False
        k_str = ""
        key_pos = 0
        end_pos = len_line
        # Searching Special Command
        w_loc = -1
        while True:
            w_loc += 1
            if w_loc == len(vec_key[i]):
                return not_line, False
            if vec_key[i][w_loc] == '!':
                if b_pos:
                    b_pos = False
                else:
                    b_neg = True
            elif vec_key[i][w_loc] == '@':
                key_pos = pos
            elif vec_key[i][w_loc] == '+':
                w_loc += 1
                try:
                    plus_num = int(vec_key[i][w_loc])
                    key_pos, end_pos = find_loc(vec_space, pos, plus_num, len_line)
                except ValueError:
                    print 'ERR: + next number Check Dictionary' + vec_key[i]
                    return not_line, False
            elif vec_key[i][w_loc] == '%':
                b_sub = True
            elif vec_key[i][w_loc] == '#':
                b_ne = True
            else:
                k_str = vec_key[i][w_loc:]
                break
        # Searching lemma
        t_pos = 0
        if b_pos:
            if b_sub:
                pos = tmp_line[key_pos:end_pos].find(k_str)
            else:
                pos = tmp_line[key_pos:end_pos].find(' ' + k_str + ' ')
            if pos != -1:
                pos = pos + key_pos
        else:
            t_pos = key_pos
            if b_neg:
                pos = sent.find(k_str)
            elif b_sub:
                pos = not_line[key_pos:end_pos].find(k_str)
            else:
                pos = not_line[key_pos:end_pos].find(' ' + k_str + ' ')
        # Checking Result
        if b_pos:
            if pos > -1:
                if b_sub:
                    tmp_line = tmp_line[:key_pos] + tmp_line[key_pos:end_pos].replace(
                        k_str, '_' * len(k_str)) + tmp_line[end_pos:]
                else:
                    tmp_line = tmp_line[:key_pos] + tmp_line[key_pos:end_pos].replace(
                        ' ' + k_str + ' ', ' ' + '_' * len(k_str) + ' ') + tmp_line[end_pos:]
            else:
                return not_line, False
        else:
            if pos > -1:
                return not_line, False
            else:
                pos = t_pos
    return tmp_line, True


def vec_word_combine(tmp_result, output, strs_list, ws, level):
    """
    Vec word combine
    :param      tmp_result:     Temp result
    :param      output:         Output
    :param      strs_list:      Strs list
    :param      ws:             Ws
    :param      level:          Level
    :return:                    Temp result
    """
    if level == len(strs_list):
        tmp_result.append(output + ws)
    elif level == 0:
        for i in range(len(strs_list[level])):
            tmp = output + strs_list[level][i]
            vec_word_combine(tmp_result, tmp, strs_list, ws, level + 1)
    else:
        for i in range(len(strs_list[level])):
            if output[-1] == '@':
                tmp = output[:-1] + '$@' + strs_list[level][i]
            elif output[-1] == '%':
                tmp = output[:-1] + '$%' + strs_list[level][i]
            elif output[-2] == '+' and ('0' <= output[-1] <= '9'):
                tmp = output[:-1] + '$+' + output[-1] + strs_list[level][i]
            elif output[-1] == '#':
                tmp = output[:-1] + '$#' + strs_list[level][i]
            else:
                tmp = output + '$' + strs_list[level][i]
            vec_word_combine(tmp_result, tmp, strs_list, ws, level + 1)
    return tmp_result


def split_input(detect_keyword):
    """
    Split input
    :param      detect_keyword:     Detect keyword
    :return:                        Detect keyword list
    """
    detect_keyword_list = list()
    cnt = 0
    tmp = ''
    for idx in range(len(detect_keyword)):
        if detect_keyword[idx] == '(':
            cnt = 1
        elif detect_keyword[idx] == ')' and len(tmp) != 0:
            detect_keyword_list.append(tmp)
            tmp = ''
            cnt = 0
        elif cnt == 1:
            tmp += detect_keyword[idx]
    return detect_keyword_list


def load_model(model_path):
    """
    Load Model
    :param      model_path:     Model Path
    :return:                    Model Value
    """
    if not model_path.endswith('.hmdmodel'):
        raise Exception('model extension is not .hmdmodel : {0}'.format(model_path))
    try:
        in_file = open(model_path, 'rb')
        hm = hmd_pb2.HmdModel()
        hm.ParseFromString(in_file.read())
        in_file.close()
        return hm
    except Exception:
        raise Exception(traceback.format_exc())


def execute_hmd(document, sentence, model_name):
    """
    HMD
    :param      document:           document
    :param      sentence:           Sentence
    :param      model_name:         Model Name
    :return
    """
    hmd_dict = dict()
    model_path = '{0}/trained/hmd/{1}__0.hmdmodel'.format(os.getenv('MAUM_ROOT'), model_name)
    if not os.path.exists(model_path):
        raise Exception('model is not exists : {0}'.format(model_path))
    model_value = load_model(model_path)
    output_list = list()
    detect_category_dict = dict()
    for rules in model_value.rules:
        strs_list = list()
        category = rules.categories[0]
        dtc_cont = rules.rule
        detect_keyword_list = split_input(dtc_cont)
        for idx in range(len(detect_keyword_list)):
            detect_keyword = detect_keyword_list[idx].split("|")
            strs_list.append(detect_keyword)
        ws = ''
        output = ''
        tmp_result = []
        output += '{0}\t'.format(category)
        output_list += vec_word_combine(tmp_result, output, strs_list, ws, 0)
    for item in output_list:
        item = item.strip()
        if len(item) < 1 or item[0] == '#':
            continue
        item_list = item.split("\t")
        t_loc = len(item_list) - 1
        cate = ''
        for idx in range(t_loc):
            if idx != 0:
                cate += '_'
            cate += item_list[idx]
            if item_list[t_loc] not in hmd_dict:
                hmd_dict[item_list[t_loc]] = [[cate]]
            else:
                hmd_dict[item_list[t_loc]].append([cate])
    json_data = json.loads(document)
    word_list = list()
    for sentence in json_data['sentences']:
        for words in sentence['words']:
            tagged_text = words['tagged_text']
            tagged_text_list = tagged_text.split()
            for tagged_word in tagged_text_list:
                word = tagged_word.split("/")[0]
                word_list.append(word)
    nlp_sent = " ".join(word_list)
    s_tmp = " {0} ".format(nlp_sent)
    vec_space = list()
    vec_ne = list()
    rmv_ne = list()
    cnt = 0
    flag = False
    for idx in range(len(s_tmp)):
        if s_tmp[idx] == ' ':
            vec_space.append(cnt)
        elif (idx + 1 != len(s_tmp)) and s_tmp[idx:idx + 2] == '__':
            if flag:
                t_word = s_tmp[idx + 2:s_tmp[idx + 2:].find(' ') + idx + 2]
                tmp_ne.append(cnt)
                tmp_ne.append(t_word)
                vec_ne.append(tmp_ne)
                cnt = cnt - 2 - len(t_word)
                r_tmp_ne.append(idx)
                r_tmp_ne.append(idx + 2 + len(t_word))
                rmv_ne.append(r_tmp_ne)
                flag = False
            else:
                tmp_ne = list()
                tmp_ne.append(cnt)
                r_tmp_ne = list()
                r_tmp_ne.append(idx)
                flag = True
                cnt -= 2
        cnt += 1
    rmv_ne.reverse()
    for ne in rmv_ne:
        s_tmp = s_tmp[:ne[0]] + s_tmp[ne[0] + 2:ne[1]] + s_tmp[ne[2]:]
    b_check = False
    for key in hmd_dict.keys():
        tmp_line = s_tmp
        vec_key = key.split('$')
        tmp, b_print = find_hmd(vec_key, sentence, tmp_line, vec_space)
        if b_print:
            b_check = True
            for item in hmd_dict[key]:
                if item[0] not in detect_category_dict:
                    detect_category_dict[item[0]] = [key]
                else:
                    detect_category_dict[item[0]].append(key)
    return detect_category_dict


def load_meta_info_and_execute_hmd(oracle, call_date, call_id, process_id, result, model_params, full_code):
    """
    Load meta information and execute HMD
    :param          oracle:                 DB
    :param          call_date:              CALL_DATE
    :param          call_id:                CALL_ID
    :param          process_id:             PROCESS_ID
    :param          result:                 Result
    :param          model_params:           HMD model name
    :param          full_code:              Full code
    """
    hmd_result_list = list()
    for data in result:
        nlp_result_id = data[0]
        stt_result_detail_id = data[1]
        json_data = data[2]
        document = json_data.read()
        sentence = oracle.select_sentence(stt_result_detail_id)
        hmd_result = execute_hmd(document, sentence, model_params)
        if len(hmd_result) > 0:
            hmd_result_list.append((hmd_result, nlp_result_id))
        print hmd_result
        del json_data
        del document
        del data
    del result
    project_code, business_dcd = select_meta_info(self, call_id, call_date)
    oracle.insert_perfect_sales_hmd_result(hmd_result_list, call_id, call_date, business_dcd, project_code, full_code)
    oracle.update_perfect_sales_processed_yn(process_id)


def processing(target_call_date):
    """
    Processing
    :param          target_call_date:       Target call date
    """
    # Add logging
    logger_args = {
        'base_path': CONFIG['log_dir_path'],
        'log_file_name': CONFIG['log_name'],
        'log_level': CONFIG['log_level']
    }
    logger = set_logger_period_of_time(logger_args)
    # Connect db
    try:
        oracle = connect_db(logger, 'Oracle')
        if not oracle:
            print "---------- Can't connect db ----------"
            logger.error("---------- Can't connect db ----------")
            for handler in logger.handlers:
                handler.close()
                logger.removeHandler(handler)
            sys.exit(1)
    except Exception:
        exc_info = traceback.format_exc()
        print exc_info
        print "---------- Can't connect db ----------"
        logger.error(exc_info)
        logger.error("---------- Can't connect db ----------")
        for handler in logger.handlers:
            handler.close()
            logger.removeHandler(handler)
        sys.exit(1)
    try:
        call_date = "{0}-{1}-{2}".format(target_call_date[:4], target_call_date[4:6], target_call_date[6:])
        logger.info("Select CS perfect sales call list. [Target date -> {0}]".format(call_date))
        target_results = oracle.select_perfect_sales_call_list(call_date)
        if target_results:
            for target_result in target_results:
                process_id = target_result[0]
                call_id = target_result[1]
                ticket_id = target_result[2]
                ruser_id = target_result[3]
                cu_id = target_result[4]
                cu_name_hash = target_result[5]
                file_name = target_result[6]
                logger.info("PROCESS_ID = {0}, CALL_ID = {1}, TICKET_ID = {2}, RUSER_ID = {3}, CU_ID = {4},"
                            " CU_NAME_HASH = {5}, FILE_NAME = {6}".format(
                    process_id, call_id, ticket_id, ruser_id, cu_id, cu_name_hash, file_name))
                meta_code = oracle.select_outbound_monitoring_task_tb(call_date, ruser_id, cu_id, cu_name_hash)
                if meta_code:
                    full_code = oracle.select_full_code(meta_code)
                    result = oracle.select_json_data(ticket_id)
                    # 완전 판매
                    if full_code == 'OM0010':
                        logger.info("Start perfect_sales_hmd")
                        model_params = 'perfect_sales_hmd'
                        load_meta_info_and_execute_hmd(
                            oracle, call_date, call_id, process_id, result, model_params, full_code)
                    # (임) 해피카모니터링
                    elif full_code == 'OM0002':
                        logger.info("Start OM_happycar_emp_hmd")
                        model_params = 'OM_happycar_emp_hmd'
                        load_meta_info_and_execute_hmd(
                            oracle, call_date, call_id, process_id, result, model_params, full_code)
                    else:
                        logger.error("Not define HMD [full_code = {0}".format(full_code))
                else:
                    logger.error("Can't select meta_code [CALL_DATE = {0}, RUSER_ID = {1}. CU_ID = {2},"
                                 " CU_NAME_HASH = {3}".format(call_date, ruser_id, cu_id, cu_name_hash))
        else:
            logger.info("No CS perfect sales call")
    except Exception:
        exc_info = traceback.format_exc()
        print exc_info
        print "---------- ERROR ----------"
        logger.error(exc_info)
        logger.error("---------- ERROR ----------")
        oracle.disconnect()
        for handler in logger.handlers:
            handler.close()
            logger.removeHandler(handler)
        sys.exit(1)
    logger.info("END.. Start time = {0}, The time required = {1}".format(ST, elapsed_time(DT)))
    logger.info("-" * 100)
    oracle.disconnect()
    for handler in logger.handlers:
        handler.close()
        logger.removeHandler(handler)


########
# main #
########
def main(target_call_date):
    """
    This is a program that
    :param          target_call_date:        Target call date
    """
    global ST
    global DT
    ts = time.time()
    ST = datetime.fromtimestamp(ts).strftime('%Y-%m-%d-%H:%M.%S')
    DT = datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S')
    try:
        processing(target_call_date)
    except Exception:
        exc_info = traceback.format_exc()
        print(exc_info)


if __name__ == '__main__':
    if len(sys.argv) == 2:
        if len(sys.argv[1]) != 8:
            print "usage : python {0} [YYYYMMDD]".format(sys.argv[0])
            print "ex) python {0} 20180416".format(sys.argv[0])
            sys.exit(1)
        else:
            try:
                int(sys.argv[1])
            except Exception:
                print "usage : python {0} [YYYYMMDD]".format(sys.argv[0])
                print "ex) python {0} 20180416".format(sys.argv[0])
                sys.exit(1)
            main(sys.argv[1])
    elif len(sys.argv) == 1:
        main((datetime.fromtimestamp(time.time()) - timedelta(days=0)).strftime('%Y%m%d'))
    else:
        print "usage : python {0} [YYYYMMDD]".format(sys.argv[0])
        print "ex) python {0} 20180416".format(sys.argv[0])
        sys.exit(1)