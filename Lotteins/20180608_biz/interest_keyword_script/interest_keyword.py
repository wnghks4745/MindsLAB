#!/usr/bin/python
# -*- coding: utf-8 -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-06-04, modification: 0000-00-00"

###########
# imports #
###########
import os
import sys
import time
import traceback
import cx_Oracle
from datetime import datetime
from multiprocessing import Process, Manager
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
ST = ''
DT = ''
CREATOR_ID = ''

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

    def select_interest_keyword_call_list(self, call_date, project_code):
        query = """
            SELECT
                AA.CALL_ID,
                AA.BUSINESS_DCD,
                BB.PIPELINE_EVENT_ID,
                CC.SENTENCE,
                CC.SPEAKER_CODE,
                CC.STT_RESULT_DETAIL_ID,
                DD.NLP_SENTENCE,
                DD.NLP_RESULT_ID
            FROM
                (
                    SELECT
                        CALL_ID,
                        BUSINESS_DCD
                    FROM
                        CM_CALL_META_TB
                    WHERE 1=1
                        AND CALL_DATE = TO_DATE(:1, 'YYYY/MM/DD')
                        AND PROJECT_CODE = :2
                ) AA,
                (
                    SELECT
                        CALL_ID,
                        STT_RESULT_ID,
                        PIPELINE_EVENT_ID
                    FROM
                        STT_RESULT_TB
                    WHERE 1=1
                        AND CALL_DATE = TO_DATE(:3, 'YYYY/MM/DD')
                ) BB,
                (
                    SELECT
                        SENTENCE,
                        SPEAKER_CODE,
                        STT_RESULT_ID,
                        STT_RESULT_DETAIL_ID
                    FROM
                        STT_RESULT_DETAIL_TB
                    WHERE 1=1
                        AND CALL_DATE = TO_DATE(:4, 'YYYY/MM/DD')
                ) CC,
                (
                    SELECT
                        NLP_SENTENCE,
                        NLP_RESULT_ID,
                        STT_RESULT_DETAIL_ID
                    FROM
                        TA_NLP_RESULT_TB
                    WHERE 1=1
                        AND CALL_DATE = TO_DATE(:5, 'YYYY/MM/DD')
                ) DD
            WHERE 1=1
                AND AA.CALL_ID = BB.CALL_ID
                AND BB.STT_RESULT_ID = CC.STT_RESULT_ID
                AND CC.STT_RESULT_DETAIL_ID = DD.STT_RESULT_DETAIL_ID
        """
        bind = (
            call_date,
            project_code,
            call_date,
            call_date,
            call_date,
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchall()
        if result is bool:
            return False
        if not result:
            return False
        return result

    def insert_interest_keyword_hmd_result(self, values_list):
        query = """
        INSERT INTO CS_INTEREST_KEYWORD_TB
        (
            PIPELINE_EVENT_ID,
            NLP_RESULT_ID,
            CATEGORY,
            KEYWORD_NAME,
            CALL_DATE,
            SPEAKER_CODE,
            PROJECT_CODE,
            BUSINESS_DCD,
            CALL_ID,
            STT_RESULT_DETAIL_ID,
            CREATED_DTM,
            UPDATED_DTM,
            CREATOR_ID,
            UPDATOR_ID
        )
        VALUES
        (
            :1, :2, :3, :4,
            TO_DATE(:5, 'YYYY/MM/DD'),
            :6, :7, :8, :9, :10, SYSDATE,
            SYSDATE, :11, :12
        )
        """
        self.cursor.executemany(query, values_list)
        if self.cursor.rowcount > 0:
            self.conn.commit()
        else:
            self.conn.rollback()

    def delete_interest_keyword_hmd_result(self, call_date, project_code):
        try:
            query = """
                DELETE FROM
                    CS_INTEREST_KEYWORD_TB
                WHERE 1=1
                    AND CALL_DATE = TO_DATE(:1, 'YYYY-MM-DD')
                    AND PROJECT_CODE = :2
            """
            bind = (
                call_date,
                project_code,
            )
            self.cursor.execute(query, bind)
            if self.cursor.rowcount > 0:
                self.conn.commit()
                return True
            else:
                self.conn.rollback()
                return False
        except Exception:
            self.conn.rollback()
            raise Exception(traceback.format_exc())


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
    print 'Connect {0} DB ...'.format(db)
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
                print "Unknown DB [{0}]".format(db)
                logger.error("Unknown DB [{0}]".format(db))
                return False
            print "Success connect {0} DB ...".format(db)
            logger.debug("Success connect {0} DB ...".format(db))
            break
        except Exception:
            exc_info = traceback.format_exc()
            logger.error(exc_info)
            if cnt < 3:
                print "Fail connect {0} DB, retrying count = {1}".format(db, cnt)
                logger.error("Fail connect {0} DB, retrying count = {1}".format(db, cnt))
            time.sleep(10)
            continue
    if not sql:
        return False
    return sql


def find_loc(vec_space, pos, plus_num, len_line):
    """
    Find loc
    :param      vec_space:
    :param      pos:
    :param      plus_num:
    :param      len_line:
    :return:
    """
    s_i = 0
    len_vs = len(vec_space)
    for s_i in range(len_vs):
        if vec_space[s_i] > pos:
            break
    key_pos = vec_space[s_i]
    if s_i != -1:
        e_i = s_i + plus_num
        if e_i >= len_vs:
            e_i = len_vs - 1
        end_pos = vec_space[e_i] + 1
    else:
        end_pos = len_line
    return key_pos, end_pos


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


def execute_hmd(nlp_sentence, sentence, model_name):
    """
    HMD
    :param      nlp_sentence:           NLP sentence
    :param      sentence:               Sentence
    :param      model_name:             Model Name
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
    s_tmp = " {0} ".format(nlp_sentence)
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


def extract_interest_keyword_hmd(logger, idx, pro_id, model_params, target_results, return_dict):
    """
    Extract interest keyword hmd
    :param              logger:                     Logger
    :param              idx:                        Index
    :param              pro_id:                     Process ID
    :param              model_params:               Model param
    :param              target_results:             Target results
    :param              return_dict:                Return dictionary
    """
    cnt = 0
    return_list = list()
    for target_result in target_results:
        try:
            call_id = target_result[0]
            business_dcd = target_result[1]
            pipeline_event_id = target_result[2]
            sentence = target_result[3]
            speaker_code = target_result[4]
            stt_result_detail_id = target_result[5]
            nlp_sentence = target_result[6]
            nlp_result_id = target_result[7]
            hmd_result = execute_hmd(nlp_sentence, sentence, model_params)
            if len(hmd_result) > 0:
                cnt += len(hmd_result.keys())
                temp_dict = {
                    'call_id': call_id,
                    'business_dcd': business_dcd,
                    'pipeline_event_id': pipeline_event_id,
                    'sentence': sentence,
                    'speaker_code': speaker_code,
                    'stt_result_detail_id': stt_result_detail_id,
                    'nlp_sentence': nlp_sentence,
                    'nlp_result_id': nlp_result_id
                }
                return_list.append((hmd_result, temp_dict))
        except Exception:
            print traceback.format_exc()
            logger.error(traceback.format_exc())
            continue
    return_dict[idx] = return_list
    print "Process ID : {0}, Target : {1}, Result : {2}".format(pro_id, len(target_results), cnt)
    logger.info("Process ID : {0}, Target : {1}, Result : {2}".format(pro_id, len(target_results), cnt))


def insert_result_data(logger, oracle, call_date, project_code, return_list):
    """
    Insert result data
    :param          logger:             Logger
    :param          oracle:             Oracle
    :param          call_date:          CALL_DATE
    :param          project_code:       PROJECT_CODE
    :param          return_list:        Result list
    """
    cnt = 0
    value_list = list()
    print "4. Insert result data"
    logger.info("4. Insert result data")
    for hmd_result, temp_dict in return_list:
        for cls in hmd_result.keys():
            try:
                cat_list = cls.split('_')
                if len(cat_list) != 2:
                    continue
                cnt += 1
                category, keyword_name = cat_list
                values_tuple = (
                    temp_dict['pipeline_event_id'],
                    temp_dict['nlp_result_id'],
                    category,
                    keyword_name,
                    call_date,
                    temp_dict['speaker_code'],
                    project_code,
                    temp_dict['business_dcd'],
                    temp_dict['call_id'],
                    temp_dict['stt_result_detail_id'],
                    CREATOR_ID,
                    CREATOR_ID,
                )
                value_list.append(values_tuple)
                if len(value_list) == 10000:
                    cnt += 10000
                    print 'Insert data {0}'.format(cnt)
                    logger.info('Insert data {0}'.format(cnt))
                    oracle.insert_interest_keyword_hmd_result(value_list)
                    value_list = list()
            except Exception:
                print traceback.format_exc()
                logger.error(traceback.format_exc())
                continue
    if len(value_list) > 0:
        cnt += len(value_list)
        print 'Insert rest data..'
        logger.info('Insert rest data..')
        oracle.insert_interest_keyword_hmd_result(value_list)
    print "Total upload count : {0}".format(cnt)
    logger.info("Total upload count : {0}".format(cnt))


def processing(target_call_date, pro_code):
    """
    Processing
    :param          target_call_date:       Target call date
    :param          pro_code:               PROJECT_CODE
    """
    # Add logging
    logger_args = {
        'base_path': CONFIG['log_dir_path'],
        'log_file_name': pro_code + "_" + CONFIG['log_name'],
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
    # Execute
    try:
        project_code = 'PC0001' if pro_code == 'CS' else 'PC0002'
        model_params = 'interest_keyword_cs_hmd' if pro_code == 'CS' else 'interest_keyword_tm_hmd'
        call_date = "{0}-{1}-{2}".format(target_call_date[:4], target_call_date[4:6], target_call_date[6:])
        print "CALL_DATE = {0}, PROJECT_CODE = {1}, HMD = {2}".format(call_date, project_code, model_params)
        logger.info("CALL_DATE = {0}, PROJECT_CODE = {1}, HMD = {2}".format(call_date, project_code, model_params))
        print "1. Select target data"
        logger.info("1. Select target data")
        target_results = oracle.select_interest_keyword_call_list(call_date, project_code)
        oracle.disconnect()
        if target_results:
            print "{0} target data count = {1}".format(pro_code, len(target_results))
            logger.info("{0} target data count = {1}".format(pro_code, len(target_results)))
            print "2. Execute HMD"
            logger.info("2. Execute HMD")
            proc_list = list()
            split_cnt = len(target_results) / 10
            manager = Manager()
            return_dict = manager.dict()
            pro_id = 0
            for idx in range(0, len(target_results), split_cnt):
                proc = Process(target=extract_interest_keyword_hmd, args=(
                    logger, idx, pro_id, model_params, target_results[idx:split_cnt+idx], return_dict), )
                proc_list.append(proc)
                proc.start()
                pro_id += 1
            for proc in proc_list:
                proc.join()
            return_list = list()
            for item in return_dict.values():
                return_list += item
            print "Total return : {0}".format(len(return_list))
            logger.info("Total return : {0}".format(len(return_list)))
            print "3. Delete data"
            logger.info("3. Delete data")
            oracle = connect_db(logger, 'Oracle')
            oracle.delete_interest_keyword_hmd_result(call_date, project_code)
            # Insert interest keyword hmd result
            insert_result_data(logger, oracle, call_date, project_code, return_list)
            oracle.disconnect()
        else:
            print "No {0} target data".format(pro_code)
            logger.info("No {0} target data".format(pro_code))
        print "END.. Start time = {0}, The time required = {1}".format(ST, elapsed_time(DT))
        logger.info("END.. Start time = {0}, The time required = {1}".format(ST, elapsed_time(DT)))
        logger.info("-" * 100)
        for handler in logger.handlers:
            handler.close()
            logger.removeHandler(handler)
    except Exception:
        exc_info = traceback.format_exc()
        print exc_info
        print "---------- ERROR ----------"
        logger.error(exc_info)
        logger.error("---------- ERROR ----------")
        print "END.. Start time = {0}, The time required = {1}".format(ST, elapsed_time(DT))
        logger.info("END.. Start time = {0}, The time required = {1}".format(ST, elapsed_time(DT)))
        logger.info("-" * 100)
        for handler in logger.handlers:
            handler.close()
            logger.removeHandler(handler)
        sys.exit(1)


########
# main #
########
def main(pro_code, target_call_date):
    """
    This is a program that
    :param          pro_code:                PROJECT_CODE
    :param          target_call_date:        Target call date
    """
    global ST
    global DT
    global CREATOR_ID
    ts = time.time()
    ST = datetime.fromtimestamp(ts).strftime('%Y-%m-%d-%H:%M.%S')
    DT = datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S')
    try:
        CREATOR_ID = 'CS_SCR' if pro_code == 'CS' else 'TM_SCR'
        processing(target_call_date, pro_code)
    except Exception:
        print traceback.format_exc()
        sys.exit(1)


if __name__ == '__main__':
    if len(sys.argv) == 3:
        if sys.argv[1].upper() not in ['CS', 'TM']:
            print "usage : python {0} [CS or TM] [YYYYMMDD]".format(sys.argv[0])
            print "ex) python {0} CS 20180416".format(sys.argv[0])
            sys.exit(1)
        elif len(sys.argv[2].strip()) != 8:
            print "usage : python {0} [CS or TM] [YYYYMMDD]".format(sys.argv[0])
            print "ex) python {0} CS 20180416".format(sys.argv[0])
            sys.exit(1)
        try:
            int(sys.argv[2])
        except Exception:
            print "usage : python {0} [CS or TM] [YYYYMMDD]".format(sys.argv[0])
            print "ex) python {0} CS 20180416".format(sys.argv[0])
            sys.exit(1)
        main(sys.argv[1].strip().upper(), sys.argv[2].strip())
    elif len(sys.argv) == 2:
        if sys.argv[1].upper() not in ['CS', 'TM']:
            print "usage : python {0} [CS or TM] [YYYYMMDD]".format(sys.argv[0])
            print "ex) python {0} CS 20180416".format(sys.argv[0])
            sys.exit(1)
        main(sys.argv[1].strip().upper(), (datetime.fromtimestamp(time.time()) - timedelta(days=1)).strftime('%Y%m%d'))
    else:
        print "usage : python {0} [CS or TM] [YYYYMMDD]".format(sys.argv[0])
        print "ex) python {0} CS 20180416".format(sys.argv[0])
        sys.exit(1)