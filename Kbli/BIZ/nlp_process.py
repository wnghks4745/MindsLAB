#!/usr/bin/python
# -*- coding:utf-8 -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-06-19, modification: 0000-00-00"

###########
# imports #
###########
import os
import sys
sys.path.append(os.path.join(os.getenv('MAUM_ROOT'), 'lib/python'))
from biz.common import util, logger, biz_worker
from biz.process.nlp.nlp_proc import NlpProc

###########
# options #
###########
reload(sys)
sys.setdefaultencoding('utf8')

#############
# constants #
#############
LOGGER = logger.get_timed_rotating_logger(
            logger_name='nlp',
            file_name=os.path.join(os.getenv('MAUM_ROOT'), 'logs/nlp_process.log'),
            backup_count=5,
            log_level='debug'
        )
# nlp1-kor : 9811
# nlp2-kor : 9813
# nlp2-eng : 9814
# nlp3-kor : 9823
# 다음 같은 형태로 nlp ip별 프로세스 카운트를 가져온다
# remote_list_cnt = [('127.0.0.1:9813',4),('52.187.180.94:9813',6)]  // nlp2-kor
REMOTE_LIST_CNT = util.get_grpc_service_list('PT0003')
REMOTE_LIST = list()


#######
# def #
#######
def usage():
    """
    Usage
    """
    print '{0} [process_count]'.format(sys.argv[0])


def spawn_nlp_worker(index, conf, logq):
    """
    Spawn nlp worker
    :param          index:          Index
    :param          conf:           Conf
    :param          logq:           Logq
    :return:                        NlpProc class object
    """
    global REMOTE_LIST
    nlp_addr = REMOTE_LIST.pop(0)
    return NlpProc(index, conf.get('router.pull.port'), conf.get('nlp.router.port'), logq)


def main():
    """
    NLP process
    """
    global REMOTE_LIST
    global REMOTE_LIST_CNT
    # remote_list_cnt 의 ip list 를 round robin 형태로 리스트로 만든다
    # 결과는 다음처럼 생성된다. [ '127.0.0.1:9801', '52.187.189.94:9801', '127.0.0.1:9801',....]
    while True:
        remote_cnt = len(REMOTE_LIST)
        for idx in range(len(REMOTE_LIST_CNT)):
            remote, cnt = REMOTE_LIST_CNT[idx]
            if cnt > 0:
                REMOTE_LIST.append(remote)
                REMOTE_LIST_CNT[idx] = (remote, cnt - 1)
        # no more list
        if remote_cnt == len(REMOTE_LIST):
            break
    biz_worker.run_workers(spawn_nlp_worker, LOGGER, len(REMOTE_LIST))


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'help':
        usage()
    else:
        main()
