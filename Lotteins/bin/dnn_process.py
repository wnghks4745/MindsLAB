#!/usr/bin/python
# -*- coding:utf-8 -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 0000-00-00, modification: 0000-00-00"

###########
# imports #
###########
import os
import sys
sys.path.append(os.path.join(os.getenv('MAUM_ROOT'), 'lib/python'))
from biz.common import util, logger, biz_worker
from biz.process.dnn.dnn_proc import DnnProc

###########
# options #
###########
reload(sys)
sys.setdefaultencoding('utf-8')

#############
# constants #
#############
LOGGER = logger.get_timed_rotating_logger(
            logger_name='dnn',
            file_name=os.path.join(os.getenv('MAUM_ROOT'), 'logs/dnn_process.log'),
            backup_count=5,
            log_level='debug'
        )
REMOTE_LIST_CNT = util.get_grpc_service_list('PT0005')
REMOTE_LIST = list()


#######
# def #
#######
def usage():
    """
    Usage
    """
    print '{0} [process_count]'.format(sys.argv[0])


def spawn_dnn_worker(index, conf, logq):
    """
    Spawn dnn worker
    :param          index:          Index
    :param          conf:           Conf
    :param          logq:           Logq
    :return:                        DnnProc class object
    """
    global REMOTE_LIST
    dnn_addr = REMOTE_LIST.pop(0)
    return DnnProc(index, conf.get('router.pull.port'), conf.get('dnn.router.port'), logq, dnn_addr)


def main():
    """
    Dnn process
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
    biz_worker.run_workers(spawn_dnn_worker, LOGGER, len(REMOTE_LIST))


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'help':
        usage()
    else:
        main()
