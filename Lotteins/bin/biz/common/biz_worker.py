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
import zmq
import time
import util
import logger
import traceback
import multiprocessing
from random import randint
sys.path.append(os.path.join(os.getenv('MAUM_ROOT'), 'lib/python'))
from common.config import Config
from maum.biz.common import common_pb2

#############
# constants #
#############
HEARTBEAT_LIVENESS = 3
HEARTBEAT_INTERVAL = 1
INTERVAL_INIT = 1
INTERVAL_MAX = 32
#  Paranoid Pirate Protocol constants
PPP_READY = "\x01"  # Signals worker is ready
PPP_HEARTBEAT = "\x02"  # Signals worker heartbeat


#########
# class #
#########
class BizWorker(multiprocessing.Process):
    def __init__(self, frontend_port, backend_port, logq):
        """
        Args:
            frontend_port : 결과를 보낼 곳
            backend_port : 결과를 받아올 곳
        """
        multiprocessing.Process.__init__(self)
        self.logger = ''
        self.logq = logq
        self.router_socket = ''
        self.backend_port = backend_port
        self.frontend_port = frontend_port

    def log(self, log_level, message):
        self.logq.put((log_level, message))

    def initialize(self):
        pass

    def init_internal(self):
        """Process 생성 이후에 호출해야 됨을 주의할 것"""
        self.router_socket = zmq.Context().socket(zmq.PUSH)
        self.router_socket.connect("tcp://localhost:{0}".format(self.frontend_port))

    def do_work(self, frames):
        pass

    def worker_socket(self, context, poller):
        """ Helper function that returns a new configured socket
           connected to the Paranoid Pirate queue """
        worker = context.socket(zmq.DEALER)  # DEALER
        identity = "%04X-%04X" % (randint(0, 0x10000), randint(0, 0x10000))
        self.logger = logger.get_logger(identity)
        self.logger.info('WORKER IDENTITY is %s' % identity)
        worker.setsockopt(zmq.IDENTITY, identity)
        poller.register(worker, zmq.POLLIN)
        worker.connect("tcp://localhost:%s" % self.backend_port)
        worker.send(PPP_READY)
        return worker

    def sendto_router_ex(self, info, body):
        header = info.SerializeToString()
        rc = self.router_socket.send_multipart([header, body])
        #self.logger.debug('rc is (%s), sent header (%s), body (%s)' % (rc, header, body))

    def sendto_router(self, msg_hdr, flow_id, call_id, result):
        """
        Args:
            flow_set (str): The first parameter.
            flow_id (str): The second parameter.
            call_id (str): call_id
            result (protobuf): grpc result

        Returns:
            None
        """
        # Make Protobuf Message
        # input으로 들어왔던 메시지에 body값만 생성한뒤 라우터로
        # 보내면 router에서 flow_id, category, model값을 알아서
        # 교체해서 다음 프로세스로 전달한다
        msg_hdr = common_pb2.Header()
        self.router_socket.send_multipart(
            [msg_hdr.SerializeToString(), msg_meta.SerializeToString(), msg_body.SerializeToString()]
        )

    def run(self):
        self.initialize()
        self.init_internal()
        context = zmq.Context(1)
        poller = zmq.Poller()
        liveness = HEARTBEAT_LIVENESS
        interval = INTERVAL_INIT
        heartbeat_at = time.time() + HEARTBEAT_INTERVAL
        worker = self.worker_socket(context, poller)
        try:
            while True:
                socks = dict(poller.poll(HEARTBEAT_INTERVAL * 1000))
                # Handle worker activity on backend
                if socks.get(worker) == zmq.POLLIN:
                    #  Get message
                    #  - 3-part envelope + content -> request
                    #  - 1-part HEARTBEAT -> heartbeat
                    frames = worker.recv_multipart()
                    if not frames:
                        break  # Interrupted
                    if len(frames) == 2:
                        self.do_work(frames)
                        worker.send(PPP_READY)
                        # worker.send_multipart(frames)
                        liveness = HEARTBEAT_LIVENESS
                        # time.sleep(1)  # Do some heavy work
                    elif len(frames) == 1 and frames[0] == PPP_HEARTBEAT:
                        # self.logger.debug('Queue sent heartbeat')
                        # print "I: Queue heartbeat"
                        liveness = HEARTBEAT_LIVENESS
                    else:
                        print "E: Invalid message: {0}".format(frames)
                    interval = INTERVAL_INIT
                else:
                    liveness -= 1
                    if liveness == 0:
                        print "W: Heartbeat failure, can't reach queue"
                        print "W: Reconnecting in %0.2fs…" % interval
                        time.sleep(interval)
                        if interval < INTERVAL_MAX:
                            interval *= 2
                        poller.unregister(worker)
                        worker.setsockopt(zmq.LINGER, 0)
                        worker.close()
                        worker = self.worker_socket(context, poller)
                        liveness = HEARTBEAT_LIVENESS
                if time.time() > heartbeat_at:
                    heartbeat_at = time.time() + HEARTBEAT_INTERVAL
                    # self.logger.debug('Worker send heartbeat')
                    # print "I: Worker heartbeat"
                    worker.send(PPP_HEARTBEAT)
        except Exception:
            self.logger.error(traceback.format_exc())
            self.logger.error('worker is stopped')


#######
# def #
#######
def run_workers(spawn_worker, get_logger, process_count=0):
    """
    Run workers
    :param          spawn_worker:           Worker
    :param          get_logger:             Logger
    :param          process_count:          Process count
    """
    conf = Config()
    conf.init('biz.conf')
    logq = multiprocessing.Queue()
    if process_count == 0:
        if len(sys.argv) > 1:
            process_count = int(sys.argv[1])
        else:
            process_count = multiprocessing.cpu_count()
    process_list = list()
    for i in range(process_count):
        p = spawn_worker(i, conf, logq)
        p.start()
        process_list.append(p)
    while True:
        try:
            log_level, message = logq.get()
            get_logger.log(log_level, message)
        except Exception:
            break
    # process last log
    time.sleep(0.2)
    while logq.empty() is False:
        log_level, message = logq.get()
        get_logger.log(log_level, message)
    get_logger.info('Waiting for child processes...')
    for p in process_list:
        p.terminate()
