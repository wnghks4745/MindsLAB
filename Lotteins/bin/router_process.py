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
import zmq
import time
import traceback
import multiprocessing
from biz.common.worker import *
from biz.common.worker_queue import WorkerQueue
from biz.common import util, logger, db_connection
sys.path.append(os.path.join(os.getenv('MAUM_ROOT'), 'lib/python'))
from common.config import Config
from maum.biz.common import common_pb2

###########
# options #
###########
reload(sys)
sys.setdefaultencoding('utf-8')

#############
# constants #
#############
LOGGER = logger.get_timed_rotating_logger(
            logger_name='router',
            file_name=os.path.join(os.getenv('MAUM_ROOT'), 'logs/router_process.log'),
            backup_count=5,
            log_level='debug'
        )
#  Paranoid Pirate Protocol constants
PPP_READY = "\x01"  # Signals worker is ready
PPP_HEARTBEAT = "\x02"  # Signals worker heartbeat


#########
# class #
#########
class BizRouter(multiprocessing.Process):
    def __init__(self, category, frontend_port, backend_port):
        multiprocessing.Process.__init__(self)
        self.category = category
        self.frontend_port = frontend_port
        self.backend_port = backend_port

    def run(self):
        context = zmq.Context(1)
        # ROUTER
        frontend = context.socket(zmq.PULL)
        # ROUTER
        backend = context.socket(zmq.ROUTER)
        # For clients
        frontend.bind("tcp://*:{0}".format(self.frontend_port))
        # For workers
        backend.bind("tcp://*:{0}".format(self.backend_port))
        poll_workers = zmq.Poller()
        poll_workers.register(backend, zmq.POLLIN)
        poll_both = zmq.Poller()
        poll_both.register(frontend, zmq.POLLIN)
        poll_both.register(backend, zmq.POLLIN)
        workers = WorkerQueue()
        heartbeat_at = time.time() + HEARTBEAT_INTERVAL
        try:
            while True:
                poller = poll_both if len(workers.queue) > 0 else poll_workers
                socks = dict(poller.poll(HEARTBEAT_INTERVAL * 1000))
                # Handle worker activity on backend
                if socks.get(backend) == zmq.POLLIN:
                    # Use worker address for LRU routing
                    frames = backend.recv_multipart()
                    if not frames:
                        break
                    address = frames[0]
                    workers.ready(Worker(address))
                    # Validate control message, or return reply to client
                    msg = frames[1:]
                    if len(msg) == 1:
                        if msg[0] not in (PPP_READY, PPP_HEARTBEAT):
                            print "E: Invalid message from worker: {0}".format(msg)
                    else:
                        pass
                        # frontend.send_multipart(msg)
                    # send heartbeats to idle workers if it's time
                    if time.time() >= heartbeat_at:
                        for worker in workers.queue:
                            msg = [worker, PPP_HEARTBEAT]
                            backend.send_multipart(msg)
                        heartbeat_at = time.time() + HEARTBEAT_INTERVAL
                if socks.get(frontend) == zmq.POLLIN:
                    frames = frontend.recv_multipart()
                    if not frames:
                        break
                    frames.insert(0, workers.next())
                    backend.send_multipart(frames)
                    # print frames
                workers.purge()
        except KeyboardInterrupt:
            LOGGER.info('Router ({0}) Process stopped...'.format(self.category))
        except Exception:
            LOGGER.error(traceback.format_exc())


#######
# def #
#######
def main():
    """
    Router process
    """
    global LOGGER
    LOGGER.info('Router Process started...')
    conf = Config()
    conf.init('biz.conf')
    backend_map = dict()
    pipeline_info = util.create_pipeline_info()
    tree_map = pipeline_info['tree']
    category_list = [
        ('CS', conf.get('stt.pull.port'), conf.get('stt.router.port')),
        ('HMD', conf.get('hmd.pull.port'), conf.get('hmd.router.port')),
        ('NLP', conf.get('nlp.pull.port'), conf.get('nlp.router.port')),
        ('DNN', conf.get('dnn.pull.port'), conf.get('dnn.router.port'))
    ]
    process_list = list()
    for category, frontend_port, backend_port in category_list:
        p = BizRouter(category, frontend_port, backend_port)
        p.start()
        backend_map[category] = zmq.Context().socket(zmq.PUSH)
        backend_map[category].connect('tcp://127.0.0.1:{0}'.format(frontend_port))
        process_list.append(p)
    # PULLER
    frontend = zmq.Context().socket(zmq.PULL)
    # For clients
    frontend.bind("tcp://*:{0}".format(conf.get('router.pull.port')))
    cli_socket = zmq.Context().socket(zmq.REP)
    cli_socket.bind('ipc:///tmp/biz_cli.zmq')
    poller = zmq.Poller()
    poller.register(frontend, zmq.POLLIN)
    poller.register(cli_socket, zmq.POLLIN)
    while True:
        oracle = db_connection.Oracle()
        socks = dict(poller.poll(1 * 1000))
        if socks.get(frontend) == zmq.POLLIN:
            header, body = frontend.recv_multipart()
            hdr = common_pb2.Header()
            hdr.ParseFromString(header)
            LOGGER.debug('FRONT(PULL) received info : {0}'.format(hdr))
            # Select tree
            tree = tree_map[hdr.pipeline_id]
            for child in tree.children(hdr.router_id):
                # data: model_params, proc_id, proc_name, router_id, model_id,
                #       config_id, proc_meta, model_name, proc_type_code
                model_params = child.data[0]
                proc_id = child.data[1]
                proc_name = child.data[2]
#                router_id = child.data[3]
#                model_id = child.data[4]
#                config_id = child.data[5]
#                config_meta = child.data[6]
#                proc_meta = child.data[7]
#                model_name = child.data[8]
#                proc_type_code = child.data[9]
                # Make Proto buf Message
                hdr.router_id = child.identifier
                hdr.proc_id = proc_id
                hdr.status_id = util.ProcStatus.PS_WAITING
                hdr.model_params = model_params
                header = hdr.SerializeToString()
                LOGGER.debug('NEXT FLOW info : {0}'.format(hdr))
                if 'HMD' in proc_name:
                    proc_name = 'HMD'
                backend_map[proc_name].send_multipart([header, body])
                util.insert_proc_result(oracle, LOGGER, hdr)
                LOGGER.debug('Sent message to worker')
        elif socks.get(cli_socket) == zmq.POLLIN:
            cmd = cli_socket.recv()
            LOGGER.info('Get cmd : {0}'.format(cmd))
            if cmd == 'reload':
                pipeline_info = util.create_pipeline_info()
                tree_map = pipeline_info['tree']
                cli_socket.send('success')
            else:
                LOGGER.error('Invalid Command')
                cli_socket.send('failed')
        oracle.disconnect()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        LOGGER.info('Router Process stopped...')
    except Exception:
        LOGGER.error(traceback.format_exc())
