#!/usr/bin/python
# -*- coding: utf-8 -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-05-07, modification: 0000-00-00"

###########
# imports #
###########
import time
from collections import OrderedDict


#########
# class #
#########
class WorkerQueue(object):
    def __init__(self):
        self.queue = OrderedDict()

    def ready(self, worker):
        self.queue.pop(worker.address, None)
        self.queue[worker.address] = worker

    def purge(self):
        """Look for & kill expired workers."""
        now_time = time.time()
        expired = list()
        for address, worker in self.queue.iteritems():
            if now_time > worker.expiry:  # Worker expired
                expired.append(address)
        for address in expired:
            print "W: Idle worker expired: {0}".format(address)
            self.queue.pop(address, None)

    def next(self):
        address, worker = self.queue.popitem(False)
        # print 'I: %s pop, queue size is %d' % (address, len(self.queue))
        return address
