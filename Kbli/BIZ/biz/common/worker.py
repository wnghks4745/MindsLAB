#!/usr/bin/python
# -*- coding: utf-8 -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-05-07, modification: 0000-00-00"

###########
# imports #
###########
import time

#############
# constants #
#############
HEARTBEAT_LIVENESS = 3  # 3..5 is reasonable
HEARTBEAT_INTERVAL = 1.0  # Seconds


#########
# class #
#########
class Worker(object):
    def __init__(self, address):
        self.address = address
        self.expiry = time.time() + HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS
