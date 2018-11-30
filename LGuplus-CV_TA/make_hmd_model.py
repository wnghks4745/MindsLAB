#!/usr/bin/python
# -*- coding:utf-8 -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-09-20, modification: 0000-00-00"

###########
# imports #
###########
import os
import sys
import grpc
import traceback
sys.path.append(os.path.join(os.getenv('MAUM_ROOT'), 'lib/python'))
from common.config import Config
from maum.common import lang_pb2
from maum.brain.hmd import hmd_pb2
from maum.brain.hmd import hmd_pb2_grpc

###########
# options #
###########
reload(sys)
sys.setdefaultencoding("utf-8")


#########
# class #
#########
class HmdClient(object):
    def __init__(self):
        self.conf = Config()
        self.conf.init('brain-ta.conf')
        remote = 'localhost:{0}'.format(self.conf.get('brain-ta.hmd.front.port'))
        channel = grpc.insecure_channel(remote)
        self.stub = hmd_pb2_grpc.HmdClassifierStub(channel)

    def set_model(self, model_name, target_list):
        model = hmd_pb2.HmdModel()
        model.lang = lang_pb2.kor
        model.model = model_name
        rules_list = list()
        for item_dict in target_list:
            category = item_dict['category']
            rule = item_dict['rule']
            category_list = category.split('!@#$')
            hmd_client = hmd_pb2.HmdRule()
            hmd_client.rule = rule
            hmd_client.categories.extend(category_list)
            rules_list.append(hmd_client)
        model.rules.extend(rules_list)
        self.stub.SetModel(model)
        model_key = hmd_pb2.ModelKey()
        model_key.lang = lang_pb2.kor
        model_key.model = model_name


#######
# def #
#######
def main(target_list, model_name):
    """
    This program that make HMD model
    :param      target_list:        [{}, {}, {}]
    :param      model_name:         Model name
    """
    try:
        hmd_client = HmdClient()
        hmd_client.set_model(model_name, target_list)
    except Exception:
        print traceback.format_exc()
        sys.exit(1)
