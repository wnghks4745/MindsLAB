#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-05-07, modification: 0000-00-00"

###########
# imports #
###########
import os
import sys
import grpc
from google.protobuf import json_format
sys.path.append(os.path.join(os.getenv('MAUM_ROOT'), "lib/python"))
from common.config import Config
from maum.common import lang_pb2
from maum.brain.hmd import hmd_pb2
from maum.brain.hmd import hmd_pb2_grpc


#########
# class #
#########
class HmdClient(object):
    conf = Config()
    stub = None
    def __init__(self, hmd_addr):
        self.remote_addr, self.remote_port = hmd_addr.split(':')
        channel = grpc.insecure_channel(hmd_addr)
        self.stub = hmd_pb2_grpc.HmdClassifierStub(channel)

    def set_model(self):
        model = hmd_pb2.HmdModel()
        model.lang = lang_pb2.kor
        model.model = 'news'
        rules = list()
        rule1 = hmd_pb2.HmdRule()
        # 형태소 분석 결과를 바탕으로 원형 단어를 이용
        # ex) 안녕하세요. -> 안녕하/pa 시/ep 어/ec 요/jx ./s (ETRI 기준)
        # 안녕하, 시어, 요 를 이용 rule을 제작
        rule1.rule = '(안녕하)'
        rule1.categories.extend(['level1', 'level2'])
        rules.append(rule1)
        rule2 = hmd_pb2.HmdRule()
        rule2.rule = '(자연)'
        rule2.categories.extend(['level1', 'level3'])
        rules.append(rule2)
        model.rules.extend(rules)
        self.stub.SetModel(model)
        model_key = hmd_pb2.ModelKey()
        model_key.lang = lang_pb2.kor
        model_key.model = 'news'
        ret_model = self.stub.GetModel(model_key)
        print unicode(ret_model)
        # print json_format.MessageToJson(ret_model)

    def set_model2(self, file_name, model_name):
        model = hmd_pb2.HmdModel()
        model.lang = lang_pb2.kor
        model.model = model_name
        rules = list()
        f = open(file_name)
        for line in f.readlines():
            tokens = line.strip().split('\t')
            # level1, level2, level3...
            levels = tokens[:-1]
            # Last element of tokens
            keyword = tokens[-1]
            print levels, keyword
            rule = hmd_pb2.HmdRule()
            rule.rule = keyword
            rule.categories.extend(levels)
            rules.append(rule)
        model.rules.extend(rules)
        self.stub.SetModel(model)
        model_key = hmd_pb2.ModelKey()
        model_key.lang = lang_pb2.kor
        model_key.model = model_name
        ret_model = self.stub.GetModel(model_key)
        print unicode(ret_model)

    def make_hmd_model(self, temp_list, model_name):
        model = hmd_pb2.HmdModel()
        model.lang = lang_pb2.kor
        model.model = model_name
        rules = list()
        for item in temp_list:
            category_list = item[0]
            dtc_rule = item[1]
            rule = hmd_pb2.HmdRule()
            rule.rule = dtc_rule
            rule.categories.extend(category_list)
            rules.append(rule)
        model.rules.extend(rules)
        self.stub.SetModel(model)
        model_key = hmd_pb2.ModelKey()
        model_key.lang = lang_pb2.kor
        model_key.model = model_name
        self.stub.GetModel(model_key)

    def get_class_by_text(self, text):
        in_doc = hmd_pb2.HmdInputText()
        in_doc.text = text
        in_doc.model = 'news'
        in_doc.lang = lang_pb2.kor
        ret = self.stub.GetClassByText(in_doc)
        json_ret = json_format.MessageToJson(ret, True)
        # print json_ret
        for cls in ret.cls:
            print 'cls is ', cls.sent_seq
            print unicode(cls)
        for sentence in ret.document.sentences:
            print 'sentence is ', sentence.seq
            print unicode(sentence.text)
        # print unicode(ret)
        # self.stub.AnalyzeMultiple()
        # SEE grpc.io python examples

    def get_class_by_text2(self, text, model):
        in_doc = hmd_pb2.HmdInputText()
        in_doc.text = text
        in_doc.model = model
        in_doc.lang = lang_pb2.kor
        ret = self.stub.GetClassByText(in_doc)
        json_ret = json_format.MessageToJson(ret, True)
        # print json_ret
        #
        # for cls in ret.cls:
        #     print 'cls is ', cls.sent_seq
        #     print unicode(cls)
        # for sentence in ret.document.sentences:
        #     print 'sentence is ', sentence.seq
        #     print unicode(sentence.text)
        return ret

    def get_class(self, model, document):
        in_doc = hmd_pb2.HmdInputDocument()
        print 'in_doc : ', in_doc
        in_doc.model = model
        in_doc.lang = lang_pb2.kor
        in_doc.document = document
        ret = self.stub.GetClass(in_doc)
        json_ret = json_format.MessageToJson(ret, True)
        return ret