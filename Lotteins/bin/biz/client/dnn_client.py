#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 0000-00-00, modification: 0000-00-00"

###########
# imports #
###########
import os
import sys
import time
import grpc
from google.protobuf import empty_pb2
from google.protobuf import json_format
sys.path.append(os.path.join(os.getenv('MAUM_ROOT'), "lib/python"))
from common.config import Config
from maum.common import lang_pb2
from maum.common import types_pb2
from maum.brain.cl import classifier_pb2
from maum.brain.cl import classifier_pb2_grpc


#########
# class #
#########
class ClassifierClient(object):
    model = ''
    lang = ''
    conf = Config()
    cl_stub = None
    resolver_stub = None
    def __init__(self, dnn_addr):
        self.remote_addr, self.remote_port = dnn_addr.split(':')
        channel = grpc.insecure_channel(dnn_addr)
        self.resolver_stub = classifier_pb2_grpc.ClassifierResolverStub(channel)

    def get_server(self, name, lang):
        """ Find & Connect servers """
        # Define model
        model = classifier_pb2.Model()
        model.lang = lang_pb2.eng if lang == 'eng' else lang_pb2.kor
        model.model = name
        # Find Server
        try:
            server_status = self.resolver_stub.Find(model)
        except Exception:
            print "Can't found model {0}-{1}".format(name, lang)
            return False
        # Remote Classifier service
        channel = grpc.insecure_channel(server_status.server_address)
        self.cl_stub = classifier_pb2_grpc.ClassifierStub(channel)
        # Get server status
        wait_cnt = 0
        while not self.get_status(model):
            time.sleep(1)
            wait_cnt += 1
            if wait_cnt > 100:
                return False
            continue
        return True

    def get_status(self, model):
        """ Return Classifier server status """
        try:
            status = self.cl_stub.Ping(model)
            print "Model : {0}".format(status.model)
            print "Lang : {0}".format(status.lang)
            print "Running : {0}".format(status.running)
            print "Server address : {0}".format(status.server_address)
            print "Invoked by : {0}".format(status.invoked_by)
            # FORCELY SET MODEL and LANG
            self.model = status.model
            self.lang = status.lang
            return status.running
        except Exception:
            return False

    def get_class_sample(self, text):
        cl_text = classifier_pb2.ClassInputText()
        cl_text.text = text
        cl_text.model = self.model
        cl_text.lang = self.lang
        summary = self.cl_stub.GetClass(cl_text)
        output_dict = json_format.MessageToDict(summary, True, True)
        print 'BEGIN RESULT -----'
        print repr(output_dict).decode('unicode-escape')
        print 'END RESULT -----'

    def get_class(self, text):
        cl_text = classifier_pb2.ClassInputText()
        cl_text.text = text
        cl_text.model = self.model
        cl_text.lang = self.lang
        summary = self.cl_stub.GetClass(cl_text)
        return summary

    def get_models(self):
        print self.resolver_stub.GetModels(empty_pb2.Empty())

    def set_model(self, filename):
        metadata = {(b'in.lang', b'kor'), (b'in.model', 'weather')}
        resp = self.resolver_stub.SetModel(read_file_stream(filename), metadata=metadata)
        output_dict = json_format.MessageToDict(resp, True, True)
        print 'BEGIN RESULT -----'
        print repr(output_dict).decode('unicode-escape')
        print 'END RESULT -----'

    def delete_model(self, name, lang):
        model = classifier_pb2.Model()
        model.lang = lang_pb2.eng if lang == 'eng' else lang_pb2.kor
        model.model = name
        status = self.resolver_stub.DeleteModel(model)
        output_dict = json_format.MessageToDict(status, True, True)
        print 'BEGIN RESULT -----'
        print repr(output_dict).decode('unicode-escape')
        print 'END RESULT -----'


#######
# def #
#######
def read_file_stream(file_name, chunk_size=1024 * 1024):
    """
    Read file stream
    :param          file_name:               File name
    :param          chunk_size:             Chunk size
    """
    with open(file_name, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if chunk:
                part = types_pb2.FilePart()
                part.part = chunk
                yield part
            else:
                break