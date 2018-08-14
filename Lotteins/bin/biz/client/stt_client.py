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
import grpc
import time
sys.path.append(os.path.join(os.getenv('MAUM_ROOT'), "lib/python"))
from common.config import Config
from maum.common import lang_pb2, types_pb2
from maum.brain.stt import stt_pb2, stt_pb2_grpc


#########
# class #
#########
class SttClient(object):
    conf = Config()
    real_stub = None
    resolver_stub = None
    def __init__(self, stt_addr):
        self.remote_addr, self.remote_port = stt_addr.split(':')
        channel = grpc.insecure_channel(stt_addr)
        self.resolver_stub = stt_pb2_grpc.SttModelResolverStub(channel)

    def get_servers(self, _name, _lang, _sample_rate):
        """ Find & Connect servers """
        # Define model
        model = stt_pb2.Model()
        model.lang = lang_pb2.eng if _lang == 'eng' else lang_pb2.kor
        model.model = _name
        model.sample_rate = _sample_rate
        try:
            # Find Server
            server_status = self.resolver_stub.Find(model)
        except grpc.RpcError as e:
            print e
            return
        except Exception:
            print "Can't found mode {0}-{1}-{2}".format(_name, _lang, _sample_rate)
            return False
        # Remote CS service
        ip, port = server_status.server_address.split(':')
        channel = grpc.insecure_channel("{0}:{1}".format(self.remote_addr, port))
        self.real_stub = stt_pb2_grpc.SttRealServiceStub(channel)
        # Get CS server status
        wait_cnt = 0
        while not self.get_stt_status(model):
            time.sleep(1)
            wait_cnt += 1
            if wait_cnt > 10:
                return False
            continue
        return True

    def get_stt_status(self, model):
        """ Return CS server status """
        try:
            status = self.real_stub.Ping(model)
            print "Model : {0}".format(status.model)
            print "Sample Rate : {0}".format(status.sample_rate)
            print "Lang : {0}".format(status.lang)
            print "Running : {0}".format(status.running)
            print "Server Address : {0}".format(status.server_address)
            print "Invoked by : {0}".format(status.invoked_by)
            return status.running
        except Exception:
            return False

    def simple_recognize(self, audio_file):
        """ Speech to Text function """
        result = self.real_stub.SimpleRecognize(bytes_from_file(audio_file))
        print "RESULT : {0}".format(result.txt)
        return result.txt

    def detail_recognize(self, audio_file):
        """ Speech to Text function """
        result = self.real_stub.DetailRecognize(bytes_from_file(audio_file))
        # print 'RESULT : ', result.txt
        # print 'segments : ', result.segments
        # print 'fragments : ', result.fragments
        # print 'raw_mlf : ', result.raw_mlf
        return result

    def set_model(self, filename):
        metadata={(b'in.lang', b'kor'), (b'in.model', 'weather'), (b'in.samplerate', '8000') }
        result = self.resolver_stub.SetModel(bytes_from_file2(filename), metadata=metadata)
        print "RESULT : {0}".format(result.lang)
        print "RESULT : {0}".format(result.model)
        print "RESULT : {0}".format(result.sample_rate)
        print "RESULT : {0}".format(result.result)
        print "RESULT : {0}".format(result.error)

    def delete_model(self, name, lang, sample_rate):
        model = stt_pb2.Model()
        model.lang = lang_pb2.eng if lang == 'eng' else lang_pb2.kor
        model.model = name
        model.sample_rate = sample_rate
        status = self.resolver_stub.DeleteModel(model)
        print "Model : {0}".format(status.model)
        print "Sample Rate : {0}".format(status.sample_rate)
        print "Lang : {0}".format(status.lang)
        print "Running : {0}".format(status.running)
        print "Server Address : {0}".format(status.server_address)
        print "Invoked by : {0}".format(status.invoked_by)
        return status.running


#######
# def #
#######
def bytes_from_file(file_name, chunk_size=10000):
    """
    Bytes from file
    :param          file_name:           File name
    :param          chunk_size:          Chunk size
    """
    with open(file_name, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if chunk:
                speech = stt_pb2.Speech()
                speech.bin = chunk
                yield speech
            else:
                break


def bytes_from_file2(file_name, chunk_size=1024 * 1024):
    """
    Bytes from file
    :param          file_name:           File name
    :param          chunk_size:          Chunk size
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


def do_test():
    """
    Do test
    """
    st = time.time()
    pcm_filename = sys.argv[1]
    #client = SttClient('52.163.62.200:9801')
    client = SttClient('52.187.180.94:9801')
    if client.get_servers('baseline', 'kor', 8000):
        result = client.detail_recognize(pcm_filename)
        for segment in result.segments:
            print segment.start, segment.txt, segment.end
    print 'elapsed time is %.3f' % (time.time() - st)


if __name__ == '__main__':
    do_test()
