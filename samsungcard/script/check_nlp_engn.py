#!/usr/bin/python
# -*- coding: utf-8 -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-08-14, modification: 0000-00-00"

###########
# imports #
###########
import os
import sys
import grpc
import time
import argparse
import traceback
from datetime import datetime
sys.path.append(os.path.join(os.getenv('MAUM_ROOT'), 'lib/python'))
from common.config import Config
from maum.common import lang_pb2
from maum.brain.nlp import nlp_pb2
from maum.brain.nlp import nlp_pb2_grpc

###########
# options #
###########
reload(sys)
sys.setdefaultencoding("utf-8")


#########
# class #
#########
class NlpClient(object):
    def __init__(self, args):
        self.args = args
        self.conf = Config()
        self.conf.init('brain-ta.conf')
        if args.engine.lower() == 'nlp1':
            self.remote = 'localhost:{0}'.format(self.conf.get('brain-ta.nlp.1.kor.port'))
            channel = grpc.insecure_channel(self.remote)
            self.stub = nlp_pb2_grpc.NaturalLanguageProcessingServiceStub(channel)
        elif args.engine.lower() == 'nlp2':
            self.remote = 'localhost:{0}'.format(self.conf.get('brain-ta.nlp.2.kor.port'))
            channel = grpc.insecure_channel(self.remote)
            self.stub = nlp_pb2_grpc.NaturalLanguageProcessingServiceStub(channel)
        elif args.engine.lower() == 'nlp3':
            self.remote = 'localhost:{0}'.format(self.conf.get('brain-ta.nlp.3.kor.port'))
            channel = grpc.insecure_channel(self.remote)
            self.stub = nlp_pb2_grpc.NaturalLanguageProcessingServiceStub(channel)
        else:
            print 'Not existed Engine'
            raise Exception('Not existed Engine')

    def analyze(self, target_text):
        in_text = nlp_pb2.InputText()
        try:
            in_text.text = target_text
        except Exception:
            in_text.text = unicode(target_text, 'euc-kr').encode('utf-8')
        in_text.lang = lang_pb2.kor
        in_text.split_sentence = True
        in_text.use_tokenizer = False
        in_text.use_space = False
        in_text.level = 1
        in_text.keyword_frequency_level = 0
        try:
            ret = self.stub.Analyze(in_text)
            if ret:
                return True
            else:
                return False
        except Exception:
            return False


########
# main #
########
def main(args):
    """
    This program that connecting NLP engine
    :param      args:       Arguments
    """
    try:
        test_sent = '이 문장은 테스트 문장입니다.'
        start_time = datetime.fromtimestamp(time.time())
        nlp_client = NlpClient(args)
        result = nlp_client.analyze(test_sent)
        if result:
            print 'Connect succeed'
        else:
            print 'Connect failed'
            print 'Connect retrying'
            while True:
                result = nlp_client.analyze(test_sent)
                if result:
                    required_time = start_time - datetime.fromtimestamp(time.time())
                    print 'Connect succeed. The time required = {0}'.format(required_time)
                    break
    except KeyboardInterrupt:
        print 'Stopped by interrupt'
        sys.exit(0)
    except Exception:
        print traceback.format_exc()
        sys.exit(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-e', nargs='?', action='store', dest='engine', required=True,
                        help="Choose NLP engine version\n[ ex) nlp1 / nlp2 / nlp3 ]")
    arguments = parser.parse_args()
    main(arguments)
