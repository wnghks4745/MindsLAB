#!/usr/bin/python
# -*- coding: utf-8 -*-

"""program"""
__author__ = "Seungphil Lee"
__email__ = "astraea@mindslab.ai"
__date__ = "creation: 2018-07-11, modification: 0000-00-00"
__copyright__ = 'All Rights Reserved by MINDsLAB'

###########
# imports #
###########
import os
import sys
import grpc
import json
import time
import pprint
import argparse
import traceback
from datetime import datetime
from google.protobuf import json_format
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
class JsonPrinter(pprint.PrettyPrinter):
    def format(self, _object, context, max_levels, level):
        if isinstance(_object, unicode):
            return "'%s'" % _object.encode('utf8'), True, False
        elif isinstance(_object, str):
            _object = unicode(_object, 'utf8')
            return "'%s'" % _object.encode('utf8'), True, False
        return pprint.PrettyPrinter.format(self, _object, context, max_levels, level)


class NlpClient(object):
    def __init__(self, args):
        self.args = args
        self.conf = Config()
        self.json_printer = JsonPrinter()
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
            target_text = unicode(target_text, 'euc-kr').encode('utf-8')
            in_text.text = target_text
        in_text.lang = lang_pb2.kor
        in_text.split_sentence = True
        in_text.use_tokenizer = False
        in_text.use_space = self.args.space
        in_text.level = 0
        in_text.keyword_frequency_level = 0
        ret = self.stub.Analyze(in_text)
        # Result to Json format
#        json_text = json_format.MessageToJson(ret, True, True)
#        data = json.loads(json_text)
#        self.json_printer.pprint(data)
        result_list = list()
        for idx in range(len(ret.sentences)):
            nlp_word = str()
            morph_word = str()
#            text = ret.sentences[idx].text
            analysis = ret.sentences[idx].morps
            for ana_idx in range(len(analysis)):
                if analysis[ana_idx].type in ['VV', 'VA', 'VX', 'VCP', 'VCN']:
                    nlp_word += ' {0}다'.format(analysis[ana_idx].lemma)
                    morph_word += ' {0}다/{1}'.format(analysis[ana_idx].lemma, analysis[ana_idx].type)
                else:
                    nlp_word += ' {0}'.format(analysis[ana_idx].lemma)
                    morph_word += ' {0}/{1}'.format(analysis[ana_idx].lemma, analysis[ana_idx].type)
            nlp_sent = nlp_word.encode('utf-8').strip()
            morph_sent = morph_word.encode('utf-8').strip()
            result_list.append((target_text, nlp_sent, morph_sent))
        return result_list


#######
# def #
#######
def elapsed_time(sdate):
    """
    elapsed time
    :param      sdate:      date object
    :return:                Required time (type : datetime)
    """
    end_time = datetime.now()
    if not sdate or len(sdate) < 14:
        return 0, 0, 0, 0
    start_time = datetime(int(sdate[:4]), int(sdate[4:6]), int(sdate[6:8]),
                          int(sdate[8:10]), int(sdate[10:12]), int(sdate[12:14]))
    required_time = end_time - start_time
    return required_time


def execute_file_analyze(args, nlp_client, target_file_path):
    """
    Execute file NLP analyze
    :param      args:                   Arguments
    :param      nlp_client:             NLP client object
    :param      target_file_path:       Target file absolute path
    """
    output_list = list()
    with open(target_file_path) as target_file:
        for line in target_file:
            line = line.strip()
            try:
                result_list = nlp_client.analyze(line)
            except Exception:
                print "[ERROR] Can't analyze line"
                print "File path --> ", target_file_path
                print "Line --> ", line
                print traceback.format_exc()
                continue
            if args.tm_print:
                for target_text, nlp_sent, morph_sent in result_list:
                    print "{0}\n--> {1}\n--> {2}".format(target_text, nlp_sent, morph_sent)
            output_list.append(result_list)
    if args.output:
        output_file = open('{0}_nlp'.format(target_file_path), 'w')
        for result_list in output_list:
            for target_text, nlp_sent, morph_sent in result_list:
                print >> output_file, '{0}\t{1}\t{2}'.format(target_text, nlp_sent, morph_sent)
        output_file.close()


def main(args):
    """
    This program that execute NLP
    :param      args:       Arguments
    """
    try:
        ts = time.time()
        st = datetime.fromtimestamp(ts).strftime('%Y-%m-%d-%H:%M.%S')
        dt = datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S')
        nlp_client = NlpClient(args)
        if args.text:
            result_list = nlp_client.analyze(args.text)
            for target_text, nlp_sent, morph_sent in result_list:
                print "{0}\n--> {1}\n--> {2}".format(target_text, nlp_sent, morph_sent)
        if args.file_path:
            target_file_path = os.path.abspath(args.file_path)
            if os.path.exists(target_file_path):
                execute_file_analyze(args, nlp_client, target_file_path)
            else:
                print "[ERROR] Can't find {0} file".format(args.file_path)
        if args.dir_path:
            target_dir_path = os.path.abspath(args.dir_path)
            if os.path.exists(target_dir_path):
                for dir_path, sub_dirs, files in os.walk(target_dir_path):
                    for file_name in files:
                        execute_file_analyze(args, nlp_client, os.path.join(dir_path, file_name))
            else:
                print "[ERROR] Can't find {0} directory".format(args.dir_path)
        print "END.. Start time = {0}, The time required = {1}".format(st, elapsed_time(dt))
    except Exception:
        print traceback.format_exc()
        sys.exit(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-e', nargs='?', action='store', dest='engine', required=True, type=str,
                        help="Choose NLP engine version\n[ ex) nlp1 / nlp2 / nlp3 ]")
    parser.add_argument('-s', nargs='?', action='store', dest='space', default=False, type=bool,
                        help="Optional use space [ default = False ]\n[ True / False ]")
    parser.add_argument('-t', nargs='?', action='store', dest='text', type=str,
                        help="Input target text\n[ ex) '[A]드라마 다시보기 결제하다' ]")
    parser.add_argument('-f', nargs='?', action='store', dest='file_path', type=str,
                        help="Input target file path\n[ ex) /app/maum/test/test.txt ]")
    parser.add_argument('-d', nargs='?', action='store', dest='dir_path', type=str,
                        help="Input target directory path\n[ ex) /app/maum/test ]")
    parser.add_argument('-o', nargs='?', action='store', dest='output', default=True, type=bool,
                        help="Do you want output file? [ default = True ]\n[ True / False]")
    parser.add_argument('-p', nargs='?', action='store', dest='tm_print', default=False, type=bool,
                        help="Do you want print terminal? [ default = False ]\n[ True / False]")
    arguments = parser.parse_args()
    main(arguments)
