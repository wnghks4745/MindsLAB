#!/usr/bin/python
# -*- coding: utf-8 -*-

"""program"""
__author__ = "Seungphil Lee"
__email__ = "astrea@mindslab.ai"
__date__ = "creation: 2018-07-11, modification: 0000-00-00"
__copyright__ = "All Rights Reserved by MINDsLAB"

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
            in_text.text = target_text.replace('O', ' ').decode('utf-8')
            target_text = target_text.decode('utf-8')
        except Exception:
            in_text.text = unicode(target_text.replace('O', ' '), 'euc-kr')
            target_text = unicode(target_text, 'euc-kr')
        in_text.lang = lang_pb2.kor
        in_text.split_sentence = True
        in_text.use_tokenizer = False
        # in_text.use_space = False
        in_text.level = 1
        in_text.keyword_frequency_level = 0
        ret = self.stub.Analyze(in_text)
        result_list = list()
        for idx in range(len(ret.sentences)):
            nlp_word_list = [text for text in target_text]
            new_nlp_word_list = list()
            morph_word_list = list()
            analysis = ret.sentences[idx].morps
            for ana_idx in range(len(analysis)):
                morphs_word = analysis[ana_idx].lemma
                morphs_type = analysis[ana_idx].type
                if morphs_type in ['VV', 'VA', 'VX', 'VCP', 'VCN']:
                    new_nlp_word_list.append('{0}다'.format(morphs_word))
                    morph_word_list.append('{0}다/{1}'.format(morphs_word, morphs_type))
                elif ana_idx > 0 and morph_word_list[-1].split('/')[1] == 'SL' and morphs_type in ['SN', 'SW']:
                    before_word = nlp_word_list.pop()
                    morph_word_list.pop()
                    new_nlp_word_list.append('{0}{1}'.format(before_word, morphs_word))
                    morph_word_list.append('{0}{1}/NNG'.format(before_word, morphs_word))
                elif ana_idx > 0 and morph_word_list[-1].split('/')[1] == 'SN' and morphs_type in ['SL', 'SW']:
                    before_word = nlp_word_list.pop()
                    morph_word_list.pop()
                    new_nlp_word_list.append('{0}{1}'.format(before_word, morphs_word))
                    morph_word_list.append('{0}{1}/NNG'.format(before_word, morphs_word))
                elif ana_idx > 2 and morphs_type == 'SN':
                    if morph_word_list[-2].split('/')[1] == 'SN' and morph_word_list[-1].split('/')[1] == 'SP':
                        middle_word = nlp_word_list.pop()
                        head_word = nlp_word_list.pop()
                        morph_word_list.pop()
                        morph_word_list.pop()
                        new_nlp_word_list.append('{0}{1}{2}'.format(head_word, middle_word, morphs_word))
                        morph_word_list.append('{0}{1}{2}/NNG'.format(head_word, middle_word, morphs_word))
                    else:
                        new_nlp_word_list.append('{0}'.format(morphs_word))
                        morph_word_list.append('{0}/{1}'.format(morphs_word, morphs_type))
                else:
                    new_nlp_word_list.append('{0}'.format(morphs_word))
                    morph_word_list.append('{0}/{1}'.format(morphs_word, morphs_type))
            nlp_sent = ' '.join(new_nlp_word_list).encode('utf-8').strip()
            morph_sent = ' '.join(morph_word_list).encode('utf-8').strip()
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
        target_dir_path = '{0}/nlp'.format(os.path.dirname(target_file_path))
        if not os.path.exists(target_dir_path):
            os.makedirs(target_dir_path)
        target_file_name = os.path.basename(target_file_path)
        output_file = open('{0}/{1}'.format(target_dir_path, target_file_name), 'w')
        for result_list in output_list:
            line_target_text = ''
            line_nlp_sent = ''
            line_morph_sent = ''
            for target_text, nlp_sent, morph_sent in result_list:
                line_target_text += '{0} '.format(target_text)
                line_nlp_sent += '{0} '.format(nlp_sent)
                line_morph_sent += '{0} '.format(morph_sent)
            print >> output_file, '{0}\t{1}\t{2}'.format(line_target_text, line_nlp_sent, line_morph_sent)
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
                        help="Do you want output file? [ default = true ]\n[ True / False]")
    parser.add_argument('-p', nargs='?', action='store', dest='tm_print', default=False, type=bool,
                        help="Do you want print terminal? [ default = False ]\n[ True / False]")
    arguments = parser.parse_args()
    main(arguments)
