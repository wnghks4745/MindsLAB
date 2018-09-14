#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-06-19, modification: 0000-00-00"

###########
# imports #
###########
import os
import sys
import grpc
import json
from google.protobuf import empty_pb2
from google.protobuf import json_format, text_format
sys.path.append(os.path.join(os.getenv('MAUM_ROOT'), "lib/python"))
from maum.common import lang_pb2
from common.config import Config
from maum.brain.nlp import nlp_pb2
from maum.brain.nlp import nlp_pb2_grpc


#########
# class #
#########
class NlpClient(object):
    def __init__(self):
        self.conf = Config()
        remote = "localhost:{0}".format(self.conf.get('brain-ta.nlp.2.kor.port'))
        channel = grpc.insecure_channel(remote)
        self.stub = nlp_pb2_grpc.NaturalLanguageProcessingServiceStub(channel)

    def get_provider(self):
        try:
            ret = self.stub.GetProvider(empty_pb2.Empty())
        except grpc.RpcError as e:
            return str(e)
        json_ret = json_format.MessageToJson(ret, True)
        return None

    def analyze(self, text, level, keyword_level):
        in_text = nlp_pb2.InputText()
        in_text.text = text
        in_text.lang = lang_pb2.kor
        in_text.split_sentence = True
        in_text.use_tokenizer = False
        in_text.level = level
        in_text.keyword_frequency_level = keyword_level
        ret = self.stub.Analyze(in_text)
        # JSON Object 로 만들어 낸다.
        printer = json_format._Printer(True, True)
        doc = printer._MessageToJsonObject(ret)
        ret_txt = text_format.MessageToString(ret, False, False)
        # print doc
        # JSON text 로 만들어낸다.
        json_text = json_format.MessageToJson(ret, True, True)
        # print json_text
        readable_text = ''
        for idx in range(len(ret.sentences)):
            text = ret.sentences[idx].text
            analysis = ret.sentences[idx].morps
            morp = ""
            for ana_idx in range(len(analysis)):
                if analysis[ana_idx].type in ['VV', 'VA', 'VX', 'VCP']:
                    morp += " {0}다/{1}".format(analysis[ana_idx].lemma, analysis[ana_idx].type)
                else:
                    morp += " {0}/{1}".format(analysis[ana_idx].lemma, analysis[ana_idx].type)
            morp = morp.encode('utf-8').strip()
            add_morp = "morp -> {0}".format(morp)
            # print add str
            readable_text += add_morp + '\n'
            ner = ret.sentences[idx].nes
            for ner_idx in range(len(ner)):
                if ner[ner_idx].type == 'VV' or ner[ner_idx].type == 'VA' or ner[ner_idx].type == 'VX' or ner[ner_idx].type == 'VCP':
                    ne = "{0}다/{1}".format(ner[ner_idx].text, ner[ner_idx].type)
                else:
                    ne = "{0}/{1}".format(ner[ner_idx].text, ner[ner_idx].type)
                ne = ne.encode('utf-8').strip()
                add_ne = 'NE -> ' + ne
                # print add NE
                readable_text += add_ne + '\n'
        # Make nlp sentence
        json_data = json.loads(json_text)
        word_list = list()
        for sentence in json_data['sentences']:
            for words in sentence['words']:
                tagged_text = words['tagged_text']
                tagged_text_list = tagged_text.split()
                for tagged_word in tagged_text_list:
                    word = tagged_word.split("/")[0]
                    tag = tagged_word.split("/")[1]
                    if tag in ['VV', 'VA', 'VX', 'VCP', 'VCN']:
                        word += u"\ub2e4"
                    word_list.append(word)
        nlp_sent = " ".join(word_list)
        # Modify json data
        for sentence in json_data['sentences']:
            for words in sentence['words']:
                tagged_text = words['tagged_text']
                if '/VV' in tagged_text:
                    words['tagged_text'] = words['tagged_text'].replace("/VV", u"\ub2e4/VV")
                if '/VA' in tagged_text:
                    words['tagged_text'] = words['tagged_text'].replace("/VA", u"\ub2e4/VA")
                if '/VX' in tagged_text:
                    words['tagged_text'] = words['tagged_text'].replace("/VX", u"\ub2e4/VX")
                if '/VCP' in tagged_text:
                    words['tagged_text'] = words['tagged_text'].replace("/VCP", u"\ub2e4/VCP")
                if '/VCN' in tagged_text:
                    words['tagged_text'] = words['tagged_text'].replace("/VCN", u"\ub2e4/VCN")
        for sentence in json_data['sentences']:
            for morps in sentence['morps']:
                if morps['type'] == 'VV':
                    morps['lemma'] += u"\ub2e4"
                elif morps['type'] == 'VA':
                    morps['lemma'] += u"\ub2e4"
                elif morps['type'] == 'VX':
                    morps['lemma'] += u"\ub2e4"
                elif morps['type'] == 'VCP':
                    morps['lemma'] += u"\ub2e4"
                elif morps['type'] == 'VCN':
                    morps['lemma'] += u"\ub2e4"
        return nlp_sent, json.dumps(json_data), ret


#######
# def #
#######
def is_number(input_argument):
    """
    Check is number
    :param          input_argument:         Input argument
    :return                                 True or false and input argument
    """
    try:
        int(input_argument)
        return True, int(input_argument)
    except ValueError:
        return False, input_argument


def test_nlp():
    """
    Test nlp
    """
    nlp_client = NlpClient()
    nlp_client.get_provider()
    # NLP analysis level 지정 방법
    # 1. parameter 로 지정( or 변수로 할당)
    # level = 0
    # nlp_client.analyze('안녕하세요. 자연어 처리 엔진을 원격으로 호출해요.', level)
    # 2. 사용자가 입력
    while True:
        print("Usage : NLP Analysis Engine")
        print("Level 0 : All NLP Analysis With Space, Morpheme, NamedEntity, Chunk, SentimentalAnalysis")
        print("Level 1 : Only space, Morpheme")
        print("Level 2 : Level 1 + Named Entity")
        print("quit : Exit the System!!")
        input_argument = raw_input("Select NLP Analysis Level : ")
        if input_argument == "quit":
            break
        flag, level = is_number(input_argument)
        if flag:
            if level > 2 or level < 0:
                print("Select Error!!")
                print("You must type only Number range 0-2")
                continue
            else:
                print("Usage : Extraction Keyword Frequency Engine")
                print("Level 0 : Extract keyword frequency with NP, VP, NamedEntity")
                print("Level 1 : Don't extract keyword frequency with NP, VP, NamedEntity")
                keyword_select = raw_input("Select Keyword Frequency Level :")
                keyword_flag, keyword_level = is_number(keyword_select)
                if keyword_flag:
                    if keyword_level > 1 or keyword_level < 0:
                        print("Select Error!!")
                        print("You must type only Number range 0-1")
                        continue
                    elif keyword_level == 0 or keyword_level == 1:
                        nlp_client.analyze("안녕하세요. 판교에 위치한 회사입니다.", level, keyword_level)
                    else:
                        print("Select Error!!")
                        print("You must type only Number!!")
                        continue
        else:
            print("Select Error!!")
            print("You must type only Number!!")
            continue


if __name__ == '__main__':
    conf = Config()
    conf.init('brain-ta.conf')
    test_nlp()