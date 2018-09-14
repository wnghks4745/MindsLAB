#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-05-30, modification: 2018-05-30"

from ctypes import *
import argparse
import os
import shutil

LIB_FILE_PATH = '/app/prd/MindsVOC/damo/cfg/64/libscpdb_agent.so'
INI_FILE_PATH = '/app/prd/MindsVOC/damo/cfg/scpdb_agent.ini'
KEY = 'REC_KEY'
clib = cdll.LoadLibrary(LIB_FILE_PATH)


def scp_enc_file(file_path, enc_file_path):
    """
    # file Encrypt
    :param file_path:               암호화 할 대상 파일경로
    :param enc_file_path:           암호화 한 파일경로
    :return:                        0 : success , 0 != ret : fail
    """
    temp_file_name = enc_file_path+'.temp'
    ret = clib.SCP_EncFile(INI_FILE_PATH, KEY, file_path, temp_file_name)
    if ret == 0:
        shutil.move(temp_file_name, enc_file_path)
    return ret


def scp_dec_file(enc_file_path, dec_file_path):
    """
    # file Decrypt
    :param enc_file_path:           암호화 할 대상 파일경로
    :param dec_file_path:           암호화 한 파일경로
    :return:                        0 : success , 0 != ret : fail
    """
    temp_file_name = dec_file_path + '.temp'
    ret = clib.SCP_DecFile(INI_FILE_PATH, KEY, enc_file_path, temp_file_name)
    if ret == 0:
        shutil.move(temp_file_name, dec_file_path)
    return ret


def scp_enc_str(str):
    """
    text Encrypt
    :param str:                     암호화 할 대상 텍스트
    :return:                        enc str
    """
    enc = create_string_buffer(128)
    enc_len = c_int()
    plain = create_string_buffer(str)
    print INI_FILE_PATH
    ret = clib.SCP_EncStr(INI_FILE_PATH, KEY, plain, len(plain.value), enc, byref(enc_len), sizeof(enc))
    if ret != 0:
        return ret
    else:
        return repr(enc.value)


def scp_dec_str(str):
    """
    text Decrypt
    :param str:                     암호화 된 텍스트
    :return:                        dec str
    """
    dec = create_string_buffer(128)
    dec_len = c_int()
    ret = clib.SCP_DecStr(INI_FILE_PATH, KEY, str, len(str), dec, byref(dec_len), sizeof(dec))
    if ret != 0:
        return ret
    else:
        return repr(dec.value)


def scp_enc_base64_str(str):
    """
    text base64 Encrypt
    :param str:                     암호화 할 대상 텍스트
    :return:                        enc str
    """
    enc = create_string_buffer(128)
    enc_len = c_int()

    plain = create_string_buffer(str)
    ret = clib.SCP_EncB64(INI_FILE_PATH, KEY, plain, len(plain.value), enc, byref(enc_len), sizeof(enc))
    if ret != 0:
        return ret
    else:
        return repr(enc.value)


def scp_dec_base64_str(str):
    """
    text base64 Decrypt
    :param str:                     암호화 된 텍스트
    :return:                        dec str
    """
    dec = create_string_buffer(128)
    dec_len = c_int()
    ret = clib.SCP_DecB64(INI_FILE_PATH, KEY, str, len(str), dec, byref(dec_len), sizeof(dec))
    if ret != 0:
        return ret
    else:
        return repr(dec.value)


def dir_scp_enc_file(file_path):
    """
    directory all file Encrypt
    :param file_path:
    :return:
    """
    w_ob = os.walk(file_path)
    is_success = True
    for dir_path, sub_dirs, files in w_ob:
        for file_name in files:
            if file_name.endswith('.enc'):
                continue
            file_path = os.path.join(dir_path, file_name)
            out_file = "{0}.enc".format(file_path)
            ret = scp_enc_file(file_path, out_file)
            if ret == 0:
                os.remove(file_path)
            else:
                is_success = False
    return is_success


def dir_scp_dec_file(file_path):
    """
    directory all file Decrypt
    :param file_path:
    :return:
    """
    w_ob = os.walk(file_path)
    is_success = True
    for dir_path, sub_dirs, files in w_ob:
        for file_name in files:
            if file_name.endswith('.enc'):
                file_path = os.path.join(dir_path, file_name)
                out_file = "{0}/{1}".format(dir_path, file_name.replace('.enc', ''))
                ret = scp_dec_file(file_path, out_file)
                if ret == 0:
                    os.remove(file_path)
                else:
                    is_success = False
    return is_success


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-type', action='store', dest='type', type=str, help='file or str or base64'
                        , required=True, choices=['file', 'dir', 'str', 'base64'])
    parser.add_argument('-p', action='store', dest='process', type=str, help='enc or dec'
                        , required=True, choices=['enc', 'dec'])
    parser.add_argument('-data', action='store', dest='data', type=str, help='str or file path or file path with file name'
                        , required=True, nargs=argparse.REMAINDER)
    arguments = parser.parse_args()
    if arguments.type == 'file':
        if not os.path.exists(arguments.data[0]):
            raise Exception("file not found")
        if len(arguments.data) != 2:
            raise Exception("required two file path")
        if arguments.process == 'enc':
            ret = scp_enc_file(arguments.data[0], arguments.data[1])
            if ret == 0:
                print True
        elif arguments.process == 'dec':
            ret = scp_dec_file(arguments.data[0], arguments.data[1])
            if ret == 0:
                print True
    elif arguments.type == 'dir':
        if not os.path.exists(arguments.data[0]):
            raise Exception("path not found")
        if len(arguments.data) != 1:
            raise Exception("required file path")
        if arguments.process == 'enc':
            print dir_scp_enc_file(arguments.data[0])
        elif arguments.process == 'dec':
            print dir_scp_dec_file(arguments.data[0])
    elif arguments.type == 'str':
        if arguments.process == 'enc':
            print scp_enc_str(arguments.data[0])
        elif arguments.process == 'dec':
            print scp_dec_str(arguments.data[0])
    elif arguments.type == 'base64':
        if arguments.process == 'enc':
            print scp_enc_base64_str(arguments.data[0])
        elif arguments.process == 'dec':
            print scp_dec_base64_str(arguments.data[0])