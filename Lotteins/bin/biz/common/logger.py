#!/usr/bin/python
# -*- coding: utf-8 -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-05-07, modification: 0000-00-00"

###########
# imports #
###########
import os
import sys
import logging
import logging.handlers

#######
# def #
#######
def get_timed_rotating_logger(**kwargs):
    """
    Get timed rotating logger
    :param          kwargs:             Arguments
    :return:                            Logger
    """
    # create logger
    logger = logging.getLogger(kwargs.get('logger_name'))
    if kwargs.get('log_level').lower() == 'info':
        log_level = 20
    elif kwargs.get('log_level').lower() == 'warning':
        log_level = 30
    elif kwargs.get('log_level').lower() == 'error':
        log_level = 40
    elif kwargs.get('log_level').lower() == 'critical':
        log_level = 50
    else:
        log_level = 10
    logger.setLevel(log_level)
    ch = logging.handlers.TimedRotatingFileHandler(
        kwargs.get('file_name'),
        when='midnight',
        interval=1,
        backupCount=kwargs.get('backup_count'),
        encoding=None,
        delay=False,
        utc=False
    )
    ch.setLevel(log_level)
    # Create formatter
    formatter = logging.Formatter(
        fmt='%(asctime)s.%(msecs)03d - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    # Add formatter to ch
    ch.setFormatter(formatter)
    # Add ch to logger
    logger.addHandler(ch)
    st = logging.StreamHandler(sys.stdout)
    st.setFormatter(formatter)
    st.setLevel(logging.DEBUG)
    logger.addHandler(st)
    return logger


def get_logger(logger_name):
    """
    Get logger
    :param          logger_name:        Logger name
    :return:                            Logger
    """
    # Create logger
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    # Create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    # Create formatter
    formatter = logging.Formatter(
        fmt='%(asctime)s.%(msecs)03d - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    # Add formatter to ch
    ch.setFormatter(formatter)
    # Add ch to logger
    logger.addHandler(ch)
    return logger
