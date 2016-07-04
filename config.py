#!/usr/bin/env python
#coding:utf-8
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
#import settings


class PLAN_SETTINGS(object):
    DEBUG = False

    all_login_status = set([1, 2])
    #DISCOUNT = 0.97
    DISCOUNT = 0.95
    CPC_DISCOUNT = 0.99
    OTHER_AREA_ID = 440
    OTHER_AREA_GUID = '74412922'
    ROOT_AREA_ID = 1
    ROOT_AREA_GUID = '1f470dc1'
    ROOT_AREA_TYPE = 99
    PSUEDO_FACTORY_ID = 0
    PSUEDO_FACTORY_COUNT = 9999999999.0
    NON_ACTIVE_USER = 0
    DEFAULT_CLIENT_ID = -1
    DEFAULT_TAG_ID = -1
    DEFAULT_AD_TAG_ID = -2

    DEBUG_FILE = 'debug_'

    HOST = os.listdir('/mfs/log/access-log/current/ereborlog/')
    EREBOR_PATH = '/mfs/log/access-log/current/ereborlog/%s/%s'

    @classmethod
    def set_debug(cls, debug):
        cls.DEBUG = debug
        if cls.DEBUG:
            cls.DATA_PATH = '/home2/songsiyu/data/'
        else:
            cls.DATA_PATH = '/mfs/user/erebor/data/adp/production/'

PLAN_SETTINGS.set_debug(True)


import MySQLdb
class MarketStore(object):
    def __init__(self):
        self.conn = MySQLdb.connect(host='dae_b', port=3320, db='market', user='market', passwd='KAAjHBw6im')
        self.cursor = self.conn.cursor()

    def execute(self, sql):
        self.cursor.execute(sql)
        return self.cursor.fetchall()


import logging
FORMAT = '%(asctime)s %(filename)s[%(lineno)s]: %(message)s'
logging.basicConfig(format=FORMAT, level=logging.INFO)
logger = logging.getLogger('glorfindel_logger')

REDIS_TEST_HOST = "rowan4d"
REDIS_TEST_PORT = 60000
