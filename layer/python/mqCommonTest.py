import unittest
from unittest import mock
import datetime
from datetime import datetime as dt
import json
import pymysql
import rdsCommon as rds
import initCommon
import mqCommon as mq
import configparser
import sys

DB_HOST = "mysql.cpp9recuwclr.ap-northeast-1.rds.amazonaws.com"
DB_PORT = 3306
DB_USER = "admin"
DB_PASSWORD = "Yz8aBhFr"
DB_NAME = "ins001"

MQ_HOST = "eg-iot-nlb-a11512e8be0beed3.elb.ap-northeast-1.amazonaws.com"
MQ_PORT = 5671
MQ_USER = "monone_dev"
MQ_PASSWORD = "VTwJMnmyqiE2"
MQ_UT_EXCANGE = "ut.exchange"
MQ_UT_ROUTINGKEY = "ut.queue"
MQ_UT_QUEUE = "ut.queue"

MQ_CONNECT = None
LOGGER = None


def get_query(query_file_path):
    with open(query_file_path, 'r', encoding='utf-8') as f:
        query = f.read()
    return query


def createEvent(query_file_path):
    f = open(query_file_path, 'r')
    return json.load(f)

# -------------------------------------
# MQ接続共通クラスのテストクラス
# -------------------------------------
class MqCommonTest(unittest.TestCase):

    # テスト開始時に1回だけ呼び出される
    # クラスメソッドとして定義する
    @classmethod
    def setUpClass(cls):
        global MQ_CONNECT, LOGGER
        LOGGER = initCommon.getLogger("INFO")
        # MQ接続（リトライ付き）
        for num in range(1, 10):
            try:
                MQ_CONNECT = mq.mqCommon(LOGGER, MQ_HOST, MQ_PORT, MQ_USER, MQ_PASSWORD)
                break
            except Exception as ex:
                print("***** mq connect retry [%d]" % num)
        
            
    @classmethod
    def tearDownClass(cls):
        global MQ_CONNECT
        # MQ切断
        print ("***** mq close")
        del MQ_CONNECT
        
    # テストメソッドを実行するたびに呼ばれる
    def setUp(self):
        pass
 
    # テストメソッドの実行が終わるたびに呼ばれる
    def tearDown(self):
        pass
    
    # ------------------------------------------------------
    # キューの中身を全て取得してキューが空になること。
    # ------------------------------------------------------
    def test_getQueueMessage_001(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)

        # キューの中身をクリア
        MQ_CONNECT.getQueueMessage(MQ_UT_QUEUE)
       
        # テストメッセージ送信
        MQ_CONNECT.publishExchange(MQ_UT_EXCANGE, MQ_UT_ROUTINGKEY, get_query("test/mq/newError001.json"))
        MQ_CONNECT.publishExchange(MQ_UT_EXCANGE, MQ_UT_ROUTINGKEY, get_query("test/mq/newError002.json"))
        
        # 全メッセージ取得
        resultArray = MQ_CONNECT.getQueueMessage(MQ_UT_QUEUE)
        for result in resultArray:
            print(result)
            
        
    # ------------------------------------------------------
    # TENANT_ID:2のみキューを取得すること。
    # ------------------------------------------------------
    def test_getQueueMessage_002(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)

        # キューの中身をクリア
        MQ_CONNECT.getQueueMessage(MQ_UT_QUEUE)
       
        # テストメッセージ送信
        MQ_CONNECT.publishExchange(MQ_UT_EXCANGE, MQ_UT_ROUTINGKEY, get_query("test/mq/newError001.json"))
        MQ_CONNECT.publishExchange(MQ_UT_EXCANGE, MQ_UT_ROUTINGKEY, get_query("test/mq/newError002.json"))
        
        # 一部メッセージだけ取得
        resultArray = MQ_CONNECT.getQueueMessage(MQ_UT_QUEUE, ["2"])
        for result in resultArray:
            print(result)

       
    # ------------------------------------------------------
    # TENANT_IDが存在しない場合は無視されること。
    # ------------------------------------------------------
    def test_getQueueMessage_003(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)

        # キューの中身をクリア
        MQ_CONNECT.getQueueMessage(MQ_UT_QUEUE)
       
        # テストメッセージ送信
        MQ_CONNECT.publishExchange(MQ_UT_EXCANGE, MQ_UT_ROUTINGKEY, get_query("test/mq/newError001.json"))
        MQ_CONNECT.publishExchange(MQ_UT_EXCANGE, MQ_UT_ROUTINGKEY, get_query("test/mq/newError002.json"))
        MQ_CONNECT.publishExchange(MQ_UT_EXCANGE, MQ_UT_ROUTINGKEY, get_query("test/mq/newError003.json"))
        
        # 一部メッセージだけ取得
        resultArray = MQ_CONNECT.getQueueMessage(MQ_UT_QUEUE, ["2"])
        for result in resultArray:
            print(result)
            
    # ------------------------------------------------------
    # Json形式でない場合は無視されること。
    # ------------------------------------------------------
    def test_getQueueMessage_004(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)

        # キューの中身をクリア
        MQ_CONNECT.getQueueMessage(MQ_UT_QUEUE)
       
        # テストメッセージ送信
        MQ_CONNECT.publishExchange(MQ_UT_EXCANGE, MQ_UT_ROUTINGKEY, get_query("test/mq/newError001.json"))
        MQ_CONNECT.publishExchange(MQ_UT_EXCANGE, MQ_UT_ROUTINGKEY, get_query("test/mq/newError002.json"))
        MQ_CONNECT.publishExchange(MQ_UT_EXCANGE, MQ_UT_ROUTINGKEY, "平文テスト123")
        
        # 一部メッセージだけ取得
        resultArray = MQ_CONNECT.getQueueMessage(MQ_UT_QUEUE, ["2"])
        for result in resultArray:
            print(result)
