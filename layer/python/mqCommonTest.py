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


# -------------------------------------
# MQ接続共通クラスのテストクラス
# -------------------------------------
class MqCommonTest(unittest.TestCase):

    MQ_CONNECT = None
    LOGGER = None
    MQ_NLB_HOST = "rabbitmq.eg-iot-develop.com"
    MQ_PORT = 5671
    MQ_USER = "monone_dev"
    MQ_PASSWORD = "VTwJMnmyqiE2"
    MQ_UT_EXCANGE = "ut.exchange"
    MQ_UT_ROUTINGKEY = "ut.queue"
    MQ_UT_QUEUE = "ut.queue"
    
    # テスト開始時に1回だけ呼び出される
    # クラスメソッドとして定義する
    @classmethod
    def setUpClass(self):
        self.LOGGER = initCommon.getLogger("INFO")
        # MQ接続（リトライ付き）
        for num in range(1, 10):
            try:
                self.MQ_CONNECT = mq.mqCommon(self.LOGGER, self.MQ_NLB_HOST, self.MQ_PORT, self.MQ_USER, self.MQ_PASSWORD)
                break
            except Exception as ex:
                print("***** mq connect retry [%d]" % num)
        
            
    @classmethod
    def tearDownClass(self):
        # global MQ_CONNECT
        # MQ切断
        print ("***** mq close")
        del self.MQ_CONNECT
        
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
        self.MQ_CONNECT.getQueueMessage(queueName=self.MQ_UT_QUEUE, isErrDel=True)
       
        # テストメッセージ送信
        self.MQ_CONNECT.publishExchange(self.MQ_UT_EXCANGE, self.MQ_UT_ROUTINGKEY, initCommon.getQuery("test/mq/newError001.json"))
        self.MQ_CONNECT.publishExchange(self.MQ_UT_EXCANGE, self.MQ_UT_ROUTINGKEY, initCommon.getQuery("test/mq/newError002.json"))
        
        # 全メッセージ取得
        resultArray = self.MQ_CONNECT.getQueueMessage(queueName=self.MQ_UT_QUEUE)
        
        # 検証
        isCheck1 = False
        isCheck2 = False
        for result in resultArray:
            print(result)
            
            if result["tenantId"] in ["1"]:
                self.assertEqual(result["tenantId"], "1")
                isCheck1 = True
            
            if result["tenantId"] in ["2"]:
                self.assertEqual(result["tenantId"], "2")
                isCheck2 = True
        self.assertTrue(isCheck1 and isCheck2)
            
    
    # ------------------------------------------------------
    # TENANT_ID:2のみキューを取得すること。
    # ------------------------------------------------------
    def test_getQueueMessage_002(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)
    
        # キューの中身をクリア
        self.MQ_CONNECT.getQueueMessage(queueName=self.MQ_UT_QUEUE, isErrDel=True)
    
        # テストメッセージ送信
        self.MQ_CONNECT.publishExchange(self.MQ_UT_EXCANGE, self.MQ_UT_ROUTINGKEY, initCommon.getQuery("test/mq/newError001.json"))
        self.MQ_CONNECT.publishExchange(self.MQ_UT_EXCANGE, self.MQ_UT_ROUTINGKEY, initCommon.getQuery("test/mq/newError002.json"))
    
        # 一部メッセージだけ取得
        resultArray = self.MQ_CONNECT.getQueueMessage(queueName=self.MQ_UT_QUEUE, idArray=["2"])
        
        # 検証
        isCheck1 = False
        isCheck2 = False
        for result in resultArray:
            print(result)
            
            if result["tenantId"] in ["1"]:
                self.assertEqual(result["tenantId"], "1")
                isCheck1 = True
            if result["tenantId"] in ["2"]:
                isCheck2 = True
        self.assertTrue(isCheck1==False and isCheck2)
    
    
    # ------------------------------------------------------
    # TENANT_IDが存在しない場合は無視されること。
    # ------------------------------------------------------
    def test_getQueueMessage_003(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)
    
        # キューの中身をクリア
        self.MQ_CONNECT.getQueueMessage(queueName=self.MQ_UT_QUEUE, isErrDel=True)
    
        # テストメッセージ送信
        self.MQ_CONNECT.publishExchange(self.MQ_UT_EXCANGE, self.MQ_UT_ROUTINGKEY, initCommon.getQuery("test/mq/newError001.json"))
        self.MQ_CONNECT.publishExchange(self.MQ_UT_EXCANGE, self.MQ_UT_ROUTINGKEY, initCommon.getQuery("test/mq/newError002.json"))
        self.MQ_CONNECT.publishExchange(self.MQ_UT_EXCANGE, self.MQ_UT_ROUTINGKEY, initCommon.getQuery("test/mq/newError003.json"))
    
        # 一部メッセージだけ取得
        resultArray = self.MQ_CONNECT.getQueueMessage(queueName=self.MQ_UT_QUEUE, idArray=["2"])
        for result in resultArray:
            print(result)
        self.assertEqual(len(resultArray), 1)
    
    # ------------------------------------------------------
    # Json形式でない場合は無視されること。
    # ------------------------------------------------------
    def test_getQueueMessage_004(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)
    
        # キューの中身をクリア
        self.MQ_CONNECT.getQueueMessage(queueName=self.MQ_UT_QUEUE, isErrDel=True)
    
        # テストメッセージ送信
        self.MQ_CONNECT.publishExchange(self.MQ_UT_EXCANGE, self.MQ_UT_ROUTINGKEY, initCommon.getQuery("test/mq/newError001.json"))
        self.MQ_CONNECT.publishExchange(self.MQ_UT_EXCANGE, self.MQ_UT_ROUTINGKEY, initCommon.getQuery("test/mq/newError002.json"))
        self.MQ_CONNECT.publishExchange(self.MQ_UT_EXCANGE, self.MQ_UT_ROUTINGKEY, "平文テスト123")
    
        # 一部メッセージだけ取得
        resultArray = self.MQ_CONNECT.getQueueMessage(queueName=self.MQ_UT_QUEUE, idArray=["2"])
        for result in resultArray:
            print(result)
        self.assertEqual(len(resultArray), 1)
