import unittest
import datetime
from datetime import datetime as dt
import json
from collections import OrderedDict
import pprint
import lambda_function
import pymysql
import initCommon  # カスタムレイヤー
import rdsCommon  # カスタムレイヤー
import mqCommon  # カスタムレイヤー
import sys
from unittest import mock

class AmqpConsumerTest(unittest.TestCase):

    UT_RDS = None
    UT_MQ_HOST = "rabbitmq.eg-iot-develop.com"

    # テスト開始時に1回だけ呼び出される
    # クラスメソッドとして定義する
    @classmethod
    def setUpClass(self):
        lambda_function.initConfig("eg-iot-develop")
        lambda_function.setLogger(initCommon.getLogger(lambda_function.LOG_LEVEL))
        self.UT_RDS = rdsCommon.rdsCommon(lambda_function.LOGGER
                                , lambda_function.DB_HOST
                                , lambda_function.DB_PORT
                                , lambda_function.DB_USER
                                , lambda_function.DB_PASSWORD
                                , lambda_function.DB_NAME
                                , lambda_function.DB_CONNECT_TIMEOUT)

    @classmethod
    def tearDownClass(self):
        # RDS切断
        del self.UT_RDS
        
    
    # ----------------------------------------------------------------------
    # 全キューが空の場合、空配列のreceivedMessagesが返却されること。
    # ----------------------------------------------------------------------
    def test_lambda_handler_001(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)

        # 起動パラメータ
        event = initCommon.readFileToJson('test/function/input001.json')

        # テストデータ用に接続
        utMqConnect = mqCommon.mqCommon(lambda_function.LOGGER
                                      , self.UT_MQ_HOST
                                      , lambda_function.MQ_PORT
                                      , lambda_function.MQ_USER
                                      , lambda_function.MQ_PASSWORD)
        
        # キューを全てクリア
        for queueName in utMqConnect.getUpQueueArray():
            utMqConnect.getQueueMessage(queueName=queueName, isErrDel=True)
        
        del utMqConnect

        # mock使用:setterを無効にしてMQのエンドポイントをNLB経由に変更する
        resultDict = {}
        with mock.patch("lambda_function.setMqHost") as mk:
            print(mk.call_args_list)
            lambda_function.MQ_HOST = self.UT_MQ_HOST
            resultDict = lambda_function.lambda_handler(event, None)
        
        # 検証
        print("------------- result -------------")
        print(resultDict)    
        self.assertEqual(resultDict["clientName"], "eg-iot-develop")
        self.assertEqual(len(resultDict["receivedMessages"]), 0)
        
        
    # ----------------------------------------------------------------------
    # キューが一つずつの場合、キュー毎に戻り値が返却されること。
    # ----------------------------------------------------------------------
    def test_lambda_handler_002(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)

        # 起動パラメータ
        event = initCommon.readFileToJson('test/function/input001.json')

        # テストデータ用に接続
        utMqConnect = mqCommon.mqCommon(lambda_function.LOGGER
                                      , self.UT_MQ_HOST
                                      , lambda_function.MQ_PORT
                                      , lambda_function.MQ_USER
                                      , lambda_function.MQ_PASSWORD)
        
        # キューを全てクリア
        for queueName in utMqConnect.getUpQueueArray():
            utMqConnect.getQueueMessage(queueName=queueName, isErrDel=True)
        
        # テストデータ送信
        utMqConnect.publishExchange(exchangeName=utMqConnect.getExchangesUpName()
                                    , routingKey="Last_Score"
                                    , bodyMessage = initCommon.getQuery("test/mq/Last_Score_001.json"))
        utMqConnect.publishExchange(exchangeName=utMqConnect.getExchangesUpName()
                                    , routingKey="New_Error"
                                    , bodyMessage = initCommon.getQuery("test/mq/New_Error_001.json"))        
        utMqConnect.publishExchange(exchangeName=utMqConnect.getExchangesUpName()
                                    , routingKey="Restart_AI"
                                    , bodyMessage = initCommon.getQuery("test/mq/Restart_AI_001.json"))        
        utMqConnect.publishExchange(exchangeName=utMqConnect.getExchangesUpName()
                                    , routingKey="Restart_Edge_Failed"
                                    , bodyMessage = initCommon.getQuery("test/mq/Restart_Edge_Failed_001.json"))        
        utMqConnect.publishExchange(exchangeName=utMqConnect.getExchangesUpName()
                                    , routingKey="Transfer_Current_Sound"
                                    , bodyMessage = initCommon.getQuery("test/mq/Transfer_Current_Sound_001.json"))        
        del utMqConnect

        # mock使用:setterを無効にしてMQのエンドポイントをNLB経由に変更する
        resultDict = {}
        with mock.patch("lambda_function.setMqHost") as mk:
            print(mk.call_args_list)
            lambda_function.MQ_HOST = self.UT_MQ_HOST
            resultDict = lambda_function.lambda_handler(event, None)
        
        # 検証
        print("------------- result -------------")
        print(resultDict)    
        self.assertEqual(resultDict["clientName"], "eg-iot-develop")
        self.assertEqual(len(resultDict["receivedMessages"]), 5)
        self.assertEqual(resultDict["receivedMessages"][0]["queue"], "Last_Score")
        self.assertEqual(resultDict["receivedMessages"][0]["deviceId"], "1")
        self.assertEqual(resultDict["receivedMessages"][0]["records"][0]["tenantId"], "1")
        self.assertEqual(resultDict["receivedMessages"][1]["queue"], "New_Error")
        self.assertEqual(resultDict["receivedMessages"][1]["deviceId"], "1")
        self.assertEqual(resultDict["receivedMessages"][1]["records"][0]["tenantId"], "1")
        self.assertEqual(resultDict["receivedMessages"][2]["queue"], "Restart_AI")
        self.assertEqual(resultDict["receivedMessages"][2]["deviceId"], "1")
        self.assertEqual(resultDict["receivedMessages"][2]["records"][0]["tenantId"], "1")
        
        
    # ----------------------------------------------------------------------
    # キューが複数の場合、キューとTENANT_ID毎に戻り値が返却されること。
    # ----------------------------------------------------------------------
    def test_lambda_handler_003(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)

        # 起動パラメータ
        event = initCommon.readFileToJson('test/function/input001.json')

        # テストデータ用に接続
        utMqConnect = mqCommon.mqCommon(lambda_function.LOGGER
                                      , self.UT_MQ_HOST
                                      , lambda_function.MQ_PORT
                                      , lambda_function.MQ_USER
                                      , lambda_function.MQ_PASSWORD)
        
        # キューを全てクリア
        for queueName in utMqConnect.getUpQueueArray():
            utMqConnect.getQueueMessage(queueName=queueName, isErrDel=True)
        
        # テストデータ送信
        utMqConnect.publishExchange(exchangeName=utMqConnect.getExchangesUpName()
                                    , routingKey="Last_Score"
                                    , bodyMessage = initCommon.getQuery("test/mq/Last_Score_001.json"))
        utMqConnect.publishExchange(exchangeName=utMqConnect.getExchangesUpName()
                                    , routingKey="Last_Score"
                                    , bodyMessage = initCommon.getQuery("test/mq/Last_Score_002.json"))
        utMqConnect.publishExchange(exchangeName=utMqConnect.getExchangesUpName()
                                    , routingKey="Last_Score"
                                    , bodyMessage = initCommon.getQuery("test/mq/Last_Score_003.json"))
        utMqConnect.publishExchange(exchangeName=utMqConnect.getExchangesUpName()
                                    , routingKey="New_Error"
                                    , bodyMessage = initCommon.getQuery("test/mq/New_Error_001.json"))
        utMqConnect.publishExchange(exchangeName=utMqConnect.getExchangesUpName()
                                    , routingKey="New_Error"
                                    , bodyMessage = initCommon.getQuery("test/mq/New_Error_002.json"))
        utMqConnect.publishExchange(exchangeName=utMqConnect.getExchangesUpName()
                                    , routingKey="New_Error"
                                    , bodyMessage = initCommon.getQuery("test/mq/New_Error_003.json"))
        del utMqConnect

        # mock使用:setterを無効にしてMQのエンドポイントをNLB経由に変更する
        resultDict = {}
        with mock.patch("lambda_function.setMqHost") as mk:
            print(mk.call_args_list)
            lambda_function.MQ_HOST = self.UT_MQ_HOST
            resultDict = lambda_function.lambda_handler(event, None)
        
        # 検証
        print("------------- result -------------")
        print(resultDict)    
        self.assertEqual(resultDict["clientName"], "eg-iot-develop")
        self.assertEqual(len(resultDict["receivedMessages"]), 4)
        self.assertEqual(resultDict["receivedMessages"][0]["queue"], "Last_Score")
        self.assertEqual(resultDict["receivedMessages"][0]["deviceId"], "1")
        self.assertEqual(resultDict["receivedMessages"][0]["records"][0]["tenantId"], "1")
        self.assertEqual(resultDict["receivedMessages"][0]["records"][1]["tenantId"], "1")
        self.assertEqual(resultDict["receivedMessages"][1]["queue"], "Last_Score")
        self.assertEqual(resultDict["receivedMessages"][1]["deviceId"], "2")
        self.assertEqual(resultDict["receivedMessages"][1]["records"][0]["tenantId"], "2")
        self.assertEqual(resultDict["receivedMessages"][2]["queue"], "New_Error")
        self.assertEqual(resultDict["receivedMessages"][2]["deviceId"], "1")
        self.assertEqual(resultDict["receivedMessages"][2]["records"][0]["tenantId"], "1")
        self.assertEqual(resultDict["receivedMessages"][2]["records"][1]["tenantId"], "1")
        self.assertEqual(resultDict["receivedMessages"][3]["queue"], "New_Error")
        self.assertEqual(resultDict["receivedMessages"][3]["deviceId"], "2")
        self.assertEqual(resultDict["receivedMessages"][3]["records"][0]["tenantId"], "2")

