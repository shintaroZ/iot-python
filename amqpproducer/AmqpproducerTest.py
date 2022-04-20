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


class AmqpproducerTest(unittest.TestCase):

    UT_MQ_HOST = "rabbitmq.eg-iot-develop.com"

    # テスト開始時に1回だけ呼び出される
    # クラスメソッドとして定義する
    @classmethod
    def setUpClass(self):
        lambda_function.initConfig("eg-iot-develop")
        lambda_function.setLogger(initCommon.getLogger(lambda_function.LOG_LEVEL))
    
        # 検証用にMQ接続＋キュー取得
        MqConnect = mqCommon.mqCommon(lambda_function.LOGGER
                                      , self.UT_MQ_HOST
                                      , lambda_function.MQ_PORT
                                      , lambda_function.MQ_USER
                                      , lambda_function.MQ_PASSWORD)
        
        # キューを全てクリア
        resultMsg = MqConnect.getQueueMessage(queueName="Queue_To_Tenant_1"
                                              , isErrDel=True)
        print(resultMsg)
        
        
        del MqConnect
        

    @classmethod
    def tearDownClass(self):
        pass
    
    # ----------------------------------------------------------------------
    # パラメータありのMQ送信に成功した場合、送信先キューに登録されること。
    # ----------------------------------------------------------------------
    def test_lambda_handler_001(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)
    
        # 起動パラメータ
        event = initCommon.readFileToJson('test/function/input005.json')
    
        # mock使用:setterを無効にしてMQのエンドポイントをNLB経由に変更する
        resultDict = {}
        with mock.patch("lambda_function.setMqHost") as mk:
            print(mk.call_args_list)
            lambda_function.MQ_HOST = self.UT_MQ_HOST
            resultDict = lambda_function.lambda_handler(event, None)
    
        # 検証用にMQ接続＋キュー取得
        utMqConnect = mqCommon.mqCommon(lambda_function.LOGGER
                                      , self.UT_MQ_HOST
                                      , lambda_function.MQ_PORT
                                      , lambda_function.MQ_USER
                                      , lambda_function.MQ_PASSWORD)
        
        self.assertEqual(utMqConnect.getQueueCount("Queue_To_Tenant_1"), 1)
        
        # キューを全てクリア
        resultMsg = utMqConnect.getQueueMessage(queueName="Queue_To_Tenant_1"
                                                , isErrDel=True)
        
        
        del utMqConnect
        
        
    # ----------------------------------------------------------------------
    # 異なるパラメータファイルを指定した場合、
    # Exception（Argument Error. ～）がスローされること。
    # ----------------------------------------------------------------------
    def test_isArgument_001(self):
        print("------------ %s %s start------------" % (initCommon.getSysDateJst(), sys._getframe().f_code.co_name))
    
        # 起動パラメータ
        event = initCommon.readFileToJson('test/function/input001.json')
    
        exp = None
        try:
            # 実行
            lambda_function.isArgument(event)
        except Exception as ex:
            exp = ex
    
        # 検証
        print(exp)
        self.assertTrue("Argument Error. [異なるIDのパラメータファイルは指定出来ません。" in  str(exp))
    
    # ----------------------------------------------------------------------
    # 同じパラメータファイルを指定した場合、
    # Exception（Argument Error. ～）がスローされること。
    # ----------------------------------------------------------------------
    def test_isArgument_002(self):
        print("------------ %s %s start------------" % (initCommon.getSysDateJst(), sys._getframe().f_code.co_name))
    
        # 起動パラメータ
        event = initCommon.readFileToJson('test/function/input002.json')
    
        exp = None
        try:
            # 実行
            lambda_function.isArgument(event)
        except Exception as ex:
            exp = ex
    
        # 検証
        print(exp)
        self.assertTrue("Argument Error. [同じ種類のパラメータファイルは指定出来ません。" in  str(exp))
    
    
    # ----------------------------------------------------------------------
    # フォーマット不正のパラメータファイルを指定した場合、
    # Exception（Argument Error. ～）がスローされること。
    # ----------------------------------------------------------------------
    def test_isArgument_003(self):
        print("------------ %s %s start------------" % (initCommon.getSysDateJst(), sys._getframe().f_code.co_name))
    
        # 起動パラメータ
        event = initCommon.readFileToJson('test/function/input003.json')
    
        exp = None
        try:
            # 実行
            lambda_function.isArgument(event)
        except Exception as ex:
            exp = ex
    
        # 検証
        print(exp)
        self.assertTrue("Argument Error. [パラメータファイル名が不正です。" in  str(exp))
    
    # ----------------------------------------------------------------------
    # clientNameとidToken内のグループ名が異なる場合、
    # Exception（Argument Error. ～）がスローされること。
    # ----------------------------------------------------------------------
    def test_isArgument_004(self):
        print("------------ %s %s start------------" % (initCommon.getSysDateJst(), sys._getframe().f_code.co_name))
    
        # 起動パラメータ
        event = initCommon.readFileToJson('test/function/input004.json')
    
        exp = None
        try:
            # 実行
            lambda_function.isArgument(event)
        except Exception as ex:
            exp = ex
    
        # 検証
        print(exp)
        self.assertTrue("Authentication Error. [clientNameがグループに属していません。" in  str(exp))
    
    
    # ----------------------------------------------------------------------
    # 設定ファイル読み込みに成功した場合、グローバル変数に値が設定されること。
    # ----------------------------------------------------------------------
    def test_initConfig_001(self):
        print("------------ %s %s start------------" % (initCommon.getSysDateJst(), sys._getframe().f_code.co_name))
    
        # 実行
        lambda_function.initConfig("eg-iot-develop")
    
        # 検証
        self.assertEqual(lambda_function.MQ_HOST, "b-cce44f46-fd28-4ea1-926c-38c764a4af75.mq.ap-northeast-1.amazonaws.com")
    
    
    # ----------------------------------------------------------------------
    # 設定ファイル読み込みに失敗した場合、Exceptionがスローされること。
    # ----------------------------------------------------------------------
    def test_initConfig_002(self):
        print("------------ %s %s start------------" % (initCommon.getSysDateJst(), sys._getframe().f_code.co_name))
    
        with self.assertRaises(Exception):
            # 実行
            lambda_function.initConfig("eg-iot-develop2")

        
