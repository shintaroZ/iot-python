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


class OpcuaFormatConverterTest(unittest.TestCase):

    # テスト開始時に1回だけ呼び出される
    # クラスメソッドとして定義する
    @classmethod
    def setUpClass(self):
        pass

    @classmethod
    def tearDownClass(self):
        pass
    
    # ----------------------------------------------------------------------
    # 数値型のOPC-UAのフォーマットがDGW用に整形されること。
    # ----------------------------------------------------------------------
    def test_lambda_handler_001(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)

        event = initCommon.readFileToJson('test/function/input001.json')
        result = lambda_function.lambda_handler(event, None)
        self.assertEqual(result.get("receivedMessages")[0].get("deviceId"), "cb24680e-b029-44c4-a7e6-feff0e4e780e" )
        self.assertEqual(result.get("receivedMessages")[0].get("requestTimeStamp"), "2022-11-08 10:48:56" )
        
        
    # ----------------------------------------------------------------------
    # 文字型のOPC-UAのフォーマットがDGW用に整形されること。
    # ----------------------------------------------------------------------
    def test_lambda_handler_002(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)

        event = initCommon.readFileToJson('test/function/input002.json')
        result = lambda_function.lambda_handler(event, None)
        self.assertEqual(result.get("receivedMessages")[0].get("deviceId"), "cb24680e-b029-44c4-a7e6-feff0e4e780e" )
        self.assertEqual(result.get("receivedMessages")[0].get("requestTimeStamp"), "2022-11-08 10:48:56" )
        
        
    # ----------------------------------------------------------------------
    # bool型のOPC-UAのフォーマットがDGW用に整形されること。
    # ----------------------------------------------------------------------
    def test_lambda_handler_003(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)

        event = initCommon.readFileToJson('test/function/input003.json')
        result = lambda_function.lambda_handler(event, None)
        self.assertEqual(result.get("receivedMessages")[0].get("deviceId"), "cb24680e-b029-44c4-a7e6-feff0e4e780e" )
        self.assertEqual(result.get("receivedMessages")[0].get("requestTimeStamp"), "2022-11-08 10:48:56" )
        
        
    # ----------------------------------------------------------------------
    # double型のOPC-UAのフォーマットがDGW用に整形されること。
    # ----------------------------------------------------------------------
    def test_lambda_handler_004(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)

        event = initCommon.readFileToJson('test/function/input004.json')
        result = lambda_function.lambda_handler(event, None)
        self.assertEqual(result.get("receivedMessages")[0].get("deviceId"), "cb24680e-b029-44c4-a7e6-feff0e4e780e" )
        self.assertEqual(result.get("receivedMessages")[0].get("requestTimeStamp"), "2022-11-08 10:48:56" )
        
    # ----------------------------------------------------------------------
    # フォーマット不正の場合、Exceptionが発生すること。
    # ----------------------------------------------------------------------
    def test_lambda_handler_005(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)
        
        event = initCommon.readFileToJson('test/function/input005.json')
        isError = False
        try:
            result = lambda_function.lambda_handler(event, None)
        except Exception as ex:
            print(ex)
            isError = True
        self.assertTrue(isError)
        