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
import sys


class LambdaFunctionTest(unittest.TestCase):

    UT_RDS = None

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
    # 正常ケース
    # 音ファイルが存在しplayMode=0の場合、ダウンロード用の署名付きURLが返却されること。
    # ----------------------------------------------------------------------
    def test_lambda_handler_001(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)

        # 実行
        event = initCommon.readFileToJson('test/function/input001.json')
        result = lambda_function.lambda_handler(event, None)

        print ("================ result ================")
        print (result)
        self.assertEqual(len(result), 1)
        
    # ----------------------------------------------------------------------
    # 正常ケース
    # 音ファイルが存在しplayMode=1の場合、再生用の署名付きURLが返却されること。
    # ----------------------------------------------------------------------
    def test_lambda_handler_002(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)

        # 実行
        event = initCommon.readFileToJson('test/function/input002.json')
        result = lambda_function.lambda_handler(event, None)

        print ("================ result ================")
        print (result)
        self.assertEqual(len(result), 1)
        
    # ----------------------------------------------------------------------
    # 異常ケース
    # playModeが範囲外の場合、例外（The parameters is length invalid.～）がスローされること。
    # ----------------------------------------------------------------------
    def test_lambda_handler_003(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)

        # 実行
        event = initCommon.readFileToJson('test/function/input003.json')
        with self.assertRaises(Exception):
            lambda_function.lambda_handler(event, None)
            
    # ----------------------------------------------------------------------
    # 異常ケース
    # 音ファイル作成履歴から音ファイル名の取得に失敗した場合、
    # 例外（Internal Server Error.～）がスローされること。
    # ----------------------------------------------------------------------
    def test_lambda_handler_004(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)

        # 実行
        event = initCommon.readFileToJson('test/function/input004.json')
        with self.assertRaises(Exception):
            lambda_function.lambda_handler(event, None)
            
    # ----------------------------------------------------------------------
    # 異常ケース
    # S3からファイル取得に失敗した場合、
    # 例外（Internal Server Error.～）がスローされること。
    # ----------------------------------------------------------------------
    def test_lambda_handler_005(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)

        # 実行
        event = initCommon.readFileToJson('test/function/input005.json')
        with self.assertRaises(Exception):
            lambda_function.lambda_handler(event, None)
