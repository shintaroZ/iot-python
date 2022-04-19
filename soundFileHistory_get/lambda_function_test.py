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
    #  全パラメータ指定ありの場合、抽出条件に該当するレコードが返却されること。
    # ----------------------------------------------------------------------
    def test_lambda_handler_001(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)

        # 実行
        event = initCommon.readFileToJson('test/function/input001.json')
        result = lambda_function.lambda_handler(event, None)

        print ("================ result ================")
        print (result)
        jsonResult =  json.loads(result)
        self.assertEqual(len(jsonResult), 1)
        self.assertEqual(jsonResult["records"][0]["dataCollectionSeq"], 101)
        self.assertEqual(jsonResult["records"][0]["createdDateTime"], "2021/04/22 16:50:01.895000")
        self.assertEqual(jsonResult["records"][0]["fileType"], 1)
        self.assertEqual(jsonResult["records"][0]["fileName"], "soundstrage/101/20210422/20210422165000.mp3")
        self.assertEqual(jsonResult["records"][0]["createdAt"], "2022/04/19 13:36:51")
        
    # ----------------------------------------------------------------------
    #  パラメータ指定なしの場合、全レコードが返却されること。
    # ----------------------------------------------------------------------
    def test_lambda_handler_002(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)

        # 実行
        event = initCommon.readFileToJson('test/function/input002.json')
        result = lambda_function.lambda_handler(event, None)

        print ("================ result ================")
        print (result)
        jsonResult =  json.loads(result)
        
    # ----------------------------------------------------------------------
    #  一部パラメータ指定ありの場合、抽出条件に該当するレコードが返却されること。
    # ----------------------------------------------------------------------
    def test_lambda_handler_003(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)

        # 実行
        event = initCommon.readFileToJson('test/function/input003.json')
        result = lambda_function.lambda_handler(event, None)

        print ("================ result ================")
        print (result)
        jsonResult =  json.loads(result)
        
    # ----------------------------------------------------------------------
    #  一部パラメータ指定ありの組み合わせ網羅
    # ----------------------------------------------------------------------
    def test_lambda_handler_004(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)

        # 実行
        event = initCommon.readFileToJson('test/function/input004.json')
        result = lambda_function.lambda_handler(event, None)

        print ("================ result ================")
        print (result)
        jsonResult =  json.loads(result)
        
    # ----------------------------------------------------------------------
    #  一部パラメータ指定ありの組み合わせ網羅
    # ----------------------------------------------------------------------
    def test_lambda_handler_005(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)

        # 実行
        event = initCommon.readFileToJson('test/function/input005.json')
        result = lambda_function.lambda_handler(event, None)

        print ("================ result ================")
        print (result)
        jsonResult =  json.loads(result)
        
    # ----------------------------------------------------------------------
    #  一部パラメータ指定ありの組み合わせ網羅
    # ----------------------------------------------------------------------
    def test_lambda_handler_006(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)

        # 実行
        event = initCommon.readFileToJson('test/function/input006.json')
        result = lambda_function.lambda_handler(event, None)

        print ("================ result ================")
        print (result)
        jsonResult =  json.loads(result)
        
    # ----------------------------------------------------------------------
    #  レコードなしの場合、空のrecords配列が返却されること。
    # ----------------------------------------------------------------------
    def test_lambda_handler_007(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)

        # 実行
        event = initCommon.readFileToJson('test/function/input007.json')
        result = lambda_function.lambda_handler(event, None)

        print ("================ result ================")
        print (result)
        jsonResult =  json.loads(result)
        self.assertEqual(len(jsonResult["records"]),0)
        
        
        
        
