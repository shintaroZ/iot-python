# from collections import OrderedDict
# from datetime import datetime as dt
# import datetime
# import json
# import pprint
# import unittest
#
# import pymysql
import json
import unittest
import lambda_function
import initCommon # カスタムレイヤー
import rdsCommon  # カスタムレイヤー
import sys

def createEvent(query_file_path):
    f = open(query_file_path, 'r')
    return json.load(f)

# --------------------------------------------------
# 音ファイル作成機能テスト
# --------------------------------------------------
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
    # 音ファイルがS3に登録され、音ファイル作成履歴にレコードが追加されること。
    # ----------------------------------------------------------------------
    def test_lambda_handler_001(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)
        event = createEvent("test/function/input001.json")
        lambda_function.lambda_handler(event, None)
        
        soundResult = self.UT_RDS.fetchone(initCommon.getQuery("test/sqlut/soundFileHistory_get.sql")
                             , {
                                 "dataCollectionSeq" : 101
                                 , "createdDatetime" : "2021/04/22 16:50:01.895"
                                 })
        
        self.assertEqual(soundResult["fileType"], 1)
        self.assertEqual(soundResult["fileName"], "soundstrage/101/20210422/20210422165000.mp3")
        

    # ----------------------------------------------------------------------
    # 異常ケース
    # 音ファイル作成に失敗した場合、例外がスローされること。
    # ----------------------------------------------------------------------
    def test_lambda_handler_002(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)
        event = createEvent("test/function/input002.json")
        with self.assertRaises(Exception):
            print("test_lambda_handler_002:Exception is throw")
            lambda_function.lambda_handler(event, None)
        
    # ----------------------------------------------------------------------
    # getParamReConv()の正常系テスト
    # recordsが1レコードのみの場合、data部の結合なく返却されること。
    # ----------------------------------------------------------------------
    def test_getParamReConv_001(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)
        event = createEvent("test/function/input001.json")
        result = lambda_function.getParamReConv(event["receivedMessages"][0]["records"])
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["data"], event["receivedMessages"][0]["records"][0]["data"] )

    # ----------------------------------------------------------------------
    # getParamReConv()の正常系テスト
    # recordsで分割レコードありの場合、data部が結合されて返却されること。
    # ----------------------------------------------------------------------
    def test_getParamReConv_002(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)
        event = createEvent("test/function/input002.json")
        result = lambda_function.getParamReConv(event["receivedMessages"][0]["records"])
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["data"], "aaaaabbbbb")

    # ----------------------------------------------------------------------
    # getParamReConv()の正常系テスト
    # recordsで別レコード(先頭)+分割レコードありの場合、data部が結合されて返却されること。
    # ----------------------------------------------------------------------
    def test_getParamReConv_003(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)
        event = createEvent("test/function/input003.json")
        result = lambda_function.getParamReConv(event["receivedMessages"][0]["records"])
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["data"], "aaaaabbbbb")

# if __name__ == "__main__":
#     unittest.main()