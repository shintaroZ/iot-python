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

def createEvent(query_file_path):
    f = open(query_file_path, 'r')
    return json.load(f)

# --------------------------------------------------
# 音ファイル作成機能テスト
# --------------------------------------------------
class LambdaFunctionTest(unittest.TestCase):

    # テスト開始時に1回だけ呼び出される
    # クラスメソッドとして定義する
    @classmethod
    def setUpClass(cls):
        lambda_function.initConfig("config/setting01.xml")
        lambda_function.setLogger(initCommon.getLogger(lambda_function.LOG_LEVEL))


    # ----------------------------------------------------------------------
    # getParamReConv()の正常系テスト
    # recordsが1レコードのみの場合、data部の結合なく返却されること。
    # ----------------------------------------------------------------------
    def test_getParamReConv_001(self):
        event = createEvent("test/function/input001.json")
        result = lambda_function.getParamReConv(event["records"])
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["data"], event["records"][0]["data"] )

    # ----------------------------------------------------------------------
    # getParamReConv()の正常系テスト
    # recordsで分割レコードありの場合、data部が結合されて返却されること。
    # ----------------------------------------------------------------------
    def test_getParamReConv_002(self):
        event = createEvent("test/function/input002.json")
        result = lambda_function.getParamReConv(event["records"])
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["data"], "aaaaabbbbb")

    # ----------------------------------------------------------------------
    # getParamReConv()の正常系テスト
    # recordsで別レコード(先頭)+分割レコードありの場合、data部が結合されて返却されること。
    # ----------------------------------------------------------------------
    def test_getParamReConv_003(self):
        event = createEvent("test/function/input003.json")
        result = lambda_function.getParamReConv(event["records"])
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["data"], "aaaaabbbbb")
        self.assertEqual(result[1]["data"], "ccccc")

    # ----------------------------------------------------------------------
    # getParamReConv()の正常系テスト
    # recordsで別レコード(中間)+分割レコードありの場合、data部が結合されて返却されること。
    # ----------------------------------------------------------------------
    def test_getParamReConv_004(self):
        event = createEvent("test/function/input004.json")
        result = lambda_function.getParamReConv(event["records"])
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["data"], "aaaaabbbbb")
        self.assertEqual(result[1]["data"], "ccccc")

    # ----------------------------------------------------------------------
    # getParamReConv()の正常系テスト
    # recordsで別レコード(最後)+分割レコードありの場合、data部が結合されて返却されること。
    # ----------------------------------------------------------------------
    def test_getParamReConv_005(self):
        event = createEvent("test/function/input005.json")
        result = lambda_function.getParamReConv(event["records"])
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["data"], "aaaaabbbbb")
        self.assertEqual(result[1]["data"], "ccccc")

# if __name__ == "__main__":
#     unittest.main()