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


class LambdaFunctionTest(unittest.TestCase):

    RDS = None

    # テスト開始時に1回だけ呼び出される
    # クラスメソッドとして定義する
    @classmethod
    def setUpClass(cls):
        global RDS
        lambda_function.initConfig("eg-iot-develop")
        lambda_function.setLogger(initCommon.getLogger(lambda_function.LOG_LEVEL))
        RDS = rdsCommon.rdsCommon(lambda_function.LOGGER
                                , lambda_function.DB_HOST
                                , lambda_function.DB_PORT
                                , lambda_function.DB_USER
                                , lambda_function.DB_PASSWORD
                                , lambda_function.DB_NAME
                                , lambda_function.DB_CONNECT_TIMEOUT)

    # ----------------------------------------------------------------------
    # 全項目ありの新規
    # ----------------------------------------------------------------------
    def test_lambda_handler_001(self):
        print("---test_lambda_handler_001---")
        event = initCommon.readFileToJson('test/function/input001.json')

        # 事前に削除
        result = RDS.fetchone(initCommon.getQuery("test/sql/m_mail_send/delete.sql"),
                              {
                                  "mailSendId" : event["mailSendId"]
                                  ,"deleteCount" : 0 })
        RDS.commit()

        # 実行
        lambda_function.lambda_handler(event, None)

        result = RDS.fetchone(initCommon.getQuery("test/sql/m_mail_send/findbyId.sql"),
                              {
                                  "mailSendId" : event["mailSendId"]
                                  ,"deleteCount" : 0 })
        self.assertEqual(result["mailSendId"], 3)
        self.assertEqual(result["emailAddress"], "aaaaabbb@email.co.jp")
        self.assertEqual(result["sendWeekType"], 0)
        self.assertEqual(result["sendFrequancy"], 1)
        self.assertEqual(result["sendTimeFrom"], "100000")
        self.assertEqual(result["sendTimeTo"], "190000")
        self.assertEqual(result["mailSubject"], "テスト件名1")

    # ----------------------------------------------------------------------
    # 全項目ありの更新
    # ----------------------------------------------------------------------
    def test_lambda_handler_002(self):
        print("---test_lambda_handler_002---")
        event = initCommon.readFileToJson('test/function/input002.json')

        # 実行
        lambda_function.lambda_handler(event, None)

        result = RDS.fetchone(initCommon.getQuery("test/sql/m_mail_send/findbyId.sql"),
                              {
                                  "mailSendId" : event["mailSendId"]
                                  ,"deleteCount" : 0 })
        self.assertEqual(result["mailSendId"], 3)
        self.assertEqual(result["emailAddress"], "ccc@email.co.jp")
        self.assertEqual(result["sendWeekType"], 0)
        self.assertEqual(result["sendFrequancy"], 1)
        self.assertEqual(result["sendTimeFrom"], "100000")
        self.assertEqual(result["sendTimeTo"], "190000")
        self.assertEqual(result["mailSubject"], "テスト件名1")

    # ----------------------------------------------------------------------
    # 設定ファイルなし
    # An error occurred (NoSuchBucket) when calling the GetObject operation: The specified bucket does not exist
    # ----------------------------------------------------------------------
    def test_lambda_handler_003(self):
        print("---test_lambda_handler_003---")
        event = initCommon.readFileToJson('test/function/input003.json')

        isException = False
        try:
            lambda_function.lambda_handler(event, None)
        except Exception as ex:
            print(ex)
            isException = True
        self.assertEqual(isException, True)
    # ----------------------------------------------------------------------
    # 必須項目なし
    # Missing required request parameters.
    # ----------------------------------------------------------------------
    def test_lambda_handler_004(self):
        print("---test_lambda_handler_004---")
        event = initCommon.readFileToJson('test/function/input004.json')

        isException = False
        try:
            lambda_function.lambda_handler(event, None)
        except Exception as ex:
            print(ex)
            isException = True
        self.assertEqual(isException, True)

    # ----------------------------------------------------------------------
    # データ型不正
    # The parameters is type invalid. [mailSendId,sendWeekType,sendFrequancy]
    # ----------------------------------------------------------------------
    def test_lambda_handler_005(self):
        print("---test_lambda_handler_005---")
        event = initCommon.readFileToJson('test/function/input005.json')

        isException = False
        try:
            lambda_function.lambda_handler(event, None)
        except Exception as ex:
            print(ex)
            isException = True
        self.assertEqual(isException, True)

    # ----------------------------------------------------------------------
    # データ長不正
    # Missing required range parameters.
    # ----------------------------------------------------------------------
    def test_lambda_handler_006(self):
        print("---test_lambda_handler_006---")
        event = initCommon.readFileToJson('test/function/input006.json')

        isException = False
        try:
            lambda_function.lambda_handler(event, None)
        except Exception as ex:
            print(ex)
            isException = True
        self.assertEqual(isException, True)

    # ----------------------------------------------------------------------
    # 通知IDの範囲外
    # The parameters is range invalid. [mailSendId]
    # ----------------------------------------------------------------------
    def test_lambda_handler_007(self):
        print("---test_lambda_handler_007---")
        event = initCommon.readFileToJson('test/function/input007.json')

        isException = False
        try:
            lambda_function.lambda_handler(event, None)
        except Exception as ex:
            print(ex)
            isException = True
        self.assertEqual(isException, True)
    # ----------------------------------------------------------------------
    # 通知IDの範囲外
    # The parameters is range invalid. [mailSendId]
    # ----------------------------------------------------------------------
    def test_lambda_handler_008(self):
        print("---test_lambda_handler_008---")
        event = initCommon.readFileToJson('test/function/input008.json')

        isException = False
        try:
            lambda_function.lambda_handler(event, None)
        except Exception as ex:
            print(ex)
            isException = True
        self.assertEqual(isException, True)