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
    #　削除回数 = 0
    # ----------------------------------------------------------------------
    def test_lambda_handler_001(self):
        print("---test_lambda_handler_001---")
        event = initCommon.readFileToJson('test/function/input001.json')

        # 予め更新
        RDS.execute(initCommon.getQuery("test/sql/delete.sql"),
                    {
                        "deviceId" : event["deviceId"]
                     ,  "sensorId" : event["sensorId"]
                     ,  "whereDeleteCount" : 0})
        RDS.execute(initCommon.getQuery("test/sql/update.sql"),
                    {
                        "deviceId" : event["deviceId"]
                     ,  "sensorId" : event["sensorId"]
                     ,  "whereDeleteCount" : 1
                     ,  "deleteCount" : 0})
        RDS.commit()

        # 実行
        lambda_function.lambda_handler(event, None)

        result = RDS.fetchone(initCommon.getQuery("test/sql/findbyId.sql"),
                    {
                        "deviceId" : event["deviceId"]
                     ,  "sensorId" : event["sensorId"]
                     ,  "deleteCount" : 1})
        self.assertEqual(result["deleteCount"], 1)

    # ----------------------------------------------------------------------
    #　1 < 削除回数
    # ----------------------------------------------------------------------
    def test_lambda_handler_002(self):
        print("---test_lambda_handler_002---")

        event = initCommon.readFileToJson('test/function/input002.json')

        # 予め更新
        RDS.execute(initCommon.getQuery("test/sql/delete.sql"),
                    {
                        "deviceId" : event["deviceId"]
                     ,  "sensorId" : event["sensorId"]
                     ,  "whereDeleteCount" : 0})
        RDS.execute(initCommon.getQuery("test/sql/update.sql"),
                    {
                        "deviceId" : event["deviceId"]
                     ,  "sensorId" : event["sensorId"]
                     ,  "whereDeleteCount" : 2
                     ,  "deleteCount" : 0})
        RDS.commit()

        # 実行
        lambda_function.lambda_handler(event, None)

        result = RDS.fetchone(initCommon.getQuery("test/sql/findbyId.sql"),
                    {
                        "deviceId" : event["deviceId"]
                     ,  "sensorId" : event["sensorId"]
                     ,  "deleteCount" : 2})
        self.assertEqual(result["deleteCount"], 2)


