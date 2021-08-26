import unittest
import lambda_function
import datetime
import json

import initCommon  # カスタムレイヤー
import rdsCommon  # カスタムレイヤー

class LambdaFunctionTest(unittest.TestCase):

    # ----------------------------------------------------------------------
    # initConfig()の正常系テスト
    # 顧客名が有効な場合、設定値がグローバル変数に定義されること。
    # ----------------------------------------------------------------------
    def test_initConfig_001(self):
        clientName = 'eg-iot-develop'
        lambda_function.initConfig(clientName)
        # athena setting
        self.assertEqual(lambda_function.ATENA_DATABASE, "ins001recovery")
        self.assertEqual(lambda_function.ATENA_TABLE, "backupdata")
        self.assertEqual(lambda_function.S3_OUTPUT, "tmpathenaoutput")
        self.assertEqual(lambda_function.RETRY_COUNT, 3000)
        self.assertEqual(lambda_function.RETRY_INTERVAL, 200)
        # logger setting
        self.assertEqual(lambda_function.LOG_LEVEL, "INFO")

    # ----------------------------------------------------------------------
    # initConfig()の異常系テスト
    # 顧客名が無効な場合、Exceptionがスローされること。
    # ----------------------------------------------------------------------
    def test_initConfig_002(self):
        clientName = 'eg-iot-developxxx'
        with self.assertRaises(Exception):
            lambda_function.initConfig(clientName)

    # ----------------------------------------------------------------------
    # 対象データありの場合、json形式の取得結果が返却されること。
    # 起動パラメータ:startDateTime、endDateTime
    # ----------------------------------------------------------------------
    def test_lambda_handler_001(self):
        print("---test_lambda_handler_001---")
        event = initCommon.readFileToJson('test/function/input001.json')

        resultStr = lambda_function.lambda_handler(event, None)

        print("================ result ================")
        resultJson = json.loads(resultStr)
        print("clientName:%s" % resultJson["clientName"])
        for rMessage in resultJson["receivedMessages"]:
            print("deviceId:%s" % rMessage["deviceId"])
            print("requestTimeStamp:%s" % rMessage["requestTimeStamp"])
            print("records count:%d" % len(rMessage["records"]))

        # 検証
        self.assertEqual(resultJson["clientName"], "eg-iot-develop")
        self.assertEqual(resultJson["receivedMessages"][0]["deviceId"], "700400014-F6CA332A")
        self.assertEqual(resultJson["receivedMessages"][0]["requestTimeStamp"], "2021-05-01 00:01:00.945")
        self.assertEqual(resultJson["receivedMessages"][1]["deviceId"], "700400015-66DEF1DE")
        self.assertEqual(resultJson["receivedMessages"][1]["requestTimeStamp"], "2021-05-01 00:00:59.908")

    # ----------------------------------------------------------------------
    # 対象データありの場合、json形式の取得結果が返却されること。
    # 起動パラメータ:startDateTime、endDateTime、deviceId
    # ----------------------------------------------------------------------
    def test_lambda_handler_002(self):
        print("---test_lambda_handler_002---")
        event = initCommon.readFileToJson('test/function/input002.json')

        resultStr = lambda_function.lambda_handler(event, None)

        print("================ result ================")
        resultJson = json.loads(resultStr)
        print("clientName:%s" % resultJson["clientName"])
        for rMessage in resultJson["receivedMessages"]:
            print("deviceId:%s" % rMessage["deviceId"])
            print("requestTimeStamp:%s" % rMessage["requestTimeStamp"])
            print("records count:%d" % len(rMessage["records"]))

        # 検証
        self.assertEqual(resultJson["clientName"], "eg-iot-develop")
        self.assertEqual(resultJson["receivedMessages"][0]["deviceId"], "700400015-66DEF1DE")
        self.assertEqual(resultJson["receivedMessages"][0]["requestTimeStamp"], "2021-05-01 00:00:59.908")

    # ----------------------------------------------------------------------
    # 対象データありの場合、json形式の取得結果が返却されること。
    # 起動パラメータ:startDateTime、endDateTime、deviceId、sensorId
    # ----------------------------------------------------------------------
    def test_lambda_handler_003(self):
        print("---test_lambda_handler_003---")
        event = initCommon.readFileToJson('test/function/input003.json')

        resultStr = lambda_function.lambda_handler(event, None)

        print("================ result ================")
        resultJson = json.loads(resultStr)
        print("clientName:%s" % resultJson["clientName"])
        for rMessage in resultJson["receivedMessages"]:
            print("deviceId:%s" % rMessage["deviceId"])
            print("requestTimeStamp:%s" % rMessage["requestTimeStamp"])
            print("records count:%d" % len(rMessage["records"]))

        # 検証
        self.assertEqual(resultJson["clientName"], "eg-iot-develop")
        self.assertEqual(resultJson["receivedMessages"][0]["deviceId"], "700400015-66DEF1DE")
        self.assertEqual(resultJson["receivedMessages"][0]["requestTimeStamp"], "2021-05-01 00:00:59.908")

    # ----------------------------------------------------------------------
    # 対象データなしの場合、例外(SystemExit)がスローされること。
    # 起動パラメータ:startDateTime、endDateTime、deviceId、sensorId
    # ----------------------------------------------------------------------
    def test_lambda_handler_004(self):
        print("---test_lambda_handler_004---")
        event = initCommon.readFileToJson('test/function/input004.json')

        with self.assertRaises(SystemExit):
            lambda_function.lambda_handler(event, None)
