import unittest

import lambda_function
import datetime
import json
import boto3
import initCommon  # カスタムレイヤー
import rdsCommon  # カスタムレイヤー
import sys

class LambdaFunctionTest(unittest.TestCase):

    # ----------------------------------------------------------------------
    # initConfig()の正常系テスト
    # 顧客名が有効な場合、設定値がグローバル変数に定義されること。
    # ----------------------------------------------------------------------
    def test_initConfig_001(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)
        clientName = 'eg-iot-develop'
        lambda_function.initConfig(clientName)
        # athena setting
        self.assertEqual(lambda_function.ATHENA_DATABASE, "ins001recovery")
        self.assertEqual(lambda_function.ATHENA_DGW_TABLE, "backupdatadgw")
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
        print("------------ %s start------------" % sys._getframe().f_code.co_name)
        clientName = 'eg-iot-developxxx'
        with self.assertRaises(Exception):
            lambda_function.initConfig(clientName)
          
    # ----------------------------------------------------------------------
    # パラメータが全てありの場合、where句がパラメータの値で整形されること。
    # ----------------------------------------------------------------------
    def test_createWhereParam_001(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)
        event = initCommon.readFileToJson('test/function/input001.json')
        result = lambda_function.createWhereParam(event)
        
        print ("========== result ==========")
        print (result)
        self.assertEqual(result["startDateInt"], 20210924)
        self.assertEqual(result["endDateInt"], 20210924)
        self.assertEqual(result["startDateTime"].strftime('%Y-%m-%d %H:%M:%S'), "2021-09-24 00:00:00")
        self.assertEqual(result["endDateTime"].strftime('%Y-%m-%d %H:%M:%S'), "2021-09-24 04:59:59")
        self.assertEqual(result["whereParam"], " AND rMessages.deviceid = '700400015-66DEF1DE' AND records.sensorid = 's2001'")
             
    # ----------------------------------------------------------------------
    # パラメータが全てなしの場合、whereParamは空文字が返却されること。
    # ----------------------------------------------------------------------
    def test_createWhereParam_002(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)
        event = initCommon.readFileToJson('test/function/input002.json')
        result = lambda_function.createWhereParam(event)
    
        print ("========== result ==========")
        print (result)
        self.assertEqual(result["startDateInt"], 20210501)
        self.assertEqual(result["endDateInt"], 20210501)
        self.assertEqual(result["startDateTime"].strftime('%Y-%m-%d %H:%M:%S'), "2021-05-01 00:00:00")
        self.assertEqual(result["endDateTime"].strftime('%Y-%m-%d %H:%M:%S'), "2021-05-01 23:59:59")
        self.assertEqual(result["whereParam"], "")
        
    # ----------------------------------------------------------------------
    # deviceIdありの場合、where句がパラメータで整形されること。
    # ----------------------------------------------------------------------
    def test_createWhereParam_003(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)
        event = initCommon.readFileToJson('test/function/input006.json')
        result = lambda_function.createWhereParam(event)
    
        print ("========== result ==========")
        print (result)
        self.assertEqual(result["startDateInt"], 20210924)
        self.assertEqual(result["endDateInt"], 20210924)
        self.assertEqual(result["startDateTime"].strftime('%Y-%m-%d %H:%M:%S'), "2021-09-24 00:00:00")
        self.assertEqual(result["endDateTime"].strftime('%Y-%m-%d %H:%M:%S'), "2021-09-24 23:59:59")
        self.assertEqual(result["whereParam"], " AND rMessages.deviceid = '700400015-66DEF1DE'")
        
    # ----------------------------------------------------------------------
    # s3に該当データあり(dgw)の場合、json整形後に公開db作成機能が呼び出されること。
    # 起動パラメータ:startDateTime、endDateTime、deviceId、sensorId
    # ----------------------------------------------------------------------
    def test_lambda_handler_001(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)
        event = initCommon.readFileToJson('test/function/input001.json')
        lambda_function.lambda_handler(event, None)

    # ----------------------------------------------------------------------
    # s3に該当データなしの場合、例外がスローされること。
    # 起動パラメータ:startDateTime、endDateTime、deviceId、sensorId
    # ----------------------------------------------------------------------
    def xtest2_lambda_handler_002(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)
        event = initCommon.readFileToJson('test/function/input002.json')

        with self.assertRaises(Exception):
            lambda_function.lambda_handler(event, None)
                   
    # ----------------------------------------------------------------------
    # s3に該当データあり(monone)の場合、json整形後に公開db作成機能が呼び出されること。
    # 起動パラメータ:startDateTime、endDateTime、deviceId
    # ----------------------------------------------------------------------
    def test_lambda_handler_003(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)
        event = initCommon.readFileToJson('test/function/input007.json')
        lambda_function.lambda_handler(event, None)

