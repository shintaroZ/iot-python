import unittest

import lambda_function
import datetime
import json
import boto3
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
    # パラメータが全てありの場合、where句がパラメータの値で整形されること。
    # ----------------------------------------------------------------------
    def test_createWhereParam_001(self):
        event = initCommon.readFileToJson('test/function/input001.json')
        result = lambda_function.createWhereParam(event)
        
        print ("========== result ==========")
        print (result)
        self.assertEqual(result, "createdt between 20210924 and 20210924" \
                         " AND rMessages.requesttimestamp between CAST('2021-09-24 00:00:00' as timestamp) and CAST('2021-09-24 23:59:59' as timestamp)" \
                         " AND rMessages.deviceid = '700400015-66DEF1DE'" \
                         " AND records.sensorid = 's2001'")
        
    # ----------------------------------------------------------------------
    # パラメータが全てなしの場合、where句が初期値で整形されること。
    # ----------------------------------------------------------------------
    def test_createWhereParam_002(self):
        event = initCommon.readFileToJson('test/function/input004.json')
        result = lambda_function.createWhereParam(event)
    
        print ("========== result ==========")
        print (result)
        self.assertEqual(result, "createdt between 20210924 and 99991231" \
                         " AND rMessages.requesttimestamp between CAST('2021-09-24 00:00:00' as timestamp) and CAST('9999-12-31 23:59:59' as timestamp)")
        
          
    # ----------------------------------------------------------------------
    # パラメータが全てなしの場合、where句が初期値で整形されること。
    # ----------------------------------------------------------------------
    def test_createWhereParam_002(self):
        event = initCommon.readFileToJson('test/function/input003.json')
        result = lambda_function.createWhereParam(event)
    
        print ("========== result ==========")
        print (result)
        self.assertEqual(result, "createdt between 19000101 and 99991231" \
                         " AND rMessages.requesttimestamp between CAST('1900-01-01 00:00:00' as timestamp) and CAST('9999-12-31 23:59:59' as timestamp)")
        
    # ----------------------------------------------------------------------
    # startDateTimeありの場合、where句がパラメータで整形されること。
    # ----------------------------------------------------------------------
    def test_createWhereParam_003(self):
        event = initCommon.readFileToJson('test/function/input004.json')
        result = lambda_function.createWhereParam(event)
    
        print ("========== result ==========")
        print (result)
        self.assertEqual(result, "createdt between 20210924 and 99991231" \
                         " AND rMessages.requesttimestamp between CAST('2021-09-24 00:00:00' as timestamp) and CAST('9999-12-31 23:59:59' as timestamp)")
        
    # ----------------------------------------------------------------------
    # endDateTimeありの場合、where句がパラメータで整形されること。
    # ----------------------------------------------------------------------
    def test_createWhereParam_004(self):
        event = initCommon.readFileToJson('test/function/input005.json')
        result = lambda_function.createWhereParam(event)
    
        print ("========== result ==========")
        print (result)
        self.assertEqual(result, "createdt between 19000101 and 20210924" \
                         " AND rMessages.requesttimestamp between CAST('1900-01-01 00:00:00' as timestamp) and CAST('2021-09-24 23:59:59' as timestamp)")
        
    # ----------------------------------------------------------------------
    # deviceIdありの場合、where句がパラメータで整形されること。
    # ----------------------------------------------------------------------
    def test_createWhereParam_005(self):
        event = initCommon.readFileToJson('test/function/input006.json')
        result = lambda_function.createWhereParam(event)
    
        print ("========== result ==========")
        print (result)
        self.assertEqual(result, "createdt between 20210924 and 20210924" \
                         " AND rMessages.requesttimestamp between CAST('2021-09-24 00:00:00' as timestamp) and CAST('2021-09-24 23:59:59' as timestamp)" \
                         " AND rMessages.deviceid = '700400015-66DEF1DE'" )
        
    # ----------------------------------------------------------------------
    # s3に該当データありの場合、json整形後に公開db作成機能が呼び出されること。
    # 起動パラメータ:startDateTime、endDateTime、deviceId、sensorId
    # ----------------------------------------------------------------------
    def test_lambda_handler_001(self):
        print("---test_lambda_handler_001---")
        event = initCommon.readFileToJson('test/function/input001.json')
        lambda_function.lambda_handler(event, None)

    # ----------------------------------------------------------------------
    # s3に該当データなしの場合、例外がスローされること。
    # 起動パラメータ:startDateTime、endDateTime、deviceId、sensorId
    # ----------------------------------------------------------------------
    def xtest2_lambda_handler_002(self):
        print("---test_lambda_handler_002---")
        event = initCommon.readFileToJson('test/function/input002.json')

        with self.assertRaises(Exception):
            lambda_function.lambda_handler(event, None)
