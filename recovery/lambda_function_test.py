import unittest
import lambda_function
import datetime

import initCommon  # カスタムレイヤー
import rdsCommon  # カスタムレイヤー

class LambdaFunctionTest(unittest.TestCase):

    # ----------------------------------------------------------------------
    # getMasterDataCollection()の正常系テスト
    # 該当データありの場合、レコード情報が返却されること。
    # ----------------------------------------------------------------------
    def test_lambda_handler_001(self):
        print("---test_lambda_handler_001---")
        event = initCommon.readFileToJson('test/function/input001.json')
        
        
        result = lambda_function.lambda_handler(event, None)
        print(result)
    # # ----------------------------------------------------------------------
    # # initConfig()の正常系テスト
    # # 設定ファイルパスが有効な場合、設定値がグローバル変数に定義されること。
    # # ----------------------------------------------------------------------
    # def test_initConfig_001(self):
        # filePath = 'config/setting01.ini'
        # lambda_function.initConfig(filePath)
        # # athena setting
        # self.assertEqual(lambda_function.ATENA_DATABASE, "ins001recovery")
        # self.assertEqual(lambda_function.ATENA_TABLE, "backupdata")
        # self.assertEqual(lambda_function.S3_OUTPUT, "s3://tmpathenaoutput")
        # self.assertEqual(lambda_function.RETRY_COUNT, 10)
        # # logger setting
        # self.assertEqual(lambda_function.LOG_LEVEL, "INFO")
        #
    # # ----------------------------------------------------------------------
    # # initConfig()の異常系テスト
    # # 設定ファイルパスが無効な場合、Exceptionがスローされること。
    # # ----------------------------------------------------------------------
    # def test_initConfig_001(self):
        # filePath = 'config/setting02.ini'
        # with self.assertRaises(Exception):
            # lambda_function.initConfig(filePath)
            #
    # # ----------------------------------------------------------------------
    # # customTime()の正常系テスト
    # # 現在日時のtime.struct_time型が返却されること。
    # # ----------------------------------------------------------------------
    # def test_customTime_001(self):
        # result = lambda_function.customTime()
        # dtNow = datetime.datetime.now()
        # self.assertEqual(result.tm_year, int(dtNow.strftime('%Y')))
        # self.assertEqual(result.tm_mon, int(dtNow.strftime('%m')))
        # self.assertEqual(result.tm_mday, int(dtNow.strftime('%d')))
        # self.assertEqual(result.tm_hour, int(dtNow.strftime('%H')))
        # self.assertEqual(result.tm_min, int(dtNow.strftime('%M')))
        # self.assertEqual(result.tm_sec, int(dtNow.strftime('%S')))
        #
    # # ----------------------------------------------------------------------
    # # initLogger()の正常系テスト
    # # ロガーの設定が行われ正常終了すること。
    # # ----------------------------------------------------------------------
    # def test_initLogger_001(self):
        # lambda_function.initLogger()
        #
    # # ----------------------------------------------------------------------
    # # get_query()の正常系テスト
    # # 指定したファイルの内容が返却されること。
    # # ----------------------------------------------------------------------
    # def test_get_query_001(self):
        # lambda_function.initConfig("config/setting01.ini")
        # result = lambda_function.get_query("test/function/input002.json")
        # self.assertIsNotNone(result)
        #
    # # ----------------------------------------------------------------------
    # # get_recovery_data()の正常系テスト
    # # データが正常に成形されていること。
    # # ----------------------------------------------------------------------
    # def test_data_molding_001(self):
        # lambda_function.initConfig("config/setting01.ini")
        # recoveryDataList = []
        # recoveryData = {
             # "setting" : "config/setting01.ini"
            # ,"deviceId" : "700400015-66DEF1DE"
            # ,"requestTimeStamp" : "2021-02-18 11:29:03.942"
            # ,"sensorId" : "s001"
            # ,"timeStamp" : "2021-02-18 11:29:03.942"
            # ,"value" : 12.31
        # }
        # recoveryDataList.append(recoveryData)
        # result = lambda_function.data_molding(recoveryDataList)
        # self.assertEqual(1, len(result))
        # self.assertEqual(1, result[0]["receveryFlg"])
        # self.assertEqual("config/setting01.ini", result[0]["setting"])
        # self.assertEqual("700400015-66DEF1DE", result[0]["deviceId"])
        # self.assertEqual("2021-02-18 11:29:03.942", result[0]["requestTimeStamp"])
        # self.assertEqual("s001", result[0]["records"][0]["sensorId"])
        # self.assertEqual("2021-02-18 11:29:03.942", result[0]["records"][0]["timeStamp"])
        # self.assertEqual(12.31, result[0]["records"][0]["value"])
        #
    # # ----------------------------------------------------------------------
    # # get_recovery_data()の正常系テスト
    # # データが正常に成形されていること。
    # # ----------------------------------------------------------------------
    # def test_data_molding_002(self):
        # lambda_function.initConfig("config/setting01.ini")
        # recoveryDataList = []
        # recoveryData = {
             # "setting" : "config/setting01.ini"
            # ,"deviceId" : "700400015-66DEF1DE"
            # ,"requestTimeStamp" : "2021-02-18 11:29:03.942"
            # ,"sensorId" : "s001"
            # ,"timeStamp" : "2021-02-18 11:29:03.942"
            # ,"value" : 12.31
        # }
        # recoveryDataList.append(recoveryData)
        # recoveryData = {
             # "setting" : "config/setting01.ini"
            # ,"deviceId" : "700400015-66DEF1DE"
            # ,"requestTimeStamp" : "2021-02-18 11:29:03.942"
            # ,"sensorId" : "s002"
            # ,"timeStamp" : "2021-02-18 11:29:03.942"
            # ,"value" : 23.56
        # }
        # recoveryDataList.append(recoveryData)
        # result = lambda_function.data_molding(recoveryDataList)
        # self.assertEqual(1, len(result))
        # self.assertEqual(1, result[0]["receveryFlg"])
        # self.assertEqual("config/setting01.ini", result[0]["setting"])
        # self.assertEqual("700400015-66DEF1DE", result[0]["deviceId"])
        # self.assertEqual("2021-02-18 11:29:03.942", result[0]["requestTimeStamp"])
        # self.assertEqual("s001", result[0]["records"][0]["sensorId"])
        # self.assertEqual("2021-02-18 11:29:03.942", result[0]["records"][0]["timeStamp"])
        # self.assertEqual(12.31, result[0]["records"][0]["value"])
        # self.assertEqual("s002", result[0]["records"][1]["sensorId"])
        # self.assertEqual("2021-02-18 11:29:03.942", result[0]["records"][1]["timeStamp"])
        # self.assertEqual(23.56, result[0]["records"][1]["value"])
        #
    # # ----------------------------------------------------------------------
    # # get_recovery_data()の正常系テスト
    # # データが正常に成形されていること。
    # # ----------------------------------------------------------------------
    # def test_data_molding_002(self):
        # lambda_function.initConfig("config/setting01.ini")
        # recoveryDataList = []
        # recoveryData = {
             # "setting" : "config/setting01.ini"
            # ,"deviceId" : "700400015-66DEF1DE"
            # ,"requestTimeStamp" : "2021-02-18 11:29:03.942"
            # ,"sensorId" : "s001"
            # ,"timeStamp" : "2021-02-18 11:29:03.942"
            # ,"value" : 12.31
        # }
        # recoveryDataList.append(recoveryData)
        # recoveryData = {
             # "setting" : "config/setting01.ini"
            # ,"deviceId" : "700400015-66DEF1DE"
            # ,"requestTimeStamp" : "2021-02-18 11:29:03.942"
            # ,"sensorId" : "s002"
            # ,"timeStamp" : "2021-02-18 11:29:03.942"
            # ,"value" : 23.56
        # }
        # recoveryDataList.append(recoveryData)
        # recoveryData = {
             # "setting" : "config/setting01.ini"
            # ,"deviceId" : "700400015-66DEF1DE"
            # ,"requestTimeStamp" : "2021-02-18 11:30:03.943"
            # ,"sensorId" : "s001"
            # ,"timeStamp" : "2021-02-18 11:30:03.943"
            # ,"value" : 12.32
        # }
        # recoveryDataList.append(recoveryData)
        # result = lambda_function.data_molding(recoveryDataList)
        # self.assertEqual(2, len(result))
        # self.assertEqual(1, result[0]["receveryFlg"])
        # self.assertEqual("config/setting01.ini", result[0]["setting"])
        # self.assertEqual("700400015-66DEF1DE", result[0]["deviceId"])
        # self.assertEqual("2021-02-18 11:29:03.942", result[0]["requestTimeStamp"])
        # self.assertEqual("s001", result[0]["records"][0]["sensorId"])
        # self.assertEqual("2021-02-18 11:29:03.942", result[0]["records"][0]["timeStamp"])
        # self.assertEqual(12.31, result[0]["records"][0]["value"])
        # self.assertEqual("s002", result[0]["records"][1]["sensorId"])
        # self.assertEqual("2021-02-18 11:29:03.942", result[0]["records"][1]["timeStamp"])
        # self.assertEqual(23.56, result[0]["records"][1]["value"])
        # self.assertEqual(1, result[1]["receveryFlg"])
        # self.assertEqual("config/setting01.ini", result[1]["setting"])
        # self.assertEqual("700400015-66DEF1DE", result[1]["deviceId"])
        # self.assertEqual("2021-02-18 11:30:03.943", result[1]["requestTimeStamp"])
        # self.assertEqual("s001", result[1]["records"][0]["sensorId"])
        # self.assertEqual("2021-02-18 11:30:03.943", result[1]["records"][0]["timeStamp"])
        # self.assertEqual(12.32, result[1]["records"][0]["value"])

if __name__ == '__main__':
    unittest.main()