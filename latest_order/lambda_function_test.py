import unittest
import datetime
from datetime import datetime as dt
import json
from collections import OrderedDict
import pprint
import lambda_function
import pymysql
import initCommon # カスタムレイヤー
import rdsCommon # カスタムレイヤー

def createEvent(query_file_path):
    f = open(query_file_path, 'r')
    return json.load(f)

class LambdaFunctionTest(unittest.TestCase):

    RDS = None

    # テスト開始時に1回だけ呼び出される
    # クラスメソッドとして定義する
    @classmethod
    def setUpClass(cls):
        global RDS
        lambda_function.initConfig("config/setting01.ini")
        lambda_function.setLogger(initCommon.getLogger(lambda_function.LOG_LEVEL))
        RDS = rdsCommon.rdsCommon(lambda_function.LOGGER
                                ,lambda_function.DB_HOST
                                ,lambda_function.DB_PORT
                                ,lambda_function.DB_USER
                                ,lambda_function.DB_PASSWORD
                                ,lambda_function.DB_NAME
                                ,lambda_function.DB_CONNECT_TIMEOUT)


    # ----------------------------------------------------------------------
    # getMasterSensorDistribution()の正常系テスト
    # 該当データありの場合、レコード情報が返却されること。
    # ----------------------------------------------------------------------
    def xtest_getMasterSensorDistribution_001(self):
        event = createEvent('test/function/input001.json')
        result = lambda_function.getMasterSensorDistribution(RDS, "700400015-66DEF1DE","s001")
        self.assertEqual(result['sensorName'], "温度センサ")

    # ----------------------------------------------------------------------
    # getMasterSensorDistribution()の正常系テスト
    # 該当データなしの場合、処理が終了すること。
    # ----------------------------------------------------------------------
    def xtest_getMasterSensorDistribution_002(self):
        event = createEvent('test/function/input001.json')
        with self.assertRaises(SystemExit):
            lambda_function.getMasterSensorDistribution(RDS, "700400015-66DEF1DE","xxxxx")

    # ----------------------------------------------------------------------
    # getMasterSensorDistribution()の正常系テスト
    # 行ロック
    # ----------------------------------------------------------------------
    def xtest_getMasterSensorDistribution_003(self):
        event = createEvent('test/function/input001.json')

        # 予め別コネクションで行ロックする
        con2 = pymysql.connect(host=lambda_function.DB_HOST, port=lambda_function.DB_PORT, user=lambda_function.DB_USER, passwd=lambda_function.DB_PASSWORD, db=lambda_function.DB_NAME, connect_timeout=lambda_function.DB_CONNECT_TIMEOUT)
        params = {
            "p_deviceId": "700400015-66DEF1DE"
            ,"p_sensorId": "s001"
        }
        query = initCommon.getQuery("sql/m_senrosr_distribution/findbyId.sql")

        with con2.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute("set innodb_lock_wait_timeout = 3")
            cur.execute("begin")
            cur.execute(query % params)

        with self.assertRaises(Exception):
            lambda_function.getMasterSensorDistribution(RDS, "700400015-66DEF1DE","s001")

    # ----------------------------------------------------------------------
    # lambda_handler()の正常系テスト
    # 公開DBが未登録の場合レコードが新規登録されること。
    # ----------------------------------------------------------------------
    def xtest_lambda_handler_001(self):
        print("---test_lambda_handler_001---")

        # 公開DBクリア
        RDS.execute(initCommon.getQuery("test/sql/t_public_timeseries/delete.sql")
                    , { "tableName" : "T_PUBLIC_TIMESERIES"
                    ,"receivedDatetimeBefore" : "2021/02/16 00:00:00"
                    ,"receivedDatetimeAfter" : "2021/02/16 23:59:59" })
        RDS.commit()

        # 実行
        event = createEvent('test/function/input001.json')
        lambda_function.lambda_handler(event, None)

        # assert用にSelect
        result1 = RDS.fetchone(initCommon.getQuery("test/sql/t_public_timeseries/select.sql")
                                , { "p_collectionType" : "1"
                                   ,"p_receivedDateTime" : "2021/02/16 21:56:38.895"
                                   ,"p_sensorName" : "温度センサ" })
        result2 = RDS.fetchone(initCommon.getQuery("test/sql/t_public_timeseries/select.sql")
                                , { "p_collectionType" : "2"
                                   ,"p_receivedDateTime" : "2021/02/16 21:56:38.895"
                                   ,"p_sensorName" : "湿度センサ" })
        result3 = RDS.fetchone(initCommon.getQuery("test/sql/t_public_timeseries/select.sql")
                                , { "p_collectionType" : "1"
                                   ,"p_receivedDateTime" : "2021/02/16 21:57:38.895"
                                   ,"p_sensorName" : "温度センサ" })
        self.assertEqual(result1["sensorValue"], 12.31 )
        self.assertEqual(result2["sensorValue"], 23.56 )
        self.assertEqual(result3["sensorValue"], 43.21 )

    # ----------------------------------------------------------------------
    # lambda_handler()の正常系テスト
    # 公開DBが登録済みの場合、監視テーブルに登録されること。
    # ----------------------------------------------------------------------
    def xtest_lambda_handler_002(self):
        print("---test_lambda_handler_002---")

        # 公開DB登録
        RDS.execute(initCommon.getQuery("test/sql/t_public_timeseries/upsert.sql")
                    , { "p_collectionType" : "1"
                       ,"p_receivedDateTime" : "2021/02/16 21:56:38.895"
                       ,"p_sensorName" : "温度センサ"
                       ,"p_sensorValue" : 0.12
                       ,"p_sensorUnit" : "℃"
                       ,"p_createdDateTime" : "2021/02/16 00:00:00" }
                    )
        RDS.commit()

        # 実行
        startDt = initCommon.getSysDateJst()
        event = createEvent('test/function/input001.json')
        lambda_function.lambda_handler(event, None)
        endDt = initCommon.getSysDateJst()

        # assert用にSelect
        result1 = RDS.fetchone(initCommon.getQuery("test/sql/t_surveillance/select.sql")
                                , { "p_occurredDateTimeStart" : startDt
                                   ,"p_occurredDateTimeEnd" :endDt})
        self.assertEqual(result1["functionName"], "公開DB作成機能" )

   # ----------------------------------------------------------------------
    # lambda_handler()の正常系テスト
    # 公開DBが登録済みの場合、監視テーブルに登録されること。
    # ----------------------------------------------------------------------
    def test_getEventListReConv_001(self):
        print("---test_getEventListReConv_001---")

        event = createEvent('test/function/input003.json')

        result = lambda_function.getEventListReConv(event)

        print (result)

# if __name__ == "__main__":
#     unittest.main()