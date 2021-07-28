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
    #　追加→削除
    # ----------------------------------------------------------------------
    def test_lambda_handler_001(self):
        print("---test_lambda_handler_001---")
        event = initCommon.readFileToJson('test/function/input001.json')

        # 予め更新
        RDS.execute(initCommon.getQuery("test/sql/delete_m_data_collection.sql"))
        RDS.execute(initCommon.getQuery("test/sql/delete_m_limit_check.sql"))
        RDS.execute(initCommon.getQuery("test/sql/delete_m_limit.sql"))
        RDS.execute(initCommon.getQuery("test/sql/delete_m_link_flg.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_data_collection_Fix001.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_limit_check_Fix001.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_link_flg_Fix001.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_limit_Fix001.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_limit_Fix002.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_data_collection_Fix011.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_limit_check_Fix011.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_link_flg_Fix011.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_limit_Fix011.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_limit_Fix012.sql"))
        RDS.commit()

        # 実行
        lambda_function.lambda_handler(event, None)

        resultArray = RDS.fetchall(initCommon.getQuery("test/sql/findbyId.sql"),
                    {
                        "deviceId" : event["deviceId"]
                     ,  "sensorId" : event["sensorId"]
                    })
        for result in resultArray:
            self.assertEqual(result["deleteFlg"], 1)

    # ----------------------------------------------------------------------
    #　追加→削除→追加→削除
    # ----------------------------------------------------------------------
    def test_lambda_handler_002(self):
        print("---test_lambda_handler_002---")
        event = initCommon.readFileToJson('test/function/input001.json')

        # 予め更新
        RDS.execute(initCommon.getQuery("test/sql/delete_m_data_collection.sql"))
        RDS.execute(initCommon.getQuery("test/sql/delete_m_limit_check.sql"))
        RDS.execute(initCommon.getQuery("test/sql/delete_m_limit.sql"))
        RDS.execute(initCommon.getQuery("test/sql/delete_m_link_flg.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_data_collection_Fix001.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_limit_check_Fix001.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_link_flg_Fix001.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_limit_Fix001.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_limit_Fix002.sql"))
        RDS.commit()

        # 実行
        lambda_function.lambda_handler(event, None)

        # 追加
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_data_collection_Fix011.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_limit_check_Fix011.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_link_flg_Fix011.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_limit_Fix011.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_limit_Fix012.sql"))

        # 実行
        lambda_function.lambda_handler(event, None)


        resultArray = RDS.fetchall(initCommon.getQuery("test/sql/findbyId.sql"),
                    {
                        "deviceId" : event["deviceId"]
                     ,  "sensorId" : event["sensorId"]
                    })
        for result in resultArray:
            self.assertEqual(result["deleteFlg"], 1)

    # ----------------------------------------------------------------------
    #　削除対象なし
    # ----------------------------------------------------------------------
    def test_lambda_handler_003(self):
        print("---test_lambda_handler_003---")

        event = initCommon.readFileToJson('test/function/input002.json')


        # 実行
        lambda_function.lambda_handler(event, None)

        resultArray = RDS.fetchall(initCommon.getQuery("test/sql/findbyId.sql"),
                    {
                        "deviceId" : event["deviceId"]
                     ,  "sensorId" : event["sensorId"]
                    })
        self.assertEqual(len(resultArray), 0)


