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

        # 事前にマスタ削除
        RDS.execute(initCommon.getQuery("test/sql/m_equipment/delete.sql"), {"equipmentId" : event["equipmentId"]})
        RDS.commit()
        
        # 実行
        lambda_function.lambda_handler(event, None)

        # assert用にselect
        result =RDS.fetchall(initCommon.getQuery("test/sql/m_equipment/findbyId.sql"),
                                                lambda_function.createWhereParam(event))


        self.assertEqual(result[0]["equipmentId"], "E0001")
        self.assertEqual(result[0]["equipmentName"], "設備名hoge1")
        self.assertEqual(result[0]["xCoordinate"], 123)
        self.assertEqual(result[0]["yCoordinate"], 456)
        self.assertEqual(result[0]["version"], 0)
        self.assertEqual(result[0]["deleteFlg"], 0)
        
    # ----------------------------------------------------------------------
    # 全項目ありの更新 (test_lambda_handler_001実施後に行うこと）
    # ----------------------------------------------------------------------
    def test_lambda_handler_002(self):
        print("---test_lambda_handler_002---")
        event = initCommon.readFileToJson('test/function/input002.json')

        # 実行
        lambda_function.lambda_handler(event, None)

        # assert用にselect
        result =RDS.fetchall(initCommon.getQuery("test/sql/m_equipment/findbyId.sql"),
                                                lambda_function.createWhereParam(event))


        self.assertEqual(result[0]["equipmentId"], "E0001")
        self.assertEqual(result[0]["equipmentName"], "設備名hoge1更新")
        self.assertEqual(result[0]["xCoordinate"], 1111.11)
        self.assertEqual(result[0]["yCoordinate"], 2222.22)
        self.assertEqual(result[0]["version"], 1)
        self.assertEqual(result[0]["deleteFlg"], 0)
        
    # ----------------------------------------------------------------------
    # 削除後の更新 (test_lambda_handler_002実施後に行うこと）
    # ----------------------------------------------------------------------
    def test_lambda_handler_003(self):
        print("---test_lambda_handler_003---")
        event = initCommon.readFileToJson('test/function/input003.json')

        # 事前にマスタ論理削除
        RDS.execute(initCommon.getQuery("test/sql/m_equipment/update.sql"), {"equipmentId" : event["equipmentId"]})
        RDS.commit()
        
        # 実行
        lambda_function.lambda_handler(event, None)

        # assert用にselect
        result =RDS.fetchall(initCommon.getQuery("test/sql/m_equipment/findbyId.sql"),
                                                lambda_function.createWhereParam(event))


        self.assertEqual(result[0]["equipmentId"], "E0001")
        self.assertEqual(result[0]["equipmentName"], "設備名hoge1更新2")
        self.assertEqual(result[0]["xCoordinate"], 333)
        self.assertEqual(result[0]["yCoordinate"], 444)
        self.assertEqual(result[0]["version"], 2)
        self.assertEqual(result[0]["deleteFlg"], 0)
        
    # ----------------------------------------------------------------------
    # 必須項目なし
    # ----------------------------------------------------------------------
    def test_lambda_handler_004(self):
        print("---test_lambda_handler_004---")
        event = initCommon.readFileToJson('test/function/input004.json')

        # 事前にマスタ論理削除
        RDS.execute(initCommon.getQuery("test/sql/m_equipment/update.sql"), {"equipmentId" : event["equipmentId"]})
        RDS.commit()
        
        # 実行
        lambda_function.lambda_handler(event, None)

        # assert用にselect
        result =RDS.fetchall(initCommon.getQuery("test/sql/m_equipment/findbyId.sql"),
                                                lambda_function.createWhereParam(event))


        self.assertEqual(result[0]["equipmentId"], "E0001")
        self.assertEqual(result[0]["equipmentName"], "設備名hoge1更新4")
        self.assertEqual(result[0]["xCoordinate"], None)
        self.assertEqual(result[0]["yCoordinate"], None)
        self.assertEqual(result[0]["version"], 3)
        self.assertEqual(result[0]["deleteFlg"], 0)
        
                
    # ----------------------------------------------------------------------
    # 数値項目でnull
    # ----------------------------------------------------------------------
    def test_lambda_handler_005(self):
        print("---test_lambda_handler_005---")
        event = initCommon.readFileToJson('test/function/input005.json')

        # 実行
        lambda_function.lambda_handler(event, None)

        # assert用にselect
        result =RDS.fetchall(initCommon.getQuery("test/sql/m_equipment/findbyId.sql"),
                                                lambda_function.createWhereParam(event))


        self.assertEqual(result[0]["equipmentId"], "E0001")
        self.assertEqual(result[0]["equipmentName"], "設備名hoge1更新5")
        self.assertEqual(result[0]["xCoordinate"], None)
        self.assertEqual(result[0]["yCoordinate"], None)
        self.assertEqual(result[0]["version"], 4)
        self.assertEqual(result[0]["deleteFlg"], 0)
    