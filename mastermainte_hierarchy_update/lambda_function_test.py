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
        RDS.execute(initCommon.getQuery("test/sql/m_hierarchy/delete.sql"), {"hierarchyId" : event["hierarchyId"]})
        RDS.commit()
        
        # 実行
        lambda_function.lambda_handler(event, None)

        # assert用にselect
        result =RDS.fetchall(initCommon.getQuery("test/sql/m_hierarchy/findbyId.sql"),
                                                lambda_function.createWhereParam(event))


        self.assertEqual(result[0]["hierarchyId"], "TH0001")
        self.assertEqual(result[0]["hierarchyName"], "階層hoge1")
        self.assertEqual(result[0]["hierarchyLevel"], 1)
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
        result =RDS.fetchall(initCommon.getQuery("test/sql/m_hierarchy/findbyId.sql"),
                                                lambda_function.createWhereParam(event))


        self.assertEqual(result[0]["hierarchyId"], "TH0001")
        self.assertEqual(result[0]["hierarchyName"], "階層hoge1更新2")
        self.assertEqual(result[0]["hierarchyLevel"], 1)
        self.assertEqual(result[0]["version"], 1)
        self.assertEqual(result[0]["deleteFlg"], 0)
        
    # ----------------------------------------------------------------------
    # 削除後の更新 (test_lambda_handler_002実施後に行うこと）
    # ----------------------------------------------------------------------
    def test_lambda_handler_003(self):
        print("---test_lambda_handler_003---")
        event = initCommon.readFileToJson('test/function/input003.json')

        # 事前にマスタ論理削除
        RDS.execute(initCommon.getQuery("test/sql/m_hierarchy/update.sql"), {"hierarchyId" : event["hierarchyId"]})
        RDS.commit()
        
        # 実行
        lambda_function.lambda_handler(event, None)

        # assert用にselect
        result =RDS.fetchall(initCommon.getQuery("test/sql/m_hierarchy/findbyId.sql"),
                                                lambda_function.createWhereParam(event))


        self.assertEqual(result[0]["hierarchyId"], "TH0001")
        self.assertEqual(result[0]["hierarchyName"], "階層hoge1更新3")
        self.assertEqual(result[0]["hierarchyLevel"], 1)
        self.assertEqual(result[0]["version"], 2)
        self.assertEqual(result[0]["deleteFlg"], 0)
        
        

    