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
from _ast import Try


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

        # 事前にマスタ追加
        RDS.execute(initCommon.getQuery("test/sql/delete_m_equipment.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_equipment01.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_data_collection_02.sql"))
        RDS.commit()
        
        # 実行
        lambda_function.lambda_handler(event, None)

        resultArray = RDS.fetchall(initCommon.getQuery("test/sql/findbyId.sql"),
                    {
                        "equipmentId" : event["equipmentId"]
                    })
        for result in resultArray:
            self.assertEqual(result["deleteFlg"], 1)
    
    # ----------------------------------------------------------------------
    #　追加→削除→追加→削除
    # ----------------------------------------------------------------------
    def test_lambda_handler_002(self):
        print("---test_lambda_handler_002---")
        event = initCommon.readFileToJson('test/function/input001.json')
    
        # 事前にマスタ追加
        RDS.execute(initCommon.getQuery("test/sql/delete_m_equipment.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_equipment01.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_data_collection_02.sql"))
        RDS.commit()
        
        # 実行
        lambda_function.lambda_handler(event, None)
        
        # 追加
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_equipment02.sql"))
        RDS.commit()
        
        # 実行
        lambda_function.lambda_handler(event, None)
        
    
        resultArray = RDS.fetchall(initCommon.getQuery("test/sql/findbyId.sql"),
                    {
                        "equipmentId" : event["equipmentId"]
                    })
        for result in resultArray:
            self.assertEqual(result["deleteFlg"], 1)
    
    # ----------------------------------------------------------------------
    #　子マスタあり
    # ----------------------------------------------------------------------
    def test_lambda_handler_003(self):
        print("---test_lambda_handler_003---")
    
        event = initCommon.readFileToJson('test/function/input001.json')
    
        # 事前にマスタ追加
        RDS.execute(initCommon.getQuery("test/sql/delete_m_equipment.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_equipment01.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_data_collection_01.sql"))
        RDS.commit()
        
        # 実行
        isException = False
        
        try:
            lambda_function.lambda_handler(event, None)
        except Exception  as e:
            isException = True
            print("---------- exception ----------\r\n%s" % e)
            
        self.assertTrue(isException)
        