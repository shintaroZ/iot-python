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
import sys


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
    #  レコードなしの場合、空のrecords配列が返却されること。
    # ----------------------------------------------------------------------
    def test_lambda_handler_001(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)

        # 予めデータ削除
        RDS.execute(initCommon.getQuery("test/sql/m_collection_type_delete.sql"))
        RDS.commit()

        # 実行
        event = initCommon.readFileToJson('test/function/input001.json')
        result = lambda_function.lambda_handler(event, None)

        print ("================ result ================")
        print (result)
        jsonResult =  json.loads(result)
        
        self.assertEqual(len(jsonResult["records"]), 0)
        

    # ----------------------------------------------------------------------
    #  レコードありの場合、収集区分マスタのレコードが返却されること。
    # ----------------------------------------------------------------------
    def test_lambda_handler_002(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)

        # 予めデータ登録
        RDS.execute(initCommon.getQuery("test/sql/m_collection_type_upsert.sql"))
        RDS.commit()

        # 実行
        event = initCommon.readFileToJson('test/function/input001.json')
        result = lambda_function.lambda_handler(event, None)

        print ("================ result ================")
        print (result)
        jsonResult =  json.loads(result)
        
        self.assertEqual(jsonResult["records"][0].get("edgeType"), 1)
        self.assertEqual(jsonResult["records"][0].get("collectionType"), 1)
        self.assertEqual(jsonResult["records"][0].get("collectionTypeName"), "温度")
        self.assertEqual(jsonResult["records"][0].get("dispOrder"), 1)
        self.assertEqual(jsonResult["records"][0].get("createdAt"), "2022/04/08 15:29:09")
        self.assertEqual(jsonResult["records"][1].get("edgeType"), 1)
        self.assertEqual(jsonResult["records"][1].get("collectionType"), 2)
        self.assertEqual(jsonResult["records"][1].get("collectionTypeName"), "湿度")
        self.assertEqual(jsonResult["records"][1].get("dispOrder"), 2)
        self.assertEqual(jsonResult["records"][1].get("createdAt"), "2022/04/08 15:29:09")
        self.assertEqual(jsonResult["records"][2].get("edgeType"), 1)
        self.assertEqual(jsonResult["records"][2].get("collectionType"), 3)
        self.assertEqual(jsonResult["records"][2].get("collectionTypeName"), "音")
        self.assertEqual(jsonResult["records"][2].get("dispOrder"), 3)
        self.assertEqual(jsonResult["records"][2].get("createdAt"), "2022/04/08 15:29:09")
        self.assertEqual(jsonResult["records"][3].get("edgeType"), 1)
        self.assertEqual(jsonResult["records"][3].get("collectionType"), 4)
        self.assertEqual(jsonResult["records"][3].get("collectionTypeName"), "漏水")
        self.assertEqual(jsonResult["records"][3].get("dispOrder"), 4)
        self.assertEqual(jsonResult["records"][3].get("createdAt"), "2022/04/08 15:29:09")
        self.assertEqual(jsonResult["records"][4].get("edgeType"), 1)
        self.assertEqual(jsonResult["records"][4].get("collectionType"), 5)
        self.assertEqual(jsonResult["records"][4].get("collectionTypeName"), "照度")
        self.assertEqual(jsonResult["records"][4].get("dispOrder"), 5)
        self.assertEqual(jsonResult["records"][4].get("createdAt"), "2022/04/08 15:29:09")
        self.assertEqual(jsonResult["records"][5].get("edgeType"), 1)
        self.assertEqual(jsonResult["records"][5].get("collectionType"), 6)
        self.assertEqual(jsonResult["records"][5].get("collectionTypeName"), "二酸化炭素")
        self.assertEqual(jsonResult["records"][5].get("dispOrder"), 6)
        self.assertEqual(jsonResult["records"][5].get("createdAt"), "2022/04/08 15:29:09")
        self.assertEqual(jsonResult["records"][6].get("edgeType"), 1)
        self.assertEqual(jsonResult["records"][6].get("collectionType"), 7)
        self.assertEqual(jsonResult["records"][6].get("collectionTypeName"), "臭気")
        self.assertEqual(jsonResult["records"][6].get("dispOrder"), 7)
        self.assertEqual(jsonResult["records"][6].get("createdAt"), "2022/04/08 15:29:09")
        self.assertEqual(jsonResult["records"][7].get("edgeType"), 2)
        self.assertEqual(jsonResult["records"][7].get("collectionType"), 1)
        self.assertEqual(jsonResult["records"][7].get("collectionTypeName"), "音（スコア）")
        self.assertEqual(jsonResult["records"][7].get("dispOrder"), 1)
        self.assertEqual(jsonResult["records"][7].get("createdAt"), "2022/04/08 15:29:09")
        


