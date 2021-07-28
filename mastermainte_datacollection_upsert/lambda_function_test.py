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
        result =RDS.fetchone(initCommon.getQuery("test/sql/m_data_collection/findbyId.sql"),
                                                lambda_function.createWhereParam(event))

        if result is not None:
            dataCollectionSeq = result["dataCollectionSeq"]
            RDS.execute(initCommon.getQuery("test/sql/m_limit/delete.sql"),{"dataCollectionSeq" : dataCollectionSeq})
            RDS.execute(initCommon.getQuery("test/sql/m_limit_check/delete.sql"), {"dataCollectionSeq" : dataCollectionSeq})
            RDS.execute(initCommon.getQuery("test/sql/m_link_flg/delete.sql"), {"dataCollectionSeq" : dataCollectionSeq})
            RDS.execute(initCommon.getQuery("test/sql/m_data_collection/delete.sql"), {"deviceId" : event["deviceId"], "sensorId" : event["sensorId"]})
            RDS.commit()

        # 実行
        lambda_function.lambda_handler(event, None)

        # assert用にselect
        result =RDS.fetchall(initCommon.getQuery("test/sql/m_data_collection/findbyId.sql"),
                                                lambda_function.createWhereParam(event))

        dcSeqResult =RDS.fetchone(initCommon.getQuery("test/sql/m_seq_data_collection/find.sql"))
        lcSeqResult =RDS.fetchone(initCommon.getQuery("test/sql/m_seq_limit_check/find.sql"))

        self.assertEqual(result[0]["dataCollectionSeq"], dcSeqResult["seqNo"])

        self.assertEqual(result[0]["deviceId"], "700400014-F6CA332A")
        self.assertEqual(result[0]["sensorId"], "s1001")
        self.assertEqual(result[0]["sensorName"], "温度 〈センサ1〉")
        self.assertEqual(result[0]["sensorUnit"], "℃")
        self.assertEqual(result[0]["statusTrue"], "")
        self.assertEqual(result[0]["statusFalse"], "")
        self.assertEqual(result[0]["collectionValueType"], 0)
        self.assertEqual(result[0]["collectionType"], 1)
        self.assertEqual(result[0]["revisionMagnification"], 0.01)
        self.assertEqual(result[0]["xCoordinate"], 1234.56)
        self.assertEqual(result[0]["yCoordinate"], 2345.67)
        self.assertEqual(result[0]["savingFlg"], 0)
        self.assertEqual(result[0]["limitCheckFlg"], 1)
        self.assertEqual(result[0]["limitCountType"], 1)
        self.assertEqual(result[0]["limitCount"], 5)
        self.assertEqual(result[0]["limitCountResetRange"], 3)
        self.assertEqual(result[0]["actionRange"], 2)
        self.assertEqual(result[0]["nextAction"], 1)
        self.assertEqual(result[0]["limitSubNo"], 1)
        self.assertEqual(result[0]["limitJudgeType"], 2)
        self.assertEqual(result[0]["limitValue"], -12)
        self.assertEqual(result[1]["limitSubNo"], 2)
        self.assertEqual(result[1]["limitJudgeType"], 0)
        self.assertEqual(result[1]["limitValue"], 7)

    # ----------------------------------------------------------------------
    # データ定義マスタの項目なしの新規
    # ----------------------------------------------------------------------
    def test_lambda_handler_002(self):
        print("---test_lambda_handler_002---")
        event = initCommon.readFileToJson('test/function/input002.json')

        # 事前にマスタ削除
        result =RDS.fetchone(initCommon.getQuery("test/sql/m_data_collection/findbyId.sql"),
                                                lambda_function.createWhereParam(event))

        if result is not None:
            dataCollectionSeq = result["dataCollectionSeq"]
            RDS.execute(initCommon.getQuery("test/sql/m_limit/delete.sql"),{"dataCollectionSeq" : dataCollectionSeq})
            RDS.execute(initCommon.getQuery("test/sql/m_limit_check/delete.sql"), {"dataCollectionSeq" : dataCollectionSeq})
            RDS.execute(initCommon.getQuery("test/sql/m_link_flg/delete.sql"), {"dataCollectionSeq" : dataCollectionSeq})
            RDS.execute(initCommon.getQuery("test/sql/m_data_collection/delete.sql"), {"deviceId" : event["deviceId"], "sensorId" : event["sensorId"]})
            RDS.commit()

        # 実行
        lambda_function.lambda_handler(event, None)

        result =RDS.fetchall(initCommon.getQuery("test/sql/m_data_collection/findbyId.sql"),
                                                lambda_function.createWhereParam(event))

        dcSeqResult =RDS.fetchone(initCommon.getQuery("test/sql/m_seq_data_collection/find.sql"))
        lcSeqResult =RDS.fetchone(initCommon.getQuery("test/sql/m_seq_limit_check/find.sql"))


        self.assertEqual(result[0]["deviceId"], "700400014-F6CA332A")
        self.assertEqual(result[0]["sensorId"], "s1003")
        self.assertEqual(result[0]["dataCollectionSeq"], dcSeqResult["seqNo"])
        self.assertEqual(result[0]["sensorName"], "温度 〈センサ3〉")
        self.assertEqual(result[0]["sensorUnit"], "℃")
        self.assertEqual(result[0]["statusTrue"], "")
        self.assertEqual(result[0]["statusFalse"], "")
        self.assertEqual(result[0]["collectionValueType"], 0)
        self.assertEqual(result[0]["collectionType"], 1)
        self.assertEqual(result[0]["revisionMagnification"], 0.01)
        self.assertEqual(result[0]["xCoordinate"], None)
        self.assertEqual(result[0]["yCoordinate"], None)
        self.assertEqual(result[0]["savingFlg"], 0)
        self.assertEqual(result[0]["limitCheckFlg"], 1)
        self.assertEqual(result[0]["limitCountType"], 1)
        self.assertEqual(result[0]["limitCount"], 5)
        self.assertEqual(result[0]["limitCountResetRange"], 3)
        self.assertEqual(result[0]["actionRange"], 2)
        self.assertEqual(result[0]["nextAction"], 1)
        self.assertEqual(result[0]["limitSubNo"], 1)
        self.assertEqual(result[0]["limitJudgeType"], 2)
        self.assertEqual(result[0]["limitValue"], -12)
        self.assertEqual(result[1]["limitSubNo"], 2)
        self.assertEqual(result[1]["limitJudgeType"], 0)
        self.assertEqual(result[1]["limitValue"], 7)

        print (result)

    # ----------------------------------------------------------------------
    # 閾値なしの新規
    # ----------------------------------------------------------------------
    def test_lambda_handler_003(self):
        print("---test_lambda_handler_003---")
        event = initCommon.readFileToJson('test/function/input003.json')

        # 事前にマスタ削除
        result =RDS.fetchone(initCommon.getQuery("test/sql/m_data_collection/findbyId.sql"),
                                                lambda_function.createWhereParam(event))

        if result is not None:
            dataCollectionSeq = result["dataCollectionSeq"]
            RDS.execute(initCommon.getQuery("test/sql/m_limit/delete.sql"),{"dataCollectionSeq" : dataCollectionSeq})
            RDS.execute(initCommon.getQuery("test/sql/m_limit_check/delete.sql"), {"dataCollectionSeq" : dataCollectionSeq})
            RDS.execute(initCommon.getQuery("test/sql/m_link_flg/delete.sql"), {"dataCollectionSeq" : dataCollectionSeq})
            RDS.execute(initCommon.getQuery("test/sql/m_data_collection/delete.sql"), {"deviceId" : event["deviceId"], "sensorId" : event["sensorId"]})
            RDS.commit()

        # 実行
        lambda_function.lambda_handler(event, None)

        result =RDS.fetchall(initCommon.getQuery("test/sql/m_data_collection/findbyId.sql"),
                                                lambda_function.createWhereParam(event))

        self.assertEqual(result[0]["deviceId"], "700400014-F6CA332A")
        self.assertEqual(result[0]["sensorId"], "s1004")
        self.assertEqual(result[0]["sensorName"], "温度 〈センサ4〉")
        self.assertEqual(result[0]["sensorUnit"], "℃")
        self.assertEqual(result[0]["collectionValueType"], 0)
        self.assertEqual(result[0]["collectionType"], 1)
        self.assertEqual(result[0]["revisionMagnification"], 0.01)
        self.assertEqual(result[0]["savingFlg"], 0)
        print (result)

    # ----------------------------------------------------------------------
    # 全項目ありの更新
    # ----------------------------------------------------------------------
    def test_lambda_handler_004(self):
        print("---test_lambda_handler_004---")
        event = initCommon.readFileToJson('test/function/input004.json')

        # 実行
        lambda_function.lambda_handler(event, None)

        result =RDS.fetchall(initCommon.getQuery("test/sql/m_data_collection/findbyId.sql"),
                                                lambda_function.createWhereParam(event))
        self.assertEqual(result[0]["deviceId"], "700400014-F6CA332A")
        self.assertEqual(result[0]["sensorId"], "s1001")
        self.assertEqual(result[0]["sensorName"], "温度 〈センサ1〉")
        self.assertEqual(result[0]["sensorUnit"], "℃")
        self.assertEqual(result[0]["statusTrue"], "")
        self.assertEqual(result[0]["statusFalse"], "")
        self.assertEqual(result[0]["collectionValueType"], 0)
        self.assertEqual(result[0]["collectionType"], 1)
        self.assertEqual(result[0]["revisionMagnification"], 0.01)
        self.assertEqual(result[0]["savingFlg"], 0)
        self.assertEqual(result[0]["limitCheckFlg"], 1)
        self.assertEqual(result[0]["limitCountType"], 1)
        self.assertEqual(result[0]["limitCount"], 5)
        self.assertEqual(result[0]["limitCountResetRange"], 3)
        self.assertEqual(result[0]["actionRange"], 2)
        self.assertEqual(result[0]["nextAction"], 1)
        self.assertEqual(result[0]["limitSubNo"], 1)
        self.assertEqual(result[0]["limitJudgeType"], 2)
        self.assertEqual(result[0]["limitValue"], -12)
        self.assertEqual(result[1]["limitSubNo"], 2)
        self.assertEqual(result[1]["limitJudgeType"], 0)
        self.assertEqual(result[1]["limitValue"], 7)
        print (result)

    # ----------------------------------------------------------------------
    # データ定義マスタの項目なしの更新
    # ----------------------------------------------------------------------
    def test_lambda_handler_005(self):
        print("---test_lambda_handler_005---")
        event = initCommon.readFileToJson('test/function/input005.json')

        # 実行
        lambda_function.lambda_handler(event, None)

        result =RDS.fetchall(initCommon.getQuery("test/sql/m_data_collection/findbyId.sql"),
                                                lambda_function.createWhereParam(event))

        self.assertEqual(result[0]["deviceId"], "700400014-F6CA332A")
        self.assertEqual(result[0]["sensorId"], "s1003")
        self.assertEqual(result[0]["sensorName"], "温度 〈センサ3〉")
        self.assertEqual(result[0]["sensorUnit"], "℃")
        self.assertEqual(result[0]["collectionValueType"], 0)
        self.assertEqual(result[0]["collectionType"], 1)
        self.assertEqual(result[0]["revisionMagnification"], 0.01)
        self.assertEqual(result[0]["savingFlg"], 0)
        self.assertEqual(result[0]["limitCheckFlg"], 1)
        self.assertEqual(result[0]["limitCountType"], 1)
        self.assertEqual(result[0]["limitCount"], 5)
        self.assertEqual(result[0]["limitCountResetRange"], 3)
        self.assertEqual(result[0]["actionRange"], 2)
        self.assertEqual(result[0]["nextAction"], 1)
        self.assertEqual(result[0]["limitSubNo"], 1)
        self.assertEqual(result[0]["limitJudgeType"], 2)
        self.assertEqual(result[0]["limitValue"], -12)
        self.assertEqual(result[1]["limitSubNo"], 2)
        self.assertEqual(result[1]["limitJudgeType"], 0)
        self.assertEqual(result[1]["limitValue"], 9)
        print (result)

    # ----------------------------------------------------------------------
    # 閾値なしの更新
    # ----------------------------------------------------------------------
    def test_lambda_handler_006(self):
        print("---test_lambda_handler_006---")
        event = initCommon.readFileToJson('test/function/input006.json')

        # 実行
        lambda_function.lambda_handler(event, None)

        result =RDS.fetchall(initCommon.getQuery("test/sql/m_data_collection/findbyId.sql"),
                                                lambda_function.createWhereParam(event))
        self.assertEqual(result[0]["deviceId"], "700400014-F6CA332A")
        self.assertEqual(result[0]["sensorId"], "s1004")
        self.assertEqual(result[0]["sensorName"], "温度 〈センサ4〉")
        self.assertEqual(result[0]["sensorUnit"], "℃")
        self.assertEqual(result[0]["collectionValueType"], 0)
        self.assertEqual(result[0]["collectionType"], 1)
        self.assertEqual(result[0]["revisionMagnification"], 0.01)
        self.assertEqual(result[0]["savingFlg"], 1)
        print (result)

    # ----------------------------------------------------------------------
    # データ定義マスタの必須項目なし
    # ----------------------------------------------------------------------
    def test_lambda_handler_007(self):
        print("---test_lambda_handler_007---")
        event = initCommon.readFileToJson('test/function/input007.json')

        isException = False
        try:
            lambda_function.lambda_handler(event, None)
        except Exception as ex:
            print(ex)
            isException = True
        self.assertEqual(isException, True)

    # ----------------------------------------------------------------------
    # 閾値マスタの歯抜けあり
    # ----------------------------------------------------------------------
    def test_lambda_handler_008(self):
        print("---test_lambda_handler_008---")
        event = initCommon.readFileToJson('test/function/input008.json')

        isException = False
        try:
            lambda_function.lambda_handler(event, None)
        except Exception as ex:
            print(ex)
            isException = True
        self.assertEqual(isException, True)

    # ----------------------------------------------------------------------
    # データ型不正
    # ----------------------------------------------------------------------
    def test_lambda_handler_009(self):
        print("---test_lambda_handler_009---")
        event = initCommon.readFileToJson('test/function/input009.json')

        isException = False
        try:
            lambda_function.lambda_handler(event, None)
        except Exception as ex:
            print(ex)
            isException = True
        self.assertEqual(isException, True)
    # ----------------------------------------------------------------------
    # データ長不正
    # ----------------------------------------------------------------------
    def test_lambda_handler_010(self):
        print("---test_lambda_handler_010---")
        event = initCommon.readFileToJson('test/function/input010.json')

        isException = False
        try:
            lambda_function.lambda_handler(event, None)
        except Exception as ex:
            print(ex)
            isException = True
        self.assertEqual(isException, True)