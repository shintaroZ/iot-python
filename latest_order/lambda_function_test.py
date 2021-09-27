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
    # getMasterDataCollection()の正常系テスト
    # 該当データありの場合、レコード情報が返却されること。
    # ----------------------------------------------------------------------
    def test_getMasterSensorDistribution_001(self):
        print("---test_getMasterSensorDistribution_001---")
        event = initCommon.readFileToJson('test/function/input001.json')
        
        # マスタメンテナンス
        RDS.execute(initCommon.getQuery("test/sql/delete_m_data_collection.sql"))
        RDS.execute(initCommon.getQuery("test/sql/delete_m_link_flg.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_data_collection_Fix001.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_link_flg_Fix001.sql"))
        RDS.commit()
        
        result = lambda_function.getMasterDataCollection(RDS, "700400015-66DEF1DE", "s001")
        self.assertEqual(result['sensorName'], "温度testSensor")

    # ----------------------------------------------------------------------
    # getMasterDataCollection()の正常系テスト
    # 該当データなしの場合、処理が終了すること。
    # ----------------------------------------------------------------------
    def test_getMasterSensorDistribution_002(self):
        print("---test_getMasterSensorDistribution_002---")
        with self.assertRaises(SystemExit):
            lambda_function.getMasterDataCollection(RDS, "700400015-66DEF1DE", "xxxxx")

    # 行ロック廃止に伴いテストも廃止
    # ----------------------------------------------------------------------
    # getMasterDataCollection()の正常系テスト
    # 行ロック
    # ----------------------------------------------------------------------
    # def test_getMasterSensorDistribution_003(self):
        # print("---test_getMasterSensorDistribution_003---")
        # event = initCommon.readFileToJson('test/function/input001.json')
        #
        # # 予め別コネクションで行ロックする
        # con2 = pymysql.connect(host=lambda_function.DB_HOST, port=lambda_function.DB_PORT, user=lambda_function.DB_USER, passwd=lambda_function.DB_PASSWORD, db=lambda_function.DB_NAME, connect_timeout=lambda_function.DB_CONNECT_TIMEOUT)
        # params = {
            # "p_deviceId": "700400015-66DEF1DE"
            # , "p_sensorId": "s001"
        # }
        # query = initCommon.getQuery("sql/m_data_collection/findbyId.sql")
        #
        # with con2.cursor(pymysql.cursors.DictCursor) as cur:
            # cur.execute("set innodb_lock_wait_timeout = 3")
            # cur.execute("begin")
            # cur.execute(query % params)
            #
        # with self.assertRaises(Exception):
            # lambda_function.getMasterDataCollection(RDS, "700400015-66DEF1DE", "s001")

    # ----------------------------------------------------------------------
    # lambda_handler()の正常系テスト
    # 公開DBが未登録の場合レコードが新規登録されること。
    # ----------------------------------------------------------------------
    def test_lambda_handler_001(self):
        print("---test_lambda_handler_001---")

        # マスタメンテナンス
        RDS.execute(initCommon.getQuery("test/sql/delete_m_data_collection.sql"))
        RDS.execute(initCommon.getQuery("test/sql/delete_m_link_flg.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_data_collection_Fix001.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_link_flg_Fix001.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_data_collection_Fix002.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_link_flg_Fix002.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_data_collection_Fix005.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_link_flg_Fix005.sql"))
        RDS.commit()

        # 公開DBクリア
        RDS.execute(initCommon.getQuery("test/sql/t_public_timeseries/delete.sql")
                    , { "tableName": "T_PUBLIC_TIMESERIES"
                    , "receivedDatetimeBefore": "2021/02/16 00:00:00"
                    , "receivedDatetimeAfter": "2021/02/16 23:59:59" })
        RDS.commit()

        # 実行
        event = initCommon.readFileToJson('test/function/input001.json')
        lambda_function.lambda_handler(event, None)

        # assert用にSelect
        result1 = RDS.fetchone(initCommon.getQuery("test/sql/t_public_timeseries/select.sql")
                                , { "p_dataCollectionSeq": 4
                                   , "p_receivedDateTime": "2021/02/16 21:56:38.895" })
        result2 = RDS.fetchone(initCommon.getQuery("test/sql/t_public_timeseries/select.sql")
                                , { "p_dataCollectionSeq": 1
                                   , "p_receivedDateTime": "2021/02/16 21:56:38.895" })
        result3 = RDS.fetchone(initCommon.getQuery("test/sql/t_public_timeseries/select.sql")
                                , { "p_dataCollectionSeq": 4
                                   , "p_receivedDateTime": "2021/02/16 21:57:38.895" })
        self.assertEqual(result1["sensorValue"], 12.31)
        self.assertEqual(result2["sensorValue"], 23.56)
        self.assertEqual(result3["sensorValue"], 43.21)

    # ----------------------------------------------------------------------
    # lambda_handler()の正常系テスト
    # 公開DBが登録済みの場合、監視テーブルに登録されること。
    # ----------------------------------------------------------------------
    def test_lambda_handler_002(self):
        print("---test_lambda_handler_002---")

        # マスタメンテナンス
        RDS.execute(initCommon.getQuery("test/sql/delete_m_data_collection.sql"))
        RDS.execute(initCommon.getQuery("test/sql/delete_m_link_flg.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_data_collection_Fix001.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_link_flg_Fix001.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_data_collection_Fix002.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_link_flg_Fix002.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_data_collection_Fix005.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_link_flg_Fix005.sql"))
        RDS.commit()
        
        # 公開DB登録
        RDS.execute(initCommon.getQuery("test/sql/t_public_timeseries/upsert.sql")
                    , { "p_dataCollectionSeq": 0
                       , "p_receivedDateTime": "2021/02/16 21:56:38.895"
                       , "p_sensorValue": 0.12
                       , "p_createdDateTime": "2021/02/16 00:00:00" }
                    )
        RDS.commit()

        # 実行
        startDt = initCommon.getSysDateJst()
        event = initCommon.readFileToJson('test/function/input001.json')
        lambda_function.lambda_handler(event, None)
        endDt = initCommon.getSysDateJst()

        # assert用にSelect
        result1 = RDS.fetchone(initCommon.getQuery("test/sql/t_surveillance/select.sql")
                                , { "p_occurredDateTimeStart": startDt
                                   , "p_occurredDateTimeEnd":endDt})
        self.assertEqual(result1["functionName"], "公開DB作成機能")

    # ----------------------------------------------------------------------
    # lambda_handler()の正常系テスト
    # リカバリ用
    # ----------------------------------------------------------------------
    def test_lambda_handler_003(self):
        print("---test_lambda_handler_003---")

        # マスタメンテナンス
        RDS.execute(initCommon.getQuery("test/sql/delete_m_data_collection.sql"))
        RDS.execute(initCommon.getQuery("test/sql/delete_m_link_flg.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_data_collection_Fix001.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_link_flg_Fix001.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_data_collection_Fix002.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_link_flg_Fix002.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_data_collection_Fix005.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_link_flg_Fix005.sql"))
        RDS.commit()
        
        # 公開DBクリア
        RDS.execute(initCommon.getQuery("test/sql/t_public_timeseries/delete.sql")
                    , { "tableName": "T_PUBLIC_TIMESERIES"
                    , "receivedDatetimeBefore": "2021/07/05 00:00:00"
                    , "receivedDatetimeAfter": "2021/07/05 23:59:59" })
        RDS.commit()

        # 実行
        event = initCommon.readFileToJson('test/function/input003.json')
        result = lambda_function.lambda_handler(event, None)

        result1 = RDS.fetchone(initCommon.getQuery("test/sql/t_public_timeseries/select.sql")
                                , { "p_dataCollectionSeq": 4
                                   , "p_receivedDateTime": "2021/07/05 12:00:00" })

        result2 = RDS.fetchone(initCommon.getQuery("test/sql/t_public_timeseries/select.sql")
                                , { "p_dataCollectionSeq": 1
                                   , "p_receivedDateTime": "2021/07/05 12:00:00" })

        result3 = RDS.fetchone(initCommon.getQuery("test/sql/t_public_timeseries/select.sql")
                                , { "p_dataCollectionSeq": 4
                                   , "p_receivedDateTime": "2021/07/05 12:10:00" })

        result4 = RDS.fetchone(initCommon.getQuery("test/sql/t_public_timeseries/select.sql")
                                , { "p_dataCollectionSeq": 4
                                   , "p_receivedDateTime": "2021/07/05 12:30:00" })

        result5 = RDS.fetchone(initCommon.getQuery("test/sql/t_public_timeseries/select.sql")
                                , { "p_dataCollectionSeq": 1
                                   , "p_receivedDateTime": "2021/07/05 12:30:00" })

        result6 = RDS.fetchone(initCommon.getQuery("test/sql/t_public_timeseries/select.sql")
                                , { "p_dataCollectionSeq": 4
                                   , "p_receivedDateTime": "2021/07/05 12:40:00" })

        self.assertEqual(result1["sensorValue"], 12.34)
        self.assertEqual(result2["sensorValue"], 23.45)
        self.assertEqual(result3["sensorValue"], 34.56)
        self.assertEqual(result4["sensorValue"], 11.11)
        self.assertEqual(result5["sensorValue"], 22.22)
        self.assertEqual(result6["sensorValue"], 33.33)

        print ("============ result ============")
        print (result)

    # ----------------------------------------------------------------------
    # lambda_handler()の正常系テスト
    # s003の蓄積対象外の場合、dbに登録されないこと
    # s004のBoolean型の場合、Falseは0、Trueは1で登録されること。
    # ----------------------------------------------------------------------
    def test_lambda_handler_004(self):
        print("---test_lambda_handler_004---")

        # マスタメンテナンス
        RDS.execute(initCommon.getQuery("test/sql/delete_m_data_collection.sql"))
        RDS.execute(initCommon.getQuery("test/sql/delete_m_link_flg.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_data_collection_Fix001.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_link_flg_Fix001.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_data_collection_Fix002.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_link_flg_Fix002.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_data_collection_Fix003.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_link_flg_Fix003.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_data_collection_Fix004.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_link_flg_Fix004.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_data_collection_Fix005.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_link_flg_Fix005.sql"))
        RDS.commit()
        
        # 公開DBクリア
        RDS.execute(initCommon.getQuery("test/sql/t_public_timeseries/delete.sql")
                    , { "tableName": "T_PUBLIC_TIMESERIES"
                    , "receivedDatetimeBefore": "2021/07/05 00:00:00"
                    , "receivedDatetimeAfter": "2021/07/05 23:59:59" })
        RDS.commit()

        # 実行
        event = initCommon.readFileToJson('test/function/input004.json')
        result = lambda_function.lambda_handler(event, None)

        result1 = RDS.fetchone(initCommon.getQuery("test/sql/t_public_timeseries/select.sql")
                                , { "p_dataCollectionSeq": 2
                                   , "p_receivedDateTime": "2021/07/05 12:00:00" })

        result2 = RDS.fetchone(initCommon.getQuery("test/sql/t_public_timeseries/select.sql")
                                , { "p_dataCollectionSeq": 3
                                   , "p_receivedDateTime": "2021/07/05 12:10:00" })

        result3 = RDS.fetchone(initCommon.getQuery("test/sql/t_public_timeseries/select.sql")
                                , { "p_dataCollectionSeq": 3
                                   , "p_receivedDateTime": "2021/07/05 12:12:00" })
        self.assertEqual(result1, None)
        self.assertEqual(result2["sensorValue"], 1)
        self.assertEqual(result3["sensorValue"], 0)

        print ("============ result ============")
        print (result)


    # ----------------------------------------------------------------------
    # lambda_handler()の異常系テスト
    # 数値、bool型以外の値が来た場合、監視テーブルへ登録されること。
    # ----------------------------------------------------------------------
    def test_lambda_handler_005(self):
        print("---test_lambda_handler_005---")

        # マスタメンテナンス
        RDS.execute(initCommon.getQuery("test/sql/delete_m_data_collection.sql"))
        RDS.execute(initCommon.getQuery("test/sql/delete_m_link_flg.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_data_collection_Fix001.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_link_flg_Fix001.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_data_collection_Fix002.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_link_flg_Fix002.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_data_collection_Fix003.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_link_flg_Fix003.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_data_collection_Fix004.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_link_flg_Fix004.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_data_collection_Fix005.sql"))
        RDS.execute(initCommon.getQuery("test/sql/upsert_m_link_flg_Fix005.sql"))
        RDS.commit()
        
        # 公開DBクリア
        RDS.execute(initCommon.getQuery("test/sql/t_public_timeseries/delete.sql")
                    , { "tableName": "T_PUBLIC_TIMESERIES"
                    , "receivedDatetimeBefore": "2021/07/05 00:00:00"
                    , "receivedDatetimeAfter": "2021/07/05 23:59:59" })
        RDS.commit()

        # 実行
        startDt = initCommon.getSysDateJst()
        event = initCommon.readFileToJson('test/function/input005.json')
        result = lambda_function.lambda_handler(event, None)
        endDt = initCommon.getSysDateJst()

        # assert用にSelect
        result1 = RDS.fetchall(initCommon.getQuery("test/sql/t_surveillance/select.sql")
                                , { "p_occurredDateTimeStart": startDt
                                   , "p_occurredDateTimeEnd":endDt})

        self.assertEqual(result1[0]["message"], "センサの値が不正です。(デバイスID:700400015-66DEF1DE 送信日時:2021-07-05 12:00:00.000 センサID:s001 値:XXXXX)")
        self.assertEqual(result1[1]["message"], "センサの値が不正です。(デバイスID:700400015-66DEF1DE 送信日時:2021-07-05 12:00:00.000 センサID:s004 値:ZZZZZ)")

        print ("============ result ============")
        print (result)

