import unittest
import lambda_function
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
    # 閾値成立回数:1
    # ----------------------------------------------------------------------
    def test_lambda_handler_001(self):
        print("---test_lambda_handler_001---")
        event = initCommon.readFileToJson('test/function/input001.json')

        # 時系列
        RDS.execute(initCommon.getQuery("test/sql/t_public_timeseries/delete.sql"))
        RDS.execute(initCommon.getQuery("test/sql/t_public_timeseries/upsert001.sql"))

        # マスタメンテ
        RDS.execute(initCommon.getQuery("test/sql/mastermainte/limitCheckUpsert_1_001.sql"))
        RDS.execute(initCommon.getQuery("test/sql/mastermainte/limitUpsert_1_001.sql"))
        RDS.execute(initCommon.getQuery("test/sql/mastermainte/limitCheckUpsert_4_001.sql"))
        RDS.execute(initCommon.getQuery("test/sql/mastermainte/limitUpsert_4_001.sql"))
        RDS.execute(initCommon.getQuery("test/sql/mastermainte/mailSendUpsert001.sql"))

        # 管理
        RDS.execute(initCommon.getQuery("test/sql/t_limit_hit_managed/delete.sql"))
        RDS.execute(initCommon.getQuery("test/sql/t_mail_send_managed/delete.sql"))

        RDS.commit()

        # 実行
        lambda_function.lambda_handler(event, None)
        
        # # 検証
        # limitHitManagedResult1 = RDS.fetchone("test/sql/t_limit_hit_managed/findbyId.sql"
                                              # , { "dataCollectionSeq" : 4
                                                 # , "detectionDateTime" : "2021/08/05 12:00:00.000"
                                                 # , "limitSubNo" : 1})
        # limitHitManagedResult2 = RDS.fetchone("test/sql/t_limit_hit_managed/findbyId.sql"
                                              # , { "dataCollectionSeq" : 4
                                                 # , "detectionDateTime" : "2021/08/05 12:00:50.000"
                                                 # , "limitSubNo" : 3})
        # mailSendManagedResult1 = RDS.fetchone("test/sql/t_mail_send_managed/findbyId.sql"
                                              # , { "dataCollectionSeq" : 4
                                                 # , "detectionDateTime" : "2021/08/05 12:00:00.000"
                                                 # , "limitSubNo" : 1})
        # mailSendManagedResult2 = RDS.fetchone("test/sql/t_mail_send_managed/findbyId.sql"
                                              # , { "dataCollectionSeq" : 4
                                                 # , "detectionDateTime" : "2021/08/05 12:00:50.000"
                                                 # , "limitSubNo" : 3})


    # # ----------------------------------------------------------------------
    # # 閾値成立回数条件:0（継続）
    # # 閾値成立回数:3
    # # ----------------------------------------------------------------------
    # def test_lambda_handler_002(self):
        # print("---test_lambda_handler_002---")
        # event = initCommon.readFileToJson('test/function/input001.json')
        #
        # # 時系列
        # RDS.execute(initCommon.getQuery("test/sql/t_public_timeseries/delete.sql"))
        # RDS.execute(initCommon.getQuery("test/sql/t_public_timeseries/upsert002.sql"))
        #
        # # マスタメンテ
        # RDS.execute(initCommon.getQuery("test/sql/mastermainte/limitCheckUpsert_1_001.sql"))
        # RDS.execute(initCommon.getQuery("test/sql/mastermainte/limitUpsert_1_001.sql"))
        # RDS.execute(initCommon.getQuery("test/sql/mastermainte/limitCheckUpsert_4_002.sql"))
        # RDS.execute(initCommon.getQuery("test/sql/mastermainte/limitUpsert_4_001.sql"))
        # RDS.execute(initCommon.getQuery("test/sql/mastermainte/mailSendUpsert001.sql"))
        #
        # # 管理
        # RDS.execute(initCommon.getQuery("test/sql/t_limit_hit_managed/delete.sql"))
        # RDS.execute(initCommon.getQuery("test/sql/t_mail_send_managed/delete.sql"))
        #
        # RDS.commit()
        #
        # # 実行
        # lambda_function.lambda_handler(event, None)
        #
    # # ----------------------------------------------------------------------
    # # 閾値成立回数条件:1（累積）
    # # 閾値成立回数:3
    # # 閾値成立回数リセット：5
    # # ----------------------------------------------------------------------
    # def test_lambda_handler_003(self):
        # print("---test_lambda_handler_003---")
        # event = initCommon.readFileToJson('test/function/input001.json')
        #
        # # 時系列
        # RDS.execute(initCommon.getQuery("test/sql/t_public_timeseries/delete.sql"))
        # RDS.execute(initCommon.getQuery("test/sql/t_public_timeseries/upsert003.sql"))
        #
        # # マスタメンテ
        # RDS.execute(initCommon.getQuery("test/sql/mastermainte/limitCheckUpsert_1_001.sql"))
        # RDS.execute(initCommon.getQuery("test/sql/mastermainte/limitUpsert_1_001.sql"))
        # RDS.execute(initCommon.getQuery("test/sql/mastermainte/limitCheckUpsert_4_003.sql"))
        # RDS.execute(initCommon.getQuery("test/sql/mastermainte/limitUpsert_4_001.sql"))
        # RDS.execute(initCommon.getQuery("test/sql/mastermainte/mailSendUpsert001.sql"))
        #
        # # 管理
        # RDS.execute(initCommon.getQuery("test/sql/t_limit_hit_managed/delete.sql"))
        # RDS.execute(initCommon.getQuery("test/sql/t_mail_send_managed/delete.sql"))
        #
        # RDS.commit()
        #
        # # 実行
        # lambda_function.lambda_handler(event, None)
        #
    # # ----------------------------------------------------------------------
    # # 閾値成立回数条件:1（累積）
    # # 閾値成立回数:3
    # # 閾値成立回数リセット：5
    # # 通知間隔:2
    # # 前回通知時刻2分前あり
    # # ----------------------------------------------------------------------
    # def test_lambda_handler_004(self):
        # print("---test_lambda_handler_004---")
        # event = initCommon.readFileToJson('test/function/input001.json')
        #
        # # 時系列
        # RDS.execute(initCommon.getQuery("test/sql/t_public_timeseries/delete.sql"))
        # RDS.execute(initCommon.getQuery("test/sql/t_public_timeseries/upsert003.sql"))
        #
        # # マスタメンテ
        # RDS.execute(initCommon.getQuery("test/sql/mastermainte/limitCheckUpsert_1_001.sql"))
        # RDS.execute(initCommon.getQuery("test/sql/mastermainte/limitUpsert_1_001.sql"))
        # RDS.execute(initCommon.getQuery("test/sql/mastermainte/limitCheckUpsert_4_003.sql"))
        # RDS.execute(initCommon.getQuery("test/sql/mastermainte/limitUpsert_4_001.sql"))
        # RDS.execute(initCommon.getQuery("test/sql/mastermainte/mailSendUpsert001.sql"))
        #
        # # 管理
        # RDS.execute(initCommon.getQuery("test/sql/t_limit_hit_managed/delete.sql"))
        # RDS.execute(initCommon.getQuery("test/sql/t_mail_send_managed/delete.sql"))
        #
        # RDS.commit()
        #
        # # 実行
        # lambda_function.lambda_handler(event, None)
