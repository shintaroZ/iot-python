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
    # 送信曜日区分=0でメール送信されること。
    # ----------------------------------------------------------------------
    def test_lambda_handler_001(self):
        print("---test_lambda_handler_001---")
        event = initCommon.readFileToJson('test/function/input001.json')

        # マスタ
        RDS.execute(initCommon.getQuery("test/sql/m_mail_send/upsertFix001.sql"))

        # テストデータ
        RDS.execute(initCommon.getQuery("test/sql/t_mail_send_managed/delete.sql"))
        RDS.execute(initCommon.getQuery("test/sql/t_mail_send_managed/insertFix001.sql"))
        RDS.commit()

        # 実行
        lambda_function.lambda_handler(event, None)

        # ステータス
        result = RDS.fetchone(initCommon.getQuery("test/sql/t_mail_send_managed/findbyId.sql")
                              , {
                                  "dataCollectionSeq" : 224
                                  , "detectionDateTime" : '2021/07/20 13:00:00'
                                  , "limitSubNo" : 1
                                  , "mailSendSeq" : 0
                                })
        self.assertEqual(result["sendStatus"], 2)
#
    # # ----------------------------------------------------------------------
    # # 送信曜日区分=1で休祝日マスタに含まれない場合、メール送信されること。
    # # ----------------------------------------------------------------------
    # def test_lambda_handler_002(self):
        # print("---test_lambda_handler_002---")
        # event = initCommon.readFileToJson('test/function/input001.json')
        #
        # # マスタ
        # RDS.execute(initCommon.getQuery("test/sql/m_mail_send/upsertFix002.sql"))
        # RDS.execute(initCommon.getQuery("test/sql/m_holiday/deleteToday.sql"))
        #
        # # テストデータ
        # RDS.execute(initCommon.getQuery("test/sql/t_mail_send_managed/delete.sql"))
        # RDS.execute(initCommon.getQuery("test/sql/t_mail_send_managed/insertFix001.sql"))
        # RDS.commit()
        #
        # # 実行
        # lambda_function.lambda_handler(event, None)
        #
        # # ステータス
        # result = RDS.fetchone(initCommon.getQuery("test/sql/t_mail_send_managed/findbyId.sql")
                              # , {
                                  # "dataCollectionSeq" : 224
                                  # , "detectionDateTime" : '2021/07/20 13:00:00'
                                  # , "limitSubNo" : 1
                                  # , "mailSendSeq" : 0
                                # })
        # self.assertEqual(result["sendStatus"], 2)
        #
        #
    # # ----------------------------------------------------------------------
    # # 送信曜日区分=1で休祝日マスタに含まれる場合、メール送信されないこと。
    # # ----------------------------------------------------------------------
    # def test_lambda_handler_003(self):
        # print("---test_lambda_handler_003---")
        # event = initCommon.readFileToJson('test/function/input001.json')
        #
        # # マスタ
        # RDS.execute(initCommon.getQuery("test/sql/m_mail_send/upsertFix002.sql"))
        # RDS.execute(initCommon.getQuery("test/sql/m_holiday/upsertToday.sql"))
        #
        # # テストデータ
        # RDS.execute(initCommon.getQuery("test/sql/t_mail_send_managed/delete.sql"))
        # RDS.execute(initCommon.getQuery("test/sql/t_mail_send_managed/insertFix001.sql"))
        # RDS.commit()
        #
        # # 実行
        # lambda_function.lambda_handler(event, None)
        #
        # # ステータス
        # result = RDS.fetchone(initCommon.getQuery("test/sql/t_mail_send_managed/findbyId.sql")
                              # , {
                                  # "dataCollectionSeq" : 224
                                  # , "detectionDateTime" : '2021/07/20 13:00:00'
                                  # , "limitSubNo" : 1
                                  # , "mailSendSeq" : 0
                                # })
        # self.assertEqual(result["sendStatus"], 0)
        #
        # # 後処理
        # RDS.execute(initCommon.getQuery("test/sql/m_holiday/deleteToday.sql"))
        # RDS.commit()
        #
        #
    # # ----------------------------------------------------------------------
    # # 送信曜日区分=1で休祝日マスタに含まれない かつ 送信可能時間帯外の場合、メール送信されないこと。
    # # 23:00～翌0:00の実行は避けること。
    # # ----------------------------------------------------------------------
    # def test_lambda_handler_004(self):
        # print("---test_lambda_handler_004---")
        # event = initCommon.readFileToJson('test/function/input001.json')
        #
        # # マスタ
        # RDS.execute(initCommon.getQuery("test/sql/m_mail_send/upsertFix003.sql"))
        # RDS.execute(initCommon.getQuery("test/sql/m_holiday/deleteToday.sql"))
        #
        # # テストデータ
        # RDS.execute(initCommon.getQuery("test/sql/t_mail_send_managed/delete.sql"))
        # RDS.execute(initCommon.getQuery("test/sql/t_mail_send_managed/insertFix001.sql"))
        # RDS.commit()
        #
        # # 実行
        # lambda_function.lambda_handler(event, None)
        #
        # # ステータス
        # result = RDS.fetchone(initCommon.getQuery("test/sql/t_mail_send_managed/findbyId.sql")
                              # , {
                                  # "dataCollectionSeq" : 224
                                  # , "detectionDateTime" : '2021/07/20 13:00:00'
                                  # , "limitSubNo" : 1
                                  # , "mailSendSeq" : 0
                                # })
        # self.assertEqual(result["sendStatus"], 0)
        #
    # # ----------------------------------------------------------------------
    # # 置き換え文字列
    # # ----------------------------------------------------------------------
    # def test_lambda_handler_005(self):
        # print("---test_lambda_handler_005---")
        # event = initCommon.readFileToJson('test/function/input001.json')
        #
        # # マスタ
        # RDS.execute(initCommon.getQuery("test/sql/m_mail_send/upsertFix004.sql"))
        #
        # # テストデータ
        # RDS.execute(initCommon.getQuery("test/sql/t_mail_send_managed/delete.sql"))
        # RDS.execute(initCommon.getQuery("test/sql/t_mail_send_managed/insertFix002.sql"))
        # RDS.commit()
        #
        # # 実行
        # lambda_function.lambda_handler(event, None)

    # # ----------------------------------------------------------------------
    # # メールアドレス不正
    # # ----------------------------------------------------------------------
    # def test_lambda_handler_006(self):
        # print("---test_lambda_handler_006---")
        # event = initCommon.readFileToJson('test/function/input001.json')
        #
        # # マスタ
        # RDS.execute(initCommon.getQuery("test/sql/m_mail_send/upsertFix005.sql"))
        #
        # # テストデータ
        # RDS.execute(initCommon.getQuery("test/sql/t_mail_send_managed/delete.sql"))
        # RDS.execute(initCommon.getQuery("test/sql/t_mail_send_managed/insertFix003.sql"))
        # RDS.commit()
        #
        # # 実行
        # lambda_function.lambda_handler(event, None)
        #
        # # ステータス
        # result1 = RDS.fetchone(initCommon.getQuery("test/sql/t_mail_send_managed/findbyId.sql")
                              # , {
                                  # "dataCollectionSeq" : 224
                                  # , "detectionDateTime" : '2021/07/20 13:00:00'
                                  # , "limitSubNo" : 1
                                  # , "mailSendSeq" : 0
                                # })
        # # 異常でも正常値となる
        # self.assertEqual(result1["sendStatus"], 2)

