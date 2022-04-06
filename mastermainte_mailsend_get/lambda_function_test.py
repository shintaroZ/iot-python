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
    # 追加⇨追加
    # ----------------------------------------------------------------------
    def test_lambda_handler_001(self):
        print("---test_lambda_handler_001---")
        event = initCommon.readFileToJson('test/function/input001.json')
    
        # ２世代追加
        RDS.execute(initCommon.getQuery("test/sql/m_mail_send/delete.sql"), { "mailSendId" : 1 })
        RDS.execute(initCommon.getQuery("test/sql/m_mail_send/insertFix001.sql"))
        RDS.execute(initCommon.getQuery("test/sql/m_mail_send/insertFix002.sql"))
        RDS.commit()
        
        # 実行
        result = lambda_function.lambda_handler(event, None)

        print ("================ result ================")
        print (result)
        
        resultJson = json.loads(result)
        for r in resultJson["records"]:
            if r["mailSendId"] == 1:
                self.assertEqual(r["version"], 1)
                self.assertEqual(r["mailSubject"], "埋め込みサンプル件名")

    # ----------------------------------------------------------------------
    # 追加⇨削除
    # ----------------------------------------------------------------------
    def test_lambda_handler_002(self):
        print("---test_lambda_handler_002---")
        event = initCommon.readFileToJson('test/function/input001.json')
        
        # 追加⇨削除
        RDS.execute(initCommon.getQuery("test/sql/m_mail_send/delete.sql"), { "mailSendId" : 1 })
        RDS.execute(initCommon.getQuery("test/sql/m_mail_send/insertFix001.sql"))
        RDS.execute(initCommon.getQuery("test/sql/m_mail_send/update.sql"), { "mailSendId" : 1 })
        RDS.commit()
        
        # 実行
        result = lambda_function.lambda_handler(event, None)

        print ("================ result ================")
        print (result)
        
        resultJson = json.loads(result)
        isNotRecord = False
        for r in resultJson["records"]:
            if r["mailSendId"] == 1:
                isNotRecord = True
        self.assertEqual(isNotRecord, False)
        
    # ----------------------------------------------------------------------
    # 追加⇨削除⇨追加
    # ----------------------------------------------------------------------
    def test_lambda_handler_003(self):
        print("---test_lambda_handler_003---")
        event = initCommon.readFileToJson('test/function/input001.json')
        
        # 追加⇨削除
        RDS.execute(initCommon.getQuery("test/sql/m_mail_send/delete.sql"), { "mailSendId" : 1 })
        RDS.execute(initCommon.getQuery("test/sql/m_mail_send/insertFix001.sql"))
        RDS.execute(initCommon.getQuery("test/sql/m_mail_send/update.sql"), { "mailSendId" : 1 })
        RDS.execute(initCommon.getQuery("test/sql/m_mail_send/insertFix002.sql"))
        RDS.commit()
        
        # 実行
        result = lambda_function.lambda_handler(event, None)

        print ("================ result ================")
        print (result)

        resultJson = json.loads(result)
        for r in resultJson["records"]:
            if r["mailSendId"] == 1:
                self.assertEqual(r["version"], 1)
                self.assertEqual(r["mailSubject"], "埋め込みサンプル件名")
