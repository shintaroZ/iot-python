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
import mqCommon  # カスタムレイヤー
import sys
from unittest import mock


class S3BackupTest(unittest.TestCase):

    UT_RDS = None
    UT_MQ_HOST = "rabbitmq.eg-iot-develop.com"

    # テスト開始時に1回だけ呼び出される
    # クラスメソッドとして定義する
    @classmethod
    def setUpClass(self):
        pass

    @classmethod
    def tearDownClass(self):
        pass
    
    # ----------------------------------------------------------------------
    # 全キューが空の場合、空配列のreceivedMessagesが返却されること。
    # ----------------------------------------------------------------------
    def test_lambda_handler_001(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)

        # 起動パラメータ
        event = initCommon.readFileToJson('test/function/input001.json')
        lambda_function.lambda_handler(event, None)
        
        
