import unittest
from unittest import mock
import datetime
from datetime import datetime as dt
import json
import pymysql
import rdsCommon as rds
import initCommon
import mqCommon as mq
import configparser


DB_HOST = "mysql.cpp9recuwclr.ap-northeast-1.rds.amazonaws.com"
DB_PORT = 3306
DB_USER = "admin"
DB_PASSWORD = "Yz8aBhFr"
DB_NAME = "ins001"

MQ_HOST = "eg-iot-nlb-a11512e8be0beed3.elb.ap-northeast-1.amazonaws.com"
MQ_PORT = 5671
MQ_USER = "monone_dev"
MQ_PASSWORD = "VTwJMnmyqiE2"

def get_query(query_file_path):
    with open(query_file_path, 'r', encoding='utf-8') as f:
        query = f.read()
    return query

def createEvent(query_file_path):
    f = open(query_file_path, 'r')
    return json.load(f)

class InitCommonTest(unittest.TestCase):

    def test_getS3_001(self):
        print("------------ %s start------------" % "test_getS3_001" )
        # 設定ファイル読み込み
        result = initCommon.getS3Object("eg-iot-develop", "config.ini")

        # ConfigParserへパース
        config = configparser.ConfigParser(allow_no_value=True)
        config.read_string(result)
        

        self.assertEqual(config["rds setting"]["host"], "mysql.cpp9recuwclr.ap-northeast-1.rds.amazonaws.com")
        
    def test_getS3_002(self):
        print("------------ %s start------------" % "test_getS3_002" )
        # バケット名が存在しない場合は例外
        with self.assertRaises(Exception):
            result = initCommon.getS3Object("eg-iot-developXXX", "config.ini")
 
    def test_getS3_003(self):
        print("------------ %s start------------" % "test_getS3_003" )
        # オブジェクト名が存在しない場合は例外
        with self.assertRaises(Exception):
            result = initCommon.getS3Object("eg-iot-develop", "config.iniXXX")

    def test_getPayLoad_001(self):
        print("------------ %s start------------" % "test_getPayLoad_001" )
        logger = initCommon.getLogger("INFO")
        token = "eyJraWQiOiJ1UXBkSHRxR240aXhNNmVnT1FZVGN5TXA1UXRwbHRENVdOaEtkU2JWbmNjPSIsImFsZyI6IlJTMjU2In0.eyJhdF9oYXNoIjoieVlSN3pTT3lrTnBrcktCQ05fSnJvdyIsInN1YiI6Ijc2NjA5OTM4LTRlOGMtNDc4ZS1iMDk2LTdmMTIxNzE0Y2EwNCIsImNvZ25pdG86Z3JvdXBzIjpbImRldkdyb3VwIiwiYWRtaW4iXSwiZW1haWxfdmVyaWZpZWQiOnRydWUsImlzcyI6Imh0dHBzOlwvXC9jb2duaXRvLWlkcC5hcC1ub3J0aGVhc3QtMS5hbWF6b25hd3MuY29tXC9hcC1ub3J0aGVhc3QtMV96dFI3RlppYTkiLCJjb2duaXRvOnVzZXJuYW1lIjoiZGV2dXNlciIsImNvZ25pdG86cm9sZXMiOlsiYXJuOmF3czppYW06Ojk2ODUwNjcyMTE0NDpyb2xlXC9zaW1wbGVJb3QiXSwiYXVkIjoiMTRxaXZsbjFsODU1NDJ0MWY5cWtmb283bWEiLCJldmVudF9pZCI6ImEzODA3ZWZlLWZhMmYtNDlhMS1iNjM4LTJiNmQzYTQ5ZDkyMyIsInRva2VuX3VzZSI6ImlkIiwiYXV0aF90aW1lIjoxNjI1MTE5NzI4LCJleHAiOjE2MjUxMjMzMjgsImlhdCI6MTYyNTExOTcyOCwiZW1haWwiOiJzX290b2lAbWF0aXNzZS5jby5qcCJ9.vWv__SjtsLl5f4IpUOgG1zL31t4cxe95URqrQ4WKVWcabxiO26mqFEFkDYg64SH-W4RyNdKpYZY7EywRLFLckFOqMozK-I4j6G4FnW4JbxKDPu-mFmphjd7j9qXTNWD_kaIR38ME2AFAxPaoCwLqoeDaUZGT5VL6kGgzRRZvbC0TCtKzlg7gLq1R0p5RqUy_900vL85Mxia3FYO6TpJLvUcSVLYzkRT2MC3mHIkwX5S8kYJVEBNki1AvgayzH3VIry-tf8kJ0c5oFLp2Osx6EGiwn10zsFQuDjdXscUBvRWiIz4vCYia8Z_HpEk5C3Oia34K1B6FxAswFABm_nzSfA"

        # ユーザが所属するグループ名の取得
        result = initCommon.getPayLoadKey(token, "cognito:groups")

        # グループ名出力
        for record in result:
            print(record)

        # 取得出来ない時は例外
        with self.assertRaises(Exception):
            initCommon.getPayLoadKey(token, "cognito:groups2")

