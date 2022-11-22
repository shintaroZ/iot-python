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

class RdsCommonTest(unittest.TestCase):

    def test_rdsCommon_001(self):
        print("------------ %s start------------" % "test_rdsCommon_001" )
        logger = initCommon.getLogger("DEBUG")
        with self.assertRaises(Exception):
            ins = rds.rdsCommon(logger,DB_HOST,99999,DB_USER,DB_PASSWORD,DB_NAME,3,True)


    def test_rdsCommon_002(self):
        print("------------ %s start------------" % "test_rdsCommon_002" )
        logger = initCommon.getLogger("DEBUG")
        ins = rds.rdsCommon(logger,DB_HOST,DB_PORT,DB_USER,DB_PASSWORD,DB_NAME,3,True)

        print("------------ delete ------------")
        delQuery = initCommon.getQuery("test/sql/t_tmp/delete.sql")
        ins.execute(delQuery)

        print("------------ insert ------------")
        insQuery = initCommon.getQuery("test/sql/t_tmp/insert001.sql")
        ins.execute(insQuery, {"receivedDatetime" : "2020/12/31 00:00:00"})
        ins.execute(insQuery, {"receivedDatetime" : "2020/12/31 00:00:01"})
        ins.execute(insQuery, {"receivedDatetime" : "2020/12/31 00:00:02"})
        
        print("------------ select ------------")
        sQuery = initCommon.getQuery("test/sql/t_tmp/select.sql")
        resultAll = ins.fetchall(sQuery)
        resultOne = ins.fetchone(sQuery)

        print(resultAll)
        self.assertEqual(resultAll[0]["RECEIVED_DATETIME"], dt.strptime("2020/12/31 00:00:00", "%Y/%m/%d %H:%M:%S") )
        self.assertEqual(resultAll[1]["RECEIVED_DATETIME"], dt.strptime("2020/12/31 00:00:01", "%Y/%m/%d %H:%M:%S") )
        self.assertEqual(resultAll[2]["RECEIVED_DATETIME"], dt.strptime("2020/12/31 00:00:02", "%Y/%m/%d %H:%M:%S") )
        print(resultOne)
        self.assertEqual(resultOne["RECEIVED_DATETIME"], dt.strptime("2020/12/31 00:00:00", "%Y/%m/%d %H:%M:%S") )

        del ins

        
