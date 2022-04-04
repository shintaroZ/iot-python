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

class MqCommonTest(unittest.TestCase):

    def test_mqCommon_001(self):
        print("------------ %s start------------" % "test_mqCommon_001" )
        logger = initCommon.getLogger("DEBUG")
        mqIns = mq.mqCommon(logger,MQ_HOST,MQ_PORT,MQ_USER,MQ_PASSWORD)
        
        resultCount = mqIns.getQueueCount("New_Error")
        resultMessage = mqIns.getQueueMessage("New_Error")
        
        print("------------ %s end------------" % "test_mqCommon_001x" )
        del mqIns
        
