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
import sys
import base64


# -------------------------------------
# MQ接続共通クラスのテストクラス
# -------------------------------------
class MqCommonTest(unittest.TestCase):

    MQ_CONNECT = None
    LOGGER = None
    MQ_NLB_HOST = "rabbitmq.eg-iot-develop.com"
    MQ_PORT = 5671
    MQ_USER = "monone_dev"
    MQ_PASSWORD = "VTwJMnmyqiE2"
    MQ_UT_EXCANGE = "ut.exchange"
    MQ_UT_ROUTINGKEY = "ut.queue"
    MQ_UT_QUEUE = "ut.queue"
    
    # テスト開始時に1回だけ呼び出される
    # クラスメソッドとして定義する
    @classmethod
    def setUpClass(self):
        self.LOGGER = initCommon.getLogger("INFO")
        # MQ接続（リトライ付き）
        for num in range(1, 10):
            try:
                self.MQ_CONNECT = mq.mqCommon(self.LOGGER, self.MQ_NLB_HOST, self.MQ_PORT, self.MQ_USER, self.MQ_PASSWORD)
                break
            except Exception as ex:
                print("***** mq connect retry [%d]" % num)
            
    @classmethod
    def tearDownClass(self):
        # global MQ_CONNECT
        # MQ切断
        print ("***** mq close")
        del self.MQ_CONNECT
        
    # テストメソッドを実行するたびに呼ばれる
    def setUp(self):
        pass
 
    # テストメソッドの実行が終わるたびに呼ばれる
    def tearDown(self):
        pass
       
    @unittest.skip("検証用")
    # ------------------------------------------------------
    # キューの中身を全て取得してキューが空になること。
    # ------------------------------------------------------
    def test_x_fff(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)
        
        resultArray = self.MQ_CONNECT.getQueueMessage(queueName="Queue_To_Tenant_101")
        for result in resultArray:
            print (result)
        

        # # byte文字列をbyteへ変換
        # errReqStr = initCommon.getQuery("ERR_GaussianMixture_ID01-body.txt")
        #
        # # endode
        # errReqByte = errReqStr.encode(encoding='utf-8')
        #
        # # b64でデコード     
        # decodeStr = base64.b64decode(errReqByte)
        #
        # with open("ERR_OUTPUT_DECODE_GaussianMixture_ID01", "wb") as f:
        #     f.write(decodeStr)
            
       
        # img = None
        # json_path = "test/threshold_ID01.json"
        # ai_path = "test/GaussianMixture_ID01.pkl"
        
        # with open(file_path, "r", encoding = "shift_jis") as f:
        
        # # バイナリモードで開く
        # with open(json_path, "rb") as f:
        #     jsonImg = f.read()
        #     jsonEnc = base64.b64encode(jsonImg)
        # with open(ai_path, "rb") as f:
        #     aiImg = f.read()
        #     aiEnc = base64.b64encode(aiImg)
        
        # # テキストモードで開く
        # with open(json_path, "r") as f:
        #     jsonStr = f.read()
        # # with open(ai_path, "r", encoding="cp037") as f:
        # # with open(ai_path, "r", encoding="cp437") as f:
        # with open(ai_path, "r", encoding="utf_16") as f:
        #     aiStr = f.read()
        
        # print("★json-body")
        # with open("threshold_ID01-body", 'w') as f4:
        #     f4.write(jsonEnc.decode())
        # print("★AI-body")
        # with open("GaussianMixture_ID01-body", 'w') as f4:
        #     f4.write(aiEnc.decode())
        #
        # print("★json file")
        # with open("NEW_threshold_ID01.json", 'wb') as f4:
        #     f4.write(base64.b64decode(jsonEnc))
        # print("★AI file")
        # with open("NEW_GaussianMixture_ID01.pkl", 'wb') as f4:
        #     f4.write(base64.b64decode(aiEnc))
        #

        # with open(json_path, "rb") as f:
        #     img = f.read()
        #
        # print(img)
        # # img = initCommon.getQuery("test/base64_AI").encode('utf-8')
        # img = initCommon.getQuery("test/base64_AI").encode('shift-jis')
        #
        # # with open(file_path, 'rb') as f:
        # #     img = base64.b64encode(f.read())
        # #
        # #
        # with open("New_GaussianMixture_ID01.pkl", 'bw') as f4:
        #     f4.write(base64.b64decode(aiEnc))
        
        # print("★start")
        # print(img)
        # print("★end")     
        
    # ------------------------------------------------------
    # キューの中身を全て取得してキューが空になること。
    # ------------------------------------------------------
    def test_getQueueMessage_001(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)

        # キューの中身をクリア
        self.MQ_CONNECT.getQueueMessage(queueName=self.MQ_UT_QUEUE, isErrDel=True)
       
        # テストメッセージ送信
        self.MQ_CONNECT.publishExchange(self.MQ_UT_EXCANGE, self.MQ_UT_ROUTINGKEY, initCommon.getQuery("test/mq/newError001.json"))
        self.MQ_CONNECT.publishExchange(self.MQ_UT_EXCANGE, self.MQ_UT_ROUTINGKEY, initCommon.getQuery("test/mq/newError002.json"))
        
        # 全メッセージ取得
        resultArray = self.MQ_CONNECT.getQueueMessage(queueName=self.MQ_UT_QUEUE)
        
        # 検証
        isCheck1 = False
        isCheck2 = False
        for result in resultArray:
            print(result)
            
            if result["tenantId"] in ["1"]:
                self.assertEqual(result["tenantId"], "1")
                isCheck1 = True
            
            if result["tenantId"] in ["2"]:
                self.assertEqual(result["tenantId"], "2")
                isCheck2 = True
        self.assertTrue(isCheck1 and isCheck2)
    
    # ------------------------------------------------------
    # TENANT_ID:2のみキューを取得すること。
    # ------------------------------------------------------
    def test_getQueueMessage_002(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)
    
        # キューの中身をクリア
        self.MQ_CONNECT.getQueueMessage(queueName=self.MQ_UT_QUEUE, isErrDel=True)
    
        # テストメッセージ送信
        self.MQ_CONNECT.publishExchange(self.MQ_UT_EXCANGE, self.MQ_UT_ROUTINGKEY, initCommon.getQuery("test/mq/newError001.json"))
        self.MQ_CONNECT.publishExchange(self.MQ_UT_EXCANGE, self.MQ_UT_ROUTINGKEY, initCommon.getQuery("test/mq/newError002.json"))
    
        # 一部メッセージだけ取得
        resultArray = self.MQ_CONNECT.getQueueMessage(queueName=self.MQ_UT_QUEUE, idArray=["2"])
        
        # 検証
        isCheck1 = False
        isCheck2 = False
        for result in resultArray:
            print(result)
            
            if result["tenantId"] in ["1"]:
                self.assertEqual(result["tenantId"], "1")
                isCheck1 = True
            if result["tenantId"] in ["2"]:
                isCheck2 = True
        self.assertTrue(isCheck1 == False and isCheck2)
    
    # ------------------------------------------------------
    # TENANT_IDが存在しない場合は無視されること。
    # ------------------------------------------------------
    def test_getQueueMessage_003(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)
    
        # キューの中身をクリア
        self.MQ_CONNECT.getQueueMessage(queueName=self.MQ_UT_QUEUE, isErrDel=True)
    
        # テストメッセージ送信
        self.MQ_CONNECT.publishExchange(self.MQ_UT_EXCANGE, self.MQ_UT_ROUTINGKEY, initCommon.getQuery("test/mq/newError001.json"))
        self.MQ_CONNECT.publishExchange(self.MQ_UT_EXCANGE, self.MQ_UT_ROUTINGKEY, initCommon.getQuery("test/mq/newError002.json"))
        self.MQ_CONNECT.publishExchange(self.MQ_UT_EXCANGE, self.MQ_UT_ROUTINGKEY, initCommon.getQuery("test/mq/newError003.json"))
    
        # 一部メッセージだけ取得
        resultArray = self.MQ_CONNECT.getQueueMessage(queueName=self.MQ_UT_QUEUE, idArray=["2"])
        for result in resultArray:
            print(result)
        self.assertEqual(len(resultArray), 1)
    
    # ------------------------------------------------------
    # Json形式でない場合は無視されること。
    # ------------------------------------------------------
    def test_getQueueMessage_004(self):
        print("------------ %s start------------" % sys._getframe().f_code.co_name)
    
        # キューの中身をクリア
        self.MQ_CONNECT.getQueueMessage(queueName=self.MQ_UT_QUEUE, isErrDel=True)
    
        # テストメッセージ送信
        self.MQ_CONNECT.publishExchange(self.MQ_UT_EXCANGE, self.MQ_UT_ROUTINGKEY, initCommon.getQuery("test/mq/newError001.json"))
        self.MQ_CONNECT.publishExchange(self.MQ_UT_EXCANGE, self.MQ_UT_ROUTINGKEY, initCommon.getQuery("test/mq/newError002.json"))
        self.MQ_CONNECT.publishExchange(self.MQ_UT_EXCANGE, self.MQ_UT_ROUTINGKEY, "平文テスト123")
    
        # 一部メッセージだけ取得
        resultArray = self.MQ_CONNECT.getQueueMessage(queueName=self.MQ_UT_QUEUE, idArray=["2"])
        for result in resultArray:
            print(result)
        self.assertEqual(len(resultArray), 1)

