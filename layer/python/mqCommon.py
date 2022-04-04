import logging
import datetime
import sys
import pymysql
import time
import pika
import ssl
from enum import IntEnum
import json
from _ast import Try

# ----------------- 上り要求 -----------------
# 0 : スコアデータ
# 1 : 異常音
# 2 : エッジ再起動
# 3 : 再起動失敗
# 4 : 現在音
# --------------------------------------------
class MqUpEnum(IntEnum):
    Last_Score = 0
    New_Error = 1
    Restart_Edge = 2
    Restart_Edge_Failed = 3
    Transfer_Current_Sound = 4
    
# -------------------------
# MQ接続共通クラス
# -------------------------
class mqCommon:

    # クラス変数
    LOGGER = None
    MQ_CONNECT = None
    CHANNEL = None
    
    # エクスチェンジ名
    EXCHANGES_UP_NAME = "fuuryokuhatsuden.to_web"
    EXCHANGES_DW_NAME = "fuuryokuhatsuden.to_tenant.%d"
    
    # ルーティングキー / キュー名
    ROUTING_QUEUE = []
    ROUTING_QUEUE.append( {"Last_Score" : "Last_Score"})
    ROUTING_QUEUE.append( {"New_Error" : "New_Error"})
    ROUTING_QUEUE.append( {"Restart_Edge" : "Restart_Edge"})
    ROUTING_QUEUE.append( {"Restart_Edge_Failed" : "Restart_Edge_Failed"})
    ROUTING_QUEUE.append( {"Transfer_Current_Sound" : "Transfer_Current_Sound"})

    # TENANT_ID
    TENANT_ID = "tenantId"
    # --------------------------------------------------
    # プロパティ
    # --------------------------------------------------
    def getConnect(self):
        global MQ_CONNECT
        return MQ_CONNECT
    def getLogger(self):
        global LOGGER
        return LOGGER

    # --------------------------------------------------
    # コンストラクタ パラメータを元にRDS接続を行う
    # --------------------------------------------------
    # logger(logger)      : ロガー
    # host(str)           : 接続先エンドポイント
    # port(int)           : 接続先ポート番号
    # user(str)           : ユーザ
    # password(str)       : パスワード
    # dbName(str)         : 接続先インスタンス
    # connectTimeout(int) : 接続タイムアウト(s)
    # autoCommit(bool)    : 自動コミット
    # --------------------------------------------------
    def __init__(self, logger=None, host="localhost", port=5671, user="hoge", password="hoge" ,dbName="hoge"
                 ,connectTimeout=3, autoCommit=False):
        global LOGGER, MQ_CONNECT, CHANNEL
        LOGGER = logger
        MQ_CONNECT = None

        try:
            # AmazonMQ接続先URL作成（amqps://{ユーザ}:{パスワード}@{接続先エンドポイント}:{ポート番号}）
            connectUrl = "amqps://%s:%s@%s:%d" % (user, password, host, port)
            LOGGER.debug(connectUrl)
        
            # SSL Context for TLS configuration of Amazon MQ for RabbitMQ
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
            ssl_context.set_ciphers('ECDHE+AESGCM:!ECDSA')
            
            connect_param = pika.URLParameters(connectUrl)
            # connect_param = pika.ConnectionParameters(connectUrl, heartbeat=10)
            # connect_param = pika.ConnectionParameters(connectUrl, connection_attempts=3)
            connect_param.ssl_options = pika.SSLOptions(context=ssl_context)
            
            # AmazonMQコネクション作成
            LOGGER.info("---- MQ Connection Start")
            MQ_CONNECT = pika.BlockingConnection(connect_param)
            LOGGER.info("---- MQ Connection Successfull")
            
            # チャンネル作成
            CHANNEL = MQ_CONNECT.channel()
        except Exception  as e:
            LOGGER.error("---- MQ Connection Failed")
            raise(e)

    # --------------------------------------------------
    # デストラクタ コネsクションをクローズ
    # --------------------------------------------------
    def __del__(self):
        global MQ_CONNECT
        if MQ_CONNECT is not None:
            MQ_CONNECT.close()


    # --------------------------------------------------
    # キューのメッセージ数を取得
    # queueName(str)  : キュー名
    # --------------------------------------------------
    def getQueueCount(self, queueName):
        
        # キュー毎のメッセージの件数出力
        queueCount = CHANNEL.queue_declare(queue=queueName,
                                            durable=True,  
                                            exclusive=False,
                                            auto_delete=False).method.message_count
        LOGGER.info("%s max count : %d" % (queueName, queueCount))
        
        return queueCount
    
    # --------------------------------------------------
    # キューのメッセージ数を取得
    # queueName(str)  : キュー名
    # --------------------------------------------------
    def getQueueMessage(self, queueName):
        records = []
        
        # キューからメッセージ取得
        for method_frame, properties, body in CHANNEL.consume(queue=queueName, inactivity_timeout=0.5):
    
            # inactivity_timeout(秒)を超えるとNone返却するので終了
            if method_frame is None:
                break
            else:
                print ("***** %s" % body.decode('utf-8'))
                # 文字列データをJson形式にパース
                jsonDict = json.loads(body)
                records.append(jsonDict)
                # ACKを返すことでキュー消化
                # channel.basic_ack(delivery_tag=method_frame.delivery_tag)
        
        LOGGER.info("%s result : %s" % (queueName, records))
        return records
    
    def isQueue(self, jsonDict, tenantId):
        try:
            return (tenantId == jsonDict[TENANT_ID])
            
        except Exception  as e:
            LOGGER.error("---- MQ Connection Failed")
            raise(e)
        
