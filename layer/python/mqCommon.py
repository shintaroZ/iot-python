import logging
import datetime
import sys
import pymysql
import time
import pika
import ssl
from enum import IntEnum
import json


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
    ROUTING_QUEUE.append({"Last_Score": "Last_Score"})
    ROUTING_QUEUE.append({"New_Error": "New_Error"})
    ROUTING_QUEUE.append({"Restart_Edge": "Restart_Edge"})
    ROUTING_QUEUE.append({"Restart_Edge_Failed": "Restart_Edge_Failed"})
    ROUTING_QUEUE.append({"Transfer_Current_Sound": "Transfer_Current_Sound"})

    # TENANT_ID
    TENANT_ID = "tenantId"

    # --------------------------------------------------
    # プロパティ
    # --------------------------------------------------
    def getConnect(self):
        return self.MQ_CONNECT

    def getLogger(self):
        return self.LOGGER

    def getExchangesUpName(self):
        return self.EXCHANGES_UP_NAME

    def getExchangesDwName(self):
        return self.EXCHANGES_DW_NAME

    # --------------------------------------------------
    # コンストラクタ パラメータを元にRDS接続を行う
    # --------------------------------------------------
    # logger(logger)      : ロガー
    # host(str)           : 接続先エンドポイント
    # port(int)           : 接続先ポート番号
    # user(str)           : ユーザ
    # password(str)       : パスワード
    # --------------------------------------------------
    def __init__(self, logger=None, host="localhost", port=5671, user="hoge", password="hoge" ):
        # global LOGGER, MQ_CONNECT, CHANNEL
        self.LOGGER = logger
        # self.MQ_CONNECT = None

        try:
            # SSL Context for TLS configuration of Amazon MQ for RabbitMQ
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
            ssl_context.set_ciphers('ECDHE+AESGCM:!ECDSA')
            
            # AmazonMQ接続先URL作成（amqps://{ユーザ}:{パスワード}@{接続先エンドポイント}:{ポート番号}）
            connectUrl = "amqps://%s:%s@%s:%d" % (user, password, host, port)
            self.LOGGER.debug(connectUrl)
            connect_param = pika.URLParameters(connectUrl)
            connect_param.ssl_options = pika.SSLOptions(context=ssl_context)
            # connect_param = pika.ConnectionParameters(connectUrl, connection_attempts=5)
            
            # AmazonMQコネクション作成
            self.LOGGER.info("---- MQ Connection Start")
            self.MQ_CONNECT = pika.BlockingConnection(connect_param)
            self.LOGGER.info("---- MQ Connection Successfull")
            
            # チャンネル作成
            self.CHANNEL = self.MQ_CONNECT.channel()
        except Exception  as e:
            self.LOGGER.error("---- MQ Connection Failed")
            raise(e)

    # --------------------------------------------------
    # デストラクタ コネクションをクローズ
    # --------------------------------------------------
    def __del__(self):
        if (self.MQ_CONNECT is not None) and (self.MQ_CONNECT.is_closed == False):
            self.MQ_CONNECT.close()

    # --------------------------------------------------
    # キューからメッセージを取得
    # queueName(str)  : キュー名
    # idArray(array)  : 取得対象のTENANT_IDを配列で指定
    # --------------------------------------------------
    def getQueueMessage(self, queueName, idArray = None):
        records = []
        
        # キューからメッセージ取得
        for method_frame, properties, body in self.CHANNEL.consume(queue=queueName, inactivity_timeout=0.5):
    
            isAck = False
            # inactivity_timeout(秒)を超えるとNone返却するので終了
            if method_frame is None:
                break
            else:
                try:
                    # 文字列データをJson形式にパース
                    jsonDict = json.loads(body)
                    
                    # 取得判定
                    isAck = self.__isQueueContains(jsonDict, idArray)

                except json.JSONDecodeError as ex:
                    # Json形式でない場合はスキップ
                    self.LOGGER.warn("フォーマット不正の為、スキップします。:%s" % body)
                    jsonDict = body
                
                if isAck:
                    records.append(jsonDict)
                    # ACKを返すことでキュー消化
                    self.CHANNEL.basic_ack(delivery_tag=method_frame.delivery_tag)
        
        self.LOGGER.info("%s result : %s" % (queueName, records))
        return records
    
    # --------------------------------------------------
    # エクスチェンジへメッセージを送信
    # exchangeName(str)  : エクスチェンジ名
    # routingKey(str)    : ルーティングキー
    # bodyMessage(str)   : メッセージボディ部
    # exchangeType(str)  : エクスチェンジタイプ
    # --------------------------------------------------
    def publishExchange(self, exchangeName, routingKey, bodyMessage="{}", exchangeType="topic"):
        
        # ExchangeTypeの生成
        self.CHANNEL.exchange_declare(exchange=exchangeName,
                                 exchange_type=exchangeType,
                                 durable=True)
                            
        # メッセージ送信
        self.CHANNEL.basic_publish(exchange=exchangeName,
                             routing_key=routingKey,
                             body=bodyMessage,
                             properties=pika.BasicProperties(
                                 delivery_mode=2,
                                 headers={"content_type": "application/json"}
                                ))
        
    
    # キュー内のTENANT_IDに顧客対象のIDが含まれるかを判定
    def __isQueueContains(self, jsonDict, idArray):
        isResult = False
        try:
            if idArray is None:
                isResult=  True
            elif(jsonDict[self.TENANT_ID] in idArray):
                isResult = True
        except KeyError  as e:
            # TENANT_IDが存在しない
            self.LOGGER.warn("TENANT_IDが存在しないため、スキップします。:%s" % jsonDict)
        
        return isResult
0