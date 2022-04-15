import logging
import datetime
import sys
import pymysql
import time
import pika
import ssl
from enum import IntEnum
import json
from pickletools import UP_TO_NEWLINE


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
    EXCHANGES_DW_NAME = "fuuryokuhatsuden.to_tenant.%s"
    
    # 上り要求のキュー名
    UP_QUEUE_ARRAY = []
    UP_QUEUE_ARRAY.append("Last_Score")  # スコアデータ
    UP_QUEUE_ARRAY.append("New_Error")  # 異常音
    UP_QUEUE_ARRAY.append("Restart_AI")  # エッジ再起動
    UP_QUEUE_ARRAY.append("Restart_Edge_Failed")  # エッジ再起動失敗
    UP_QUEUE_ARRAY.append("Transfer_Current_Sound")  # 現在音

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

    def getUpQueueArray(self):
        return self.UP_QUEUE_ARRAY

    # --------------------------------------------------
    # コンストラクタ パラメータを元にMQ接続を行う
    # --------------------------------------------------
    # logger(logger)      : ロガー
    # host(str)           : 接続先エンドポイント
    # port(int)           : 接続先ポート番号
    # user(str)           : ユーザ
    # password(str)       : パスワード
    # --------------------------------------------------
    def __init__(self, logger=None, host="localhost", port=5671, user="hoge", password="hoge"):
        self.LOGGER = logger

        try:
            # SSL Context for TLS configuration of Amazon MQ for RabbitMQ
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
            ssl_context.set_ciphers('ECDHE+AESGCM:!ECDSA')
            
            # AmazonMQ接続先URL作成（amqps://{ユーザ}:{パスワード}@{接続先エンドポイント}:{ポート番号}）
            connectUrl = "amqps://%s:%s@%s:%d" % (user, password, host, port)
            self.LOGGER.info(connectUrl)
            # connect_param = pika.URLParameters(url=connectUrl)
            credentials = pika.PlainCredentials(user, password)
            connect_param = pika.ConnectionParameters(host=host
                                                      , port=port
                                                      , credentials=credentials
                                                      , frame_max=131072
                                                      , heartbeat=0)
            connect_param.ssl_options = pika.SSLOptions(context=ssl_context)
            
            # AmazonMQコネクション作成
            self.LOGGER.info("---- MQ Connection Start")
            self.MQ_CONNECT = pika.BlockingConnection(connect_param)
            self.LOGGER.info("---- MQ Connection Successfull")
            
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
    # キューのメッセージ数を取得
    # queueName(str)  : キュー名
    # --------------------------------------------------
    def getQueueCount(self, queueName):
        
        # チャンネル作成
        self.CHANNEL = self.MQ_CONNECT.channel()
        
        # キュー毎のメッセージの件数出力
        queueCount = self.CHANNEL.queue_declare(queue=queueName,
                                            durable=True,
                                            exclusive=False,
                                            auto_delete=False).method.message_count
        self.LOGGER.info("%s count : %d" % (queueName, queueCount))
        
        # クローズ
        self.CHANNEL.close()
        return queueCount

    # --------------------------------------------------
    # キューからメッセージを取得し、配列で返却
    # queueName(str)  : キュー名
    # idArray(array)  : 取得対象のTENANT_IDを配列で指定
    # isErrDel(bool)  : 不正メッセージの扱い(false:スキップ、true:削除)
    # --------------------------------------------------
    def getQueueMessage(self, queueName, isErrDel=False, idArray=None):
        records = []
        
        # チャンネル作成
        self.CHANNEL = self.MQ_CONNECT.channel()
        
        # キューからメッセージ取得
        for method_frame, properties, body in self.CHANNEL.consume(queue=queueName, inactivity_timeout=0.5):
    
            isAck = False
            isErr = False
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
                    # Json形式でない場合
                    isErr = True
                    self.LOGGER.warn("フォーマット不正 : %s" % body)
                    jsonDict = body
                except Exception as ex2:
                    isErr = True
                    self.LOGGER.warn(ex2)
                    
                if isAck:
                    records.append(jsonDict)
                    # ACKを返すことでキュー消化
                    self.CHANNEL.basic_ack(delivery_tag=method_frame.delivery_tag)
                
                # エラー時の扱い
                if isErr and isErrDel:
                    self.CHANNEL.basic_ack(delivery_tag=method_frame.delivery_tag)
        
        self.LOGGER.info("%s result : %s" % (queueName, records))
        
        # クローズ
        self.CHANNEL.close()
        return records
    
    # --------------------------------------------------
    # エクスチェンジへメッセージを送信
    # exchangeName(str)  : エクスチェンジ名
    # routingKey(str)    : ルーティングキー
    # bodyMessage(str)   : メッセージボディ部
    # exchangeType(str)  : エクスチェンジタイプ
    # --------------------------------------------------
    def publishExchange(self, exchangeName, routingKey, bodyMessage="{}", exchangeType="topic"):
        
        # チャンネル作成
        self.CHANNEL = self.MQ_CONNECT.channel()
        
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
        
        # クローズ
        self.CHANNEL.close()
    
    # キュー内のTENANT_IDに顧客対象のIDが含まれるかを判定
    def __isQueueContains(self, jsonDict, idArray):
        isResult = False
        try:
            if idArray is None:
                isResult = True
            elif(jsonDict[self.TENANT_ID] in idArray):
                isResult = True
        except KeyError  as e:
            # TENANT_IDが存在しない
            raise Exception("TENANT_IDが存在しません。:%s" % jsonDict)
        
        return isResult


0
