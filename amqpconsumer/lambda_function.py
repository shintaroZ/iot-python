import json
import configparser
import pika
import initCommon # カスタムレイヤー
import rdsCommon # カスタムレイヤー
import mqCommon # カスタムレイヤー

# global
LOG_LEVEL = "INFO"
DB_HOST = "localhost"
DB_PORT = 3306
DB_USER = "hoge"
DB_PASSWORD = "hoge"
DB_NAME = "hoge"
DB_CONNECT_TIMEOUT = 3
MQ_HOST = "localhost"
MQ_PORT = 3306
MQ_USER = "hoge"
MQ_PASSWORD = "hoge"

# setter
def setLogLevel(loglevel):
    global LOG_LEVEL
    LOG_LEVEL = loglevel
def setDbHost(dbHost):
    global DB_HOST
    DB_HOST = dbHost
def setDbPort(dbPort):
    global DB_PORT
    DB_PORT = int(dbPort)
def setDbUser(dbUser):
    global DB_USER
    DB_USER = dbUser
def setDbPassword(dbPassword):
    global DB_PASSWORD
    DB_PASSWORD = dbPassword
def setDbName(dbName):
    global DB_NAME
    DB_NAME = dbName
def setDbConnectTimeout(dbConnectTimeout):
    global DB_CONNECT_TIMEOUT
    DB_CONNECT_TIMEOUT = int(dbConnectTimeout)
def setMqHost(mqHost):
    global MQ_HOST
    MQ_HOST = mqHost
def setMqPort(mqPort):
    global MQ_PORT
    MQ_PORT = int(mqPort)
def setMqUser(mqUser):
    global MQ_USER
    MQ_USER = mqUser
def setMqPassword(mqPassword):
    global MQ_PASSWORD
    MQ_PASSWORD = mqPassword

# --------------------------------------------------
# 起動パラメータチェック
# --------------------------------------------------
def isArgument(event):

    # トークン取得
    token = event["idToken"]
    
    # グループ名
    try:
        groupList = initCommon.getPayLoadKey(token, "cognito:groups")
    
        # 顧客名がグループ名に含まれること
        if (event["clientName"] not in groupList):
            raise Exception("clientNameがグループに属していません。clientName:%s groupName:%s" % (event["clientName"], ",".join(groupList) ))
    except Exception as ex:
        raise Exception("Authentication Error. [%s]" %  ex)
        
        
# --------------------------------------------------
# 設定ファイル読み込み
# --------------------------------------------------
def initConfig(clientName):
    try:
        # 設定ファイル読み込み
        result = initCommon.getS3Object(clientName, "config.ini")

        # ConfigParserへパース
        config_ini = configparser.ConfigParser()
        config_ini.read_string(result)

        setLogLevel(config_ini['logger setting']['loglevel'])
        setDbHost(config_ini['rds setting']['host'])
        setDbPort(config_ini['rds setting']['port'])
        setDbUser(config_ini['rds setting']['user'])
        setDbPassword(config_ini['rds setting']['password'])
        setDbName(config_ini['rds setting']['db'])
        setDbConnectTimeout(config_ini['rds setting']['connect_timeout'])
        setMqHost(config_ini['mq setting']['host_NLB'])
        setMqPort(config_ini['mq setting']['port'])
        setMqUser(config_ini['mq setting']['user'])
        setMqPassword(config_ini['mq setting']['password'])
    except Exception as e:
        print ('設定ファイルの読み込みに失敗しました。')
        raise(e)
#####################
# main
#####################
def lambda_handler(event, context):

    # 初期処理
    initConfig(event["clientName"])
    logger = initCommon.getLogger(LOG_LEVEL)
    
    # MQコネクション生成
    mqConnection = mqCommon.mqCommon(logger, MQ_HOST, MQ_PORT, MQ_USER, MQ_PASSWORD)
    
    return mqConnection
    #
    # # AmazonMQ接続先URL作成（amqps://{ユーザ}:{パスワード}@{接続先エンドポイント}:{ポート番号}）
    # connectUrl = "amqps://%s:%s@%s:%d" % (MQ_USER, MQ_PASSWORD, MQ_HOST, MQ_PORT)
    # logger.debug(connectUrl)
    # connect_param = pika.URLParameters(connectUrl)
    #
    # # AmazonMQコネクション作成
    # try:
    #     logger.info("---- MQ connection start")
    #     mqConnection = pika.BlockingConnection(connect_param)
    #     logger.info("---- MQ connection successfull")
    # except Exception as e:
    #     logger.error("---- MQ connection failed")
    #     raise(e)
    #
    # # チャンネル作成
    # channel = mqConnection.channel()
    #
    # # RDSコネクション作成
    # rds = rdsCommon.rdsCommon(logger, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, DB_CONNECT_TIMEOUT)
    #
    # # MQマスタ検索（起動パラメータ.dataTypeに該当するキューからデータを取得）
    # param = {}
    # param["p_dataType"] = event["dataType"]
    # query = initCommon.getQuery("sql/m_mq/findAll.sql")
    # mqmRecords = rds.fetchall(query, param)
    #
    # records = []
    # ququeCountTotal = 0
    #
    # # MQマスタ分loop
    # for record in mqmRecords:
    #
    #     queueName = record["queueName"]
    #
    #     # キュー毎のメッセージの件数出力
    #     queueCount = channel.queue_declare(queue=queueName,
    #                                         durable=True,  
    #                                         exclusive=False,
    #                                         auto_delete=False).method.message_count
    #     logger.info("%s max count : %d" % (queueName, queueCount))
    #     ququeCountTotal = ququeCountTotal + queueCount
    #
    #     # キューからメッセージ取得
    #     for method_frame, properties, body in channel.consume(queue=queueName, inactivity_timeout=0.5):
    #
    #         # inactivity_timeout(秒)を超えるとNone返却するので終了
    #         if method_frame is None:
    #             break
    #         else:
    #             # print ("***** %s" % body.decode('utf-8'))
    #             # 文字列データをJson形式にパース
    #             jsonDict = json.loads(body)
    #             records.append(jsonDict)
    #             # ACKを返すことでキュー消化
    #             channel.basic_ack(delivery_tag=method_frame.delivery_tag)
    #
    # # 戻り値生成
    # resultDict = event.copy()
    # resultDict["recordsCount"] = ququeCountTotal
    # resultDict["records"] = records
    # logger.info(resultDict)
    #
    # logger.info("---- MQ connection close")
    # channel.close()
    # mqConnection.close()
    #
    # return resultDict