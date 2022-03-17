import json
import configparser
import pika
import initCommon # カスタムレイヤー
import rdsCommon # カスタムレイヤー
import ssl

# global
LOGGER = None
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
def setLogger(logger):
    global LOGGER
    LOGGER = logger
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
# MQマスタ取得用のパラメータ生成
# --------------------------------------------------
def createMasterMqParam(dataType, tenantId):
    param = {}
    param["p_dataType"] = dataType
    param["p_tenantId"] = tenantId
    return param
# --------------------------------------------------
# 設定ファイル読み込み
# --------------------------------------------------
def initConfig(filePath):
    try:
        config_ini = configparser.ConfigParser()
        config_ini.read(filePath, encoding='utf-8')

        setLogLevel(config_ini['logger setting']['loglevel'])
        setDbHost(config_ini['rds setting']['host'])
        setDbPort(config_ini['rds setting']['port'])
        setDbUser(config_ini['rds setting']['user'])
        setDbPassword(config_ini['rds setting']['password'])
        setDbName(config_ini['rds setting']['db'])
        setDbConnectTimeout(config_ini['rds setting']['connect_timeout'])
        
        setMqHost(config_ini['mq setting']['host'])
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
    initConfig(event["setting"])
    setLogger(initCommon.getLogger(LOG_LEVEL))
    LOGGER.info("MQ送信機能開始")
    
    # パラメータから設備IDのリスト取得
    tenantList = set([record.get('tenantId') for record in event["records"]])
    
    # パラメータからRoutingKey取得
    routingKey = event["routingKey"] if ("routingKey" in event) else ""
    LOGGER.info("param(tenantList : %s, routingKey : %s)" % (tenantList, routingKey))
        
    # MQ接続先URL作成
    connectUrl = "amqps://%s:%s@%s:%d" % (MQ_USER, MQ_PASSWORD, MQ_HOST, MQ_PORT)
    LOGGER.debug(connectUrl)
    connect_param = pika.URLParameters(connectUrl)
    
    # SSL Context for TLS configuration of Amazon MQ for RabbitMQ
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
    ssl_context.set_ciphers('ECDHE+AESGCM:!ECDSA')
    connect_param.ssl_options = pika.SSLOptions(context=ssl_context)


    # MQコネクション作成
    try:
        LOGGER.info("---- MQ connection start")
        mqConnection = pika.BlockingConnection(connect_param)
        LOGGER.info("---- MQ connection successfull")
    except Exception as e:
        LOGGER.error("---- MQ connection failed")
        raise(e)
    
    # チャンネル生成
    channel = mqConnection.channel()
    LOGGER.info("---- channel create")
    
    # RDSコネクション作成
    rds = rdsCommon.rdsCommon(LOGGER, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, DB_CONNECT_TIMEOUT)
    LOGGER.info("---- rds connect")
    
    # 設備分ループ
    for t in tenantList:
        
        # MQマスタからExchangeNameとExchangeTypeを取得
        param = {}
        param["p_dataType"] = 2
        param["p_regexpTenantId"] ="_" + t + "$"
        query = initCommon.getQuery("sql/m_mq/findAll.sql")
        mqmRecords = rds.fetchone(query, param)
        if mqmRecords is None:
            LOGGER.error("MQマスタに登録されていません。設備ID : %s" % t)
            continue
            
        # Exchange名は設備単位
        exchangeName = mqmRecords["exchangeName"]
        LOGGER.info("---- exchangeName : %s" % exchangeName)
        
        
        # ExchangeTypeの生成
        channel.exchange_declare(exchange=exchangeName,
                                 exchange_type=mqmRecords["exchangeType"],
                                 durable=True)
                            
        # メッセージ送信
        LOGGER.info("publish start : %s : %s" %(exchangeName, routingKey))
        channel.basic_publish(exchange=exchangeName,
                             routing_key=routingKey,
                             body="{}",
                             properties=pika.BasicProperties(
                                 delivery_mode = 2, 
                                 headers={"content_type": "application/json"}
                                ))
        LOGGER.info("publish end")

    # クローズ      
    channel.close()
    mqConnection.close()
    del rds