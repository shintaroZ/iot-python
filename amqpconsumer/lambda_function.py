import json
import configparser
import pika
import initCommon  # カスタムレイヤー
import rdsCommon  # カスタムレイヤー
import mqCommon  # カスタムレイヤー

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
            raise Exception("clientNameがグループに属していません。clientName:%s groupName:%s" % (event["clientName"], ",".join(groupList)))
    except Exception as ex:
        raise Exception("Authentication Error. [%s]" % ex)
        
        
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
    initConfig(event["clientName"])
    setLogger(initCommon.getLogger(LOG_LEVEL))
    
    # MQコネクション生成
    mqConnection = mqCommon.mqCommon(LOGGER, MQ_HOST, MQ_PORT, MQ_USER, MQ_PASSWORD)
    
    # RDSコネクション作成
    rds = rdsCommon.rdsCommon(LOGGER, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, DB_CONNECT_TIMEOUT)
    
    # Mononeの設備IDの取得
    tenantIdArray = rds.fetchall(initCommon.getQuery("sql/m_data_collection/getTenantId.sql"))
    
    receivedMessagesArray = []
    # 上りキュー分繰り返し：キュー名はレイヤー内に保持
    for queueName in mqConnection.getUpQueueArray():
        
        
        # キューの数をカウント
        queueCount = mqConnection.getQueueCount(queueName)
        
        # キューがあるものだけ取得しにいく
        if 0 < queueCount:
            for tenantIdDict in tenantIdArray:
                receivedMessagesDict = {}
            
                # キューからtenantIdに該当するデータ取得
                messageArray = mqConnection.getQueueMessage(queueName=queueName
                                                            , isErrDel = True
                                                            , idArray = [tenantIdDict["tenantId"]])
            
                # 取得出来た場合のみ戻り値を整形
                if 0 < len(messageArray):
                    receivedMessagesDict["queue"] = queueName
                    receivedMessagesDict["deviceId"] = tenantIdDict["tenantId"]
                    receivedMessagesDict["requestTimeStamp"] = initCommon.getSysDateJst().strftime('%Y/%m/%d %H:%M:%S')
                    receivedMessagesDict["recordsCount"] = len(messageArray)
                    receivedMessagesDict["records"] = messageArray
                    
                    receivedMessagesArray.append(receivedMessagesDict)

    
    # 戻り値の親要素を整形して返却
    resultDict = {}
    resultDict["clientName"] = event["clientName"]
    resultDict["receivedMessages"] = receivedMessagesArray
    
    # 各種クローズ
    del rds
    del mqConnection
    
    return resultDict
