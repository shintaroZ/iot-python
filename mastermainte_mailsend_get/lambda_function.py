import json
import sys
import datetime
import configparser
import initCommon  # カスタムレイヤー
import rdsCommon  # カスタムレイヤー


# global
LOGGER = None
CONNECT = None
LOG_LEVEL = "INFO"
DB_HOST = "localhost"
DB_PORT = 3306
DB_USER = "hoge"
DB_PASSWORD = "hoge"
DB_NAME = "hoge"
DB_CONNECT_TIMEOUT = 3
RETRY_MAX_COUNT = 3
RETRY_INTERVAL = 500

# カラム名定数
MAIL_SEND_SEQ = "mailSendSeq"
MAIL_SEND_ID = "mailSendId"
DELETE_FLG = "deleteFlg"
EMAIL_ADDRESS = "emailAddress"
SEND_WEEK_TYPE = "sendWeekType"
SEND_FREQUANCY = "sendFrequancy"
SEND_TIME_FROM = "sendTimeFrom"
SEND_TIME_TO = "sendTimeTo"
MAIL_SUBJECT = "mailSubject"
MAIL_TEXT_HEADER = "mailTextHeader"
MAIL_TEXT_BODY = "mailTextBody"
MAIL_TEXT_FOOTER = "mailTextFooter"
EQUIPMENT_RECORDS = "equipmentRecords"
EQUIPMENT_ID = "equipmentId"

CREATED_AT = "createdAt"
UPDATED_AT = "updatedAt"
UPDATED_USER = "updatedUser"
VERSION = "version"
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


def setRetryMaxCount(retryMaxCount):
    global RETRY_MAX_COUNT
    RETRY_MAX_COUNT = int(retryMaxCount)


def setRetryInterval(retryInterval):
    global RETRY_INTERVAL
    RETRY_INTERVAL = int(retryInterval)


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
        setRetryMaxCount(config_ini['rds setting']['retryMaxcount'])
        setRetryInterval(config_ini['rds setting']['retryinterval'])
    except Exception as e:
        print ('設定ファイルの読み込みに失敗しました。')
        raise(e)

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
# 戻り値整形
# --------------------------------------------------
def convertResult(result):
    
     # 戻り値整形
    reList = []
    beforeKeyTable = {}
    childList = []
    parentTable = {}
    parentList =[]

    dataCollectionTable = {}
    dataCollectionList = []
    limitCheckTable = {}
    limitCheckList = []
    limitTable = {}
    limitList = []
    beforeKeyTable = {}

    for i in range(len(result)):

        # キーが異なる場合にセット
        if (MAIL_SEND_ID in beforeKeyTable and beforeKeyTable[MAIL_SEND_ID] != result[i][MAIL_SEND_ID]):

            if 0 < len(childList):
                parentTable[EQUIPMENT_RECORDS] = childList
            reList.append(parentTable)
            # 一時領域クリア
            childList = []
            parentTable = {}

        if len(parentTable) == 0:
            parentTable[MAIL_SEND_ID] = result[i][MAIL_SEND_ID]
            parentTable[EMAIL_ADDRESS] = result[i][EMAIL_ADDRESS]
            parentTable[SEND_WEEK_TYPE] = result[i][SEND_WEEK_TYPE]
            parentTable[SEND_FREQUANCY] = result[i][SEND_FREQUANCY]
            parentTable[SEND_TIME_FROM] = result[i][SEND_TIME_FROM]
            parentTable[SEND_TIME_TO] = result[i][SEND_TIME_TO]
            parentTable[MAIL_SUBJECT] = result[i][MAIL_SUBJECT]
            parentTable[MAIL_TEXT_HEADER] = result[i][MAIL_TEXT_HEADER]
            parentTable[MAIL_TEXT_BODY] = result[i][MAIL_TEXT_BODY]
            parentTable[MAIL_TEXT_FOOTER] = result[i][MAIL_TEXT_FOOTER]
            parentTable[EQUIPMENT_RECORDS] = [] # 予め空配列を設定

            parentTable[CREATED_AT] = result[i][CREATED_AT]
            parentTable[UPDATED_AT] = result[i][UPDATED_AT]
            parentTable[UPDATED_USER] = result[i][UPDATED_USER]
            parentTable[VERSION] = result[i][VERSION]

            # キー項目を退避
            beforeKeyTable[MAIL_SEND_ID] = result[i][MAIL_SEND_ID]

        # 可変部
        if result[i][EQUIPMENT_ID] is not None:
            childTable = {}
            childTable[EQUIPMENT_ID] = result[i][EQUIPMENT_ID]
            childList.append(childTable)

    # 最終ループ用
    if 0 < len(childList):
        parentTable[EQUIPMENT_RECORDS] = childList
    reList.append(parentTable)

    reResult = {"records" : reList}
    
    # Dict→str形式に変換して返却
    return json.dumps(reResult, ensure_ascii=False, default=initCommon.json_serial)
        
#####################
# main
#####################
def lambda_handler(event, context):

    # 初期処理
    initConfig(event["clientName"])
    setLogger(initCommon.getLogger(LOG_LEVEL))

    LOGGER.info('マスタメンテナンス機能_メール通知マスタ参照開始 : %s' % event)

    # 入力チェック
    isArgument(event)

    # RDSコネクション作成
    rds = rdsCommon.rdsCommon(LOGGER, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, DB_CONNECT_TIMEOUT)

    # マスタselect
    result = rds.fetchall(initCommon.getQuery("sql/m_mail_send/findAll.sql"))
    
    # 戻り値整形
    reReult = convertResult(result)
    
    # close
    del rds
    
    return reReult
