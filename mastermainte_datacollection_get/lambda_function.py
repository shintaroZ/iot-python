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
DEVICE_ID = "deviceId"
SENSOR_ID = "sensorId"
DATA_COLLECTION_SEQ = "dataCollectionSeq"
SENSOR_NAME = "sensorName"
SENSOR_UNIT = "sensorUnit"
STATUS_TRUE = "statusTrue"
STATUS_FALSE = "statusFalse"
COLLECTION_VALUE_TYPE = "collectionValueType"
COLLECTION_TYPE = "collectionType"
REVISION_MAGNIFICATION = "revisionMagnification"
X_COORDINATE = "xCoordinate"
Y_COORDINATE = "yCoordinate"
SAVING_FLG = "savingFlg"
LIMIT_CHECK_FLG = "limitCheckFlg"

LIMIT_COUNT_TYPE = "limitCountType"
LIMIT_COUNT = "limitCount"
LIMIT_COUNT_RESET_RANGE = "limitCountResetRange"
ACTION_RANGE = "actionRange"
NEXT_ACTION = "nextAction"

LIMIT_RECORDS = "limitRecords"
LIMIT_SUB_NO = "limitSubNo"
LIMIT_JUDGE_TYPE = "limitJudgeType"
LIMIT_VALUE = "limitValue"

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
        if ((DEVICE_ID in beforeKeyTable and beforeKeyTable[DEVICE_ID] != result[i][DEVICE_ID]) or
            (SENSOR_ID in beforeKeyTable and beforeKeyTable[SENSOR_ID] != result[i][SENSOR_ID]) ):

            if 0 < len(childList):
                parentTable[LIMIT_RECORDS] = childList
            reList.append(parentTable)
            # 一時領域クリア
            childList = []
            parentTable = {}

        if len(parentTable) == 0:
            parentTable[DEVICE_ID] = result[i][DEVICE_ID]
            parentTable[SENSOR_ID] = result[i][SENSOR_ID]
            parentTable[DATA_COLLECTION_SEQ] = result[i][DATA_COLLECTION_SEQ]
            parentTable[SENSOR_NAME] = result[i][SENSOR_NAME]
            parentTable[SENSOR_UNIT] = result[i][SENSOR_UNIT]
            parentTable[STATUS_TRUE] = result[i][STATUS_TRUE]
            parentTable[STATUS_FALSE] = result[i][STATUS_FALSE]
            parentTable[COLLECTION_VALUE_TYPE] = result[i][COLLECTION_VALUE_TYPE]
            parentTable[COLLECTION_TYPE] = result[i][COLLECTION_TYPE]
            parentTable[REVISION_MAGNIFICATION] = result[i][REVISION_MAGNIFICATION]
            parentTable[X_COORDINATE] = result[i][X_COORDINATE]
            parentTable[Y_COORDINATE] = result[i][Y_COORDINATE]
            parentTable[SAVING_FLG] = result[i][SAVING_FLG]
            parentTable[LIMIT_CHECK_FLG] = result[i][LIMIT_CHECK_FLG]

            # 可変部
            if result[i][LIMIT_COUNT_TYPE] is not None:
                parentTable[LIMIT_COUNT_TYPE] = result[i][LIMIT_COUNT_TYPE]
                parentTable[LIMIT_COUNT] = result[i][LIMIT_COUNT]
                parentTable[LIMIT_COUNT_RESET_RANGE] = result[i][LIMIT_COUNT_RESET_RANGE]
                parentTable[ACTION_RANGE] = result[i][ACTION_RANGE]
                parentTable[NEXT_ACTION] = result[i][NEXT_ACTION]

            parentTable[CREATED_AT] = result[i][CREATED_AT]
            parentTable[UPDATED_AT] = result[i][UPDATED_AT]
            parentTable[UPDATED_USER] = result[i][UPDATED_USER]
            parentTable[VERSION] = result[i][VERSION]

            # キー項目を退避
            beforeKeyTable[DEVICE_ID] = result[i][DEVICE_ID]
            beforeKeyTable[SENSOR_ID] = result[i][SENSOR_ID]

        # 可変部
        if result[i][LIMIT_SUB_NO] is not None:
            childTable = {}
            childTable[LIMIT_SUB_NO] = result[i][LIMIT_SUB_NO]
            childTable[LIMIT_JUDGE_TYPE] = result[i][LIMIT_JUDGE_TYPE]
            childTable[LIMIT_VALUE] = result[i][LIMIT_VALUE]
            childList.append(childTable)

    # 最終ループ用
    if 0 < len(childList):
        parentTable[LIMIT_RECORDS] = childList
    reList.append(parentTable)

    reResult = {"records" : reList}
    LOGGER.info(reResult)

    # Dict→str形式に変換して返却
    return json.dumps(reResult, ensure_ascii=False, default=initCommon.json_serial)

# --------------------------------------------------
# データ定義マスタ参照の可変パラメータ作成
# --------------------------------------------------
def createWhereParam(event):

    whereStr = ""
    whereArray = []
    if "deviceId" in event:
        whereArray.append("AND mdc.DEVICE_ID = '%s'" % event["deviceId"])
    if "sensorId" in event:
        whereArray.append("AND mdc.SENSOR_ID = '%s'" % event["sensorId"])

    if 0 < len(whereArray):
        whereStr = " ".join(whereArray)

    return {"p_whereParams" : whereStr}

#####################
# main
#####################
def lambda_handler(event, context):

    # 初期処理
    initConfig(event["clientName"])
    setLogger(initCommon.getLogger(LOG_LEVEL))

    LOGGER.info('マスタメンテナンス機能_データ定義マスタ参照開始 : %s' % event)

    # 入力チェック
    isArgument(event)

    # RDSコネクション作成
    rds = rdsCommon.rdsCommon(LOGGER, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, DB_CONNECT_TIMEOUT)

    # マスタselect
    result = rds.fetchall(initCommon.getQuery("sql/m_data_collection/findbyId.sql")
                          , createWhereParam(event))

    # 戻り値整形
    reReult = convertResult(result)

    # close
    del rds

    return reReult
