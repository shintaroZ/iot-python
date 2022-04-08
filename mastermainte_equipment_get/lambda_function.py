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
EQUIPMENT_ID = "equipmentId"
EQUIPMENT_NAME = "equipmentName"
X_COORDINATE = "xCoordinate"
Y_COORDINATE = "yCoordinate"

DATA_COLLECTION_RECORDS = "datacollectionRecords"

EDGE_TYPE = "edgeType"
DEVICE_ID = "deviceId"
SENSOR_ID = "sensorId"
DATA_COLLECTION_SEQ = "dataCollectionSeq"
SENSOR_NAME = "sensorName"
SENSOR_UNIT = "sensorUnit"
STATUS_TRUE = "statusTrue"
STATUS_FALSE = "statusFalse"
COLLECTION_VALUE_TYPE = "collectionValueType"
COLLECTION_TYPE = "collectionType"
COLLECTION_TYPE_NAME = "collectionTypeName"
REVISION_MAGNIFICATION = "revisionMagnification"
SAVING_FLG = "savingFlg"
LIMIT_CHECK_FLG = "limitCheckFlg"

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
        if (EQUIPMENT_ID in beforeKeyTable and beforeKeyTable[EQUIPMENT_ID] != result[i][EQUIPMENT_ID]):

            if 0 < len(childList):
                parentTable[DATA_COLLECTION_RECORDS] = childList
            reList.append(parentTable)
            # 一時領域クリア
            childList = []
            parentTable = {}

        if len(parentTable) == 0:
            parentTable[EQUIPMENT_ID] = result[i][EQUIPMENT_ID]
            parentTable[EQUIPMENT_NAME] = result[i][EQUIPMENT_NAME]
            parentTable[X_COORDINATE] = result[i][X_COORDINATE]
            parentTable[Y_COORDINATE] = result[i][Y_COORDINATE]


            parentTable[CREATED_AT] = result[i][CREATED_AT]
            parentTable[UPDATED_AT] = result[i][UPDATED_AT]
            parentTable[UPDATED_USER] = result[i][UPDATED_USER]
            parentTable[VERSION] = result[i][VERSION]

            # キー項目を退避
            beforeKeyTable[EQUIPMENT_ID] = result[i][EQUIPMENT_ID]

        # 可変部
        if result[i][DEVICE_ID] is not None:
            childTable = {}
            childTable[EDGE_TYPE] = result[i][EDGE_TYPE]
            childTable[DEVICE_ID] = result[i][DEVICE_ID]
            childTable[SENSOR_ID] = result[i][SENSOR_ID]
            childTable[DATA_COLLECTION_SEQ] = result[i][DATA_COLLECTION_SEQ]
            childTable[SENSOR_NAME] = result[i][SENSOR_NAME]
            childTable[SENSOR_UNIT] = result[i][SENSOR_UNIT]
            childTable[STATUS_TRUE] = result[i][STATUS_TRUE]
            childTable[STATUS_FALSE] = result[i][STATUS_FALSE]
            childTable[COLLECTION_VALUE_TYPE] = result[i][COLLECTION_VALUE_TYPE]
            childTable[COLLECTION_TYPE] = result[i][COLLECTION_TYPE]
            childTable[COLLECTION_TYPE_NAME] = result[i][COLLECTION_TYPE_NAME]
            childTable[REVISION_MAGNIFICATION] = result[i][REVISION_MAGNIFICATION]
            childTable[SAVING_FLG] = result[i][SAVING_FLG]
            childTable[LIMIT_CHECK_FLG] = result[i][LIMIT_CHECK_FLG]
            childList.append(childTable)

    # 最終ループ用
    if 0 < len(childList):
        parentTable[DATA_COLLECTION_RECORDS] = childList
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
    if "equipmentId" in event:
        whereArray.append("AND me.EQUIPMENT_ID = '%s'" % event["equipmentId"])

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

    LOGGER.info('マスタメンテナンス機能_設備マスタ参照開始 : %s' % event)

    # 入力チェック
    isArgument(event)

    # RDSコネクション作成
    rds = rdsCommon.rdsCommon(LOGGER, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, DB_CONNECT_TIMEOUT)

    # マスタselect
    result = rds.fetchall(initCommon.getQuery("sql/m_equipment/findbyId.sql")
                          , createWhereParam(event))

    # 戻り値整形
    reReult = convertResult(result)

    # close
    del rds

    return reReult
