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
IS_LIMIT = False

# カラム名定数
CLIENT_NAME = "clientName"
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

LIMIT_CHECK_SEQ = "limitCheckSeq"
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

# データ型定数
MAX_TYNYINT_UNSIGNED = 255
MAX_SMALLINT_UNSIGNED = 65535

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


def setIsLimit(bl):
    global IS_LIMIT
    IS_LIMIT = bl


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
# データ定義マスタ参照の可変パラメータ作成
# --------------------------------------------------
def createWhereParam(event):

    whereStr = ""
    whereArray = []
    if "deviceId" in event:
        whereArray.append("mdc.DEVICE_ID = '%s'" % event["deviceId"])
    if "sensorId" in event:
        whereArray.append("mdc.SENSOR_ID = '%s'" % event["sensorId"])
    
    if 0 < len(whereArray):
        whereStr = "where " + " AND ".join(whereArray)
    
    return {"p_whereParams" : whereStr}

    
#####################
# main
#####################
def lambda_handler(event, context):

    # 初期処理
    initConfig(event["clientName"])
    setLogger(initCommon.getLogger(LOG_LEVEL))

    LOGGER.info('マスタメンテナンス機能_データ定義マスタ削除開始 : %s' % event)

    # RDSコネクション作成
    rds = rdsCommon.rdsCommon(LOGGER, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, DB_CONNECT_TIMEOUT)

    # マスタselect
    result = rds.fetchone(initCommon.getQuery("sql/m_data_collection/findbyId.sql")
                          , createWhereParam(event))
    
    try:
        # 閾値マスタのDELETE
        if result is not None and LIMIT_CHECK_SEQ in result:
            LOGGER.info("閾値マスタのDELETE [limitCheckSeq = %d]" % result[LIMIT_CHECK_SEQ])
            rds.execute(initCommon.getQuery("sql/m_limit/delete.sql"), {LIMIT_CHECK_SEQ : result[LIMIT_CHECK_SEQ] })
        
        # 閾値判定マスタ/連携フラグマスタのDELETE
        if result is not None and DATA_COLLECTION_SEQ in result:
            LOGGER.info("閾値条件マスタのDELETE [dataCollectionSeq = %d]" % result[DATA_COLLECTION_SEQ])
            rds.execute(initCommon.getQuery("sql/m_limit_check/delete.sql"), {DATA_COLLECTION_SEQ : result[DATA_COLLECTION_SEQ]})
            
            LOGGER.info("連携フラグマスタのDELETE [dataCollectionSeq = %d]" % result[DATA_COLLECTION_SEQ])
            rds.execute(initCommon.getQuery("sql/m_link_flg/delete.sql"), {DATA_COLLECTION_SEQ : result[DATA_COLLECTION_SEQ]})

        # データ定義マスタのDELETE
        LOGGER.info("データ定義マスタのDELETE [%s, %s]" % (event[DEVICE_ID], event[SENSOR_ID]) )
        rds.execute(initCommon.getQuery("sql/m_data_collection/delete.sql"), event)

        # 公開DB:時系列のDELETE
        LOGGER.info("時系列テーブルのDELETE [%s, %s]" % (event[DEVICE_ID], event[SENSOR_ID]) )        
        rds.execute(initCommon.getQuery("sql/t_public_timeseries/delete.sql"), event)
        
    except Exception as ex:
        LOGGER.error("削除に失敗しました。ロールバックします。")
        rds.rollBack()
        raise ex
    
    # commit
    rds.commit()
    
    # close
    del rds
    