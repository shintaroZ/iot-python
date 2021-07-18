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
# 起動パラメータチェック
# --------------------------------------------------
def isArgument(event):

    # 必須項目チェック
    noneErrArray = []
    noneErrArray.append(CLIENT_NAME) if (CLIENT_NAME not in event) else 0
    noneErrArray.append(DEVICE_ID)if (DEVICE_ID not in event) else 0
    noneErrArray.append(SENSOR_ID) if (SENSOR_ID not in event) else 0
    noneErrArray.append(SENSOR_NAME) if (SENSOR_NAME not in event) else 0
    noneErrArray.append(COLLECTION_VALUE_TYPE) if (COLLECTION_VALUE_TYPE not in event) else 0
    noneErrArray.append(COLLECTION_TYPE) if (COLLECTION_TYPE not in event) else 0
    noneErrArray.append(SAVING_FLG) if (SAVING_FLG not in event) else 0
    noneErrArray.append(LIMIT_CHECK_FLG) if (LIMIT_CHECK_FLG not in event) else 0
    
    # 必須項目がない場合は例外スロー
    if 0 < len(noneErrArray):
        raise Exception ("Missing required request parameters. [%s]" % ",".join(noneErrArray))
    
    # 型チェック
    typeErrArray = []
    typeErrArray.append(COLLECTION_VALUE_TYPE) if (initCommon.isValidateNumber(event[COLLECTION_VALUE_TYPE]) == False) else 0
    typeErrArray.append(COLLECTION_TYPE) if (initCommon.isValidateNumber(event[COLLECTION_TYPE]) == False) else 0
    typeErrArray.append(REVISION_MAGNIFICATION) if (REVISION_MAGNIFICATION in event and initCommon.isValidateFloat(event[REVISION_MAGNIFICATION]) == False) else 0
    typeErrArray.append(X_COORDINATE) if (X_COORDINATE in event and initCommon.isValidateFloat(event[X_COORDINATE]) == False) else 0
    typeErrArray.append(Y_COORDINATE) if (Y_COORDINATE in event and initCommon.isValidateFloat(event[Y_COORDINATE]) == False) else 0
    typeErrArray.append(SAVING_FLG) if (initCommon.isValidateNumber(event[SAVING_FLG]) == False) else 0
    typeErrArray.append(LIMIT_CHECK_FLG) if (initCommon.isValidateNumber(event[LIMIT_CHECK_FLG]) == False) else 0
    typeErrArray.append(LIMIT_COUNT_TYPE) if (LIMIT_COUNT_TYPE in event and initCommon.isValidateNumber(event[LIMIT_COUNT_TYPE]) == False) else 0
    typeErrArray.append(LIMIT_COUNT) if (LIMIT_COUNT in event and initCommon.isValidateNumber(event[LIMIT_COUNT]) == False) else 0
    typeErrArray.append(LIMIT_COUNT_RESET_RANGE) if(LIMIT_COUNT_RESET_RANGE in event and initCommon.isValidateNumber(event[LIMIT_COUNT_RESET_RANGE]) == False) else 0
    typeErrArray.append(ACTION_RANGE) if (ACTION_RANGE in event and initCommon.isValidateNumber(event[ACTION_RANGE]) == False) else 0
    typeErrArray.append(NEXT_ACTION) if (NEXT_ACTION in event and initCommon.isValidateNumber(event[NEXT_ACTION]) == False) else 0
       
    if LIMIT_RECORDS in event:
        for r in event[LIMIT_RECORDS]:
            typeErrArray.append(LIMIT_SUB_NO) if (LIMIT_SUB_NO in r and initCommon.isValidateNumber(r[LIMIT_SUB_NO]) == False) else 0
            typeErrArray.append(LIMIT_JUDGE_TYPE) if (LIMIT_JUDGE_TYPE in r and initCommon.isValidateNumber(r[LIMIT_JUDGE_TYPE]) == False) else 0
            typeErrArray.append(LIMIT_VALUE) if (LIMIT_VALUE in r and initCommon.isValidateNumber(r[LIMIT_VALUE]) == False) else 0
            
    # 重複削除
    set(typeErrArray)
    
    # 型異常の場合は例外スロー
    if 0 < len(typeErrArray):
        raise TypeError("The parameters is type invalid. [%s]" % ",".join(typeErrArray))
    
    # 閾値の必須チェック（閾値成立回数条件〜閾値の何かが含まれる場合は必須）
    limitArray = [LIMIT_COUNT_TYPE, LIMIT_COUNT, LIMIT_COUNT_RESET_RANGE, ACTION_RANGE, NEXT_ACTION]
    limitChildArray = []
    setIsLimit(False)
    limitArray.remove(LIMIT_COUNT_TYPE) if (LIMIT_COUNT_TYPE in event) else 0
    limitArray.remove(LIMIT_COUNT) if (LIMIT_COUNT in event) else 0
    limitArray.remove(LIMIT_COUNT_RESET_RANGE) if (LIMIT_COUNT_RESET_RANGE in event) else 0
    limitArray.remove(ACTION_RANGE) if (ACTION_RANGE in event) else 0
    limitArray.remove(NEXT_ACTION) if (NEXT_ACTION in event) else 0
    limitArray.clear() if len(limitArray) == 5 else setIsLimit(True)
    
    if LIMIT_RECORDS in event:
        for r in event[LIMIT_RECORDS]:
            limitChildArray = [LIMIT_SUB_NO, LIMIT_JUDGE_TYPE, LIMIT_VALUE]
            limitChildArray.remove(LIMIT_SUB_NO) if (LIMIT_SUB_NO in r) else 0
            limitChildArray.remove(LIMIT_JUDGE_TYPE) if (LIMIT_JUDGE_TYPE in r) else 0
            limitChildArray.remove(LIMIT_VALUE) if (LIMIT_VALUE in r) else 0
            if 0 < len(limitChildArray):
                setIsLimit(True)
                break

    # 配列結合
    limitArray.extend(limitChildArray)
    
    # 閾値項目が歯抜けの場合は例外スロー
    if 0 < len(limitArray):
        raise Exception("Missing required request parameters. [%s]" % ",".join(limitArray))
        
    # データ長チェック
    lengthArray = []
    lengthArray.append(DEVICE_ID) if (20 < len(event[DEVICE_ID])) else 0
    lengthArray.append(SENSOR_ID) if (10 < len(event[SENSOR_ID])) else 0
    lengthArray.append(SENSOR_NAME) if (30 < len(event[SENSOR_NAME])) else 0
    lengthArray.append(SENSOR_UNIT) if (SENSOR_UNIT in event and 10 < len(event[SENSOR_UNIT])) else 0
    lengthArray.append(STATUS_TRUE) if (STATUS_TRUE in event and 10 < len(event[STATUS_TRUE])) else 0
    lengthArray.append(STATUS_FALSE) if (STATUS_FALSE in event and 10 < len(event[STATUS_FALSE])) else 0
    lengthArray.append(COLLECTION_VALUE_TYPE) if (MAX_TYNYINT_UNSIGNED < event[COLLECTION_VALUE_TYPE]) else 0
    lengthArray.append(COLLECTION_TYPE) if (MAX_SMALLINT_UNSIGNED < event[COLLECTION_TYPE]) else 0
    lengthArray.append(SAVING_FLG) if (MAX_TYNYINT_UNSIGNED < event[SAVING_FLG]) else 0
    lengthArray.append(LIMIT_CHECK_FLG) if (MAX_TYNYINT_UNSIGNED < event[LIMIT_CHECK_FLG]) else 0

    lengthArray.append(LIMIT_COUNT_TYPE) if (LIMIT_COUNT_TYPE in event and MAX_TYNYINT_UNSIGNED < event[LIMIT_COUNT_TYPE]) else 0
    lengthArray.append(LIMIT_COUNT) if (LIMIT_COUNT in event and MAX_SMALLINT_UNSIGNED < event[LIMIT_COUNT]) else 0
    lengthArray.append(LIMIT_COUNT_RESET_RANGE) if (LIMIT_COUNT_RESET_RANGE in event and MAX_SMALLINT_UNSIGNED < event[LIMIT_COUNT_RESET_RANGE]) else 0
    lengthArray.append(ACTION_RANGE) if (ACTION_RANGE in event and MAX_SMALLINT_UNSIGNED < event[ACTION_RANGE]) else 0
    lengthArray.append(NEXT_ACTION) if (NEXT_ACTION in event and MAX_TYNYINT_UNSIGNED < event[NEXT_ACTION]) else 0

    lengthArray.append(LIMIT_COUNT_TYPE) if (LIMIT_COUNT_TYPE in event and MAX_SMALLINT_UNSIGNED < event[LIMIT_COUNT_TYPE]) else 0
    lengthArray.append(LIMIT_COUNT_TYPE) if (LIMIT_COUNT_TYPE in event and MAX_SMALLINT_UNSIGNED < event[LIMIT_COUNT_TYPE]) else 0
    
    if LIMIT_RECORDS in event:
        for r in event[LIMIT_RECORDS]:
            lengthArray.append(LIMIT_SUB_NO) if (MAX_TYNYINT_UNSIGNED < r[LIMIT_SUB_NO]) else 0
            lengthArray.append(LIMIT_JUDGE_TYPE) if (MAX_TYNYINT_UNSIGNED < r[LIMIT_JUDGE_TYPE]) else 0
             
    # 重複削除
    set(lengthArray)
    
    # データ長異常の場合は例外スロー
    if 0 < len(lengthArray):
        raise TypeError("The parameters is length invalid. [%s]" % ",".join(lengthArray))
    
    return

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

# --------------------------------------------------
# 起動パラメータにデータ定義マスタ用のパラメータを付与して返却する。
# --------------------------------------------------
def createDataCollectionParams(event, result, version):
    
    # 起動パラメータの必須判定
    # str項目
    if SENSOR_UNIT in event:
        event[SENSOR_UNIT] = "'" + event[SENSOR_UNIT] + "'"
    elif result is None:
        event[SENSOR_UNIT] = "NULL"
    elif result[SENSOR_UNIT] is None:
        event[SENSOR_UNIT] = "NULL"
    else:
        event[SENSOR_UNIT] = "'" + result[SENSOR_UNIT] + "'"
        
    if STATUS_TRUE in event:
        event[STATUS_TRUE] = "'" + event[STATUS_TRUE] + "'"
    elif result is None:
        event[STATUS_TRUE] = "NULL"
    elif result[STATUS_TRUE] is None:
        event[STATUS_TRUE] = "NULL"
    else:
        event[STATUS_TRUE] = "'" + result[STATUS_TRUE] + "'"
        
    if STATUS_FALSE in event:
        event[STATUS_FALSE] = "'" + event[STATUS_FALSE] + "'"
    elif result is None:
        event[STATUS_FALSE] = "NULL"
    elif result[STATUS_FALSE] is None:
        event[STATUS_FALSE] = "NULL"
    else:
        event[STATUS_FALSE] = "'" + result[STATUS_FALSE] + "'"
    
    # float項目
    if REVISION_MAGNIFICATION in event:
        event[REVISION_MAGNIFICATION] = event[REVISION_MAGNIFICATION]
    elif result is None:
        event[REVISION_MAGNIFICATION] = "NULL"
    elif result[REVISION_MAGNIFICATION] is None:
        event[REVISION_MAGNIFICATION] = "NULL"
    else:
        event[REVISION_MAGNIFICATION] = result[REVISION_MAGNIFICATION]
        
    if X_COORDINATE in event:
        event[X_COORDINATE] = event[X_COORDINATE]
    elif result is None:
        event[X_COORDINATE] = "NULL"
    elif result[X_COORDINATE] is None:
        event[X_COORDINATE] = "NULL"
    else:
        event[X_COORDINATE] = result[X_COORDINATE]
        
    if Y_COORDINATE in event:
        event[Y_COORDINATE] = event[Y_COORDINATE]
    elif result is None:
        event[Y_COORDINATE] = "NULL"
    elif result[Y_COORDINATE] is None:
        event[Y_COORDINATE] = "NULL"
    else:
        event[Y_COORDINATE] = result[Y_COORDINATE]
        
        
    return createCommonParams(event, version)

# --------------------------------------------------
# 起動パラメータにシーケンス情報を付与して返却する。
# --------------------------------------------------
def createLSeqParams(event, version, dataCollectionSeq, limitCheckSeq):
    event[DATA_COLLECTION_SEQ] = dataCollectionSeq
    event[LIMIT_CHECK_SEQ] = limitCheckSeq
    return createCommonParams(event, version)

# --------------------------------------------------
# 起動パラメータに共通情報を付与して返却する。
# --------------------------------------------------
def createCommonParams(event, version):

    event[CREATED_AT] = initCommon.getSysDateJst()
    event[UPDATED_AT] = initCommon.getSysDateJst()
    event[UPDATED_USER] = "devUser" # todo イテレーション3以降で動的化
    event[VERSION] = version
    return event

    
#####################
# main
#####################
def lambda_handler(event, context):

    # 初期処理
    initConfig(event["clientName"])
    setLogger(initCommon.getLogger(LOG_LEVEL))

    LOGGER.info('マスタメンテナンス機能_データ定義マスタ更新開始 : %s' % event)

    # 入力チェック
    isArgument(event)
    
    # RDSコネクション作成
    rds = rdsCommon.rdsCommon(LOGGER, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, DB_CONNECT_TIMEOUT)

    # マスタselect
    result = rds.fetchone(initCommon.getQuery("sql/m_data_collection/findbyId.sql")
                          , createWhereParam(event))
    
    # シーケンス判定
    dataCollectionSeq = 0
    limitCheckSeq = 0
    version = 0
    if result is not None and DATA_COLLECTION_SEQ in result:
        dataCollectionSeq = result[DATA_COLLECTION_SEQ]
    else:
        seqDcResult = rds.fetchone(initCommon.getQuery("sql/m_seq/nextval.sql"), {"p_seqType" : 0})
        LOGGER.info("データ定義マスタシーケンスの新規採番 [%d]" % seqDcResult["nextSeq"])
        dataCollectionSeq = seqDcResult["nextSeq"]
        
    if result is not None and LIMIT_CHECK_SEQ in result:
        limitCheckSeq = result[LIMIT_CHECK_SEQ]
    else:
        seqLcResult = rds.fetchone(initCommon.getQuery("sql/m_seq/nextval.sql"), {"p_seqType" : 1})
        LOGGER.info("閾値条件マスタシーケンスの新規採番 [%d]" % seqLcResult["nextSeq"])
        limitCheckSeq = seqLcResult["nextSeq"]
    # バージョンのインクリメント
    if result is not None and VERSION in result:
        version = result[VERSION] + 1
        LOGGER.info("登録対象バージョン [%d]" % version)
        
    try:
        # 閾値登録判定
        if IS_LIMIT:
            # 閾値マスタのDELETE/INSERT
            LOGGER.info("閾値マスタのDELETE [limitCheckSeq = %d]" % limitCheckSeq)
            rds.execute(initCommon.getQuery("sql/m_limit/delete.sql"), {"limitCheckSeq" : limitCheckSeq })
            for r in event[LIMIT_RECORDS]:
                LOGGER.info("閾値マスタのINSERT [%s]" % r)
                rds.execute(initCommon.getQuery("sql/m_limit/insert.sql"), createLSeqParams(r, version, dataCollectionSeq, limitCheckSeq))
            
            # 閾値条件マスタのUPSERT
            LOGGER.info("閾値条件マスタのUPSERT [dataCollectionSeq = %d]" % dataCollectionSeq)
            rds.execute(initCommon.getQuery("sql/m_limit_check/upsert.sql"), createLSeqParams(event, version, dataCollectionSeq, limitCheckSeq))
        
        # 連携フラグマスタのUPSERT
        LOGGER.info("連携フラグマスタのUPSERT [dataCollectionSeq = %d]" % dataCollectionSeq)
        rds.execute(initCommon.getQuery("sql/m_link_flg/upsert.sql"), createLSeqParams(event, version, dataCollectionSeq, limitCheckSeq))
        
        # データ定義マスタのUPSERT
        LOGGER.info("データ定義マスタのUPSERT [%s, %s]" % (event[DEVICE_ID], event[SENSOR_ID]) )
        rds.execute(initCommon.getQuery("sql/m_data_collection/upsert.sql"), createDataCollectionParams(event, result, version))
    except Exception as ex:
        LOGGER.error("登録に失敗しました。ロールバックします。")
        rds.rollBack()
        raise ex
    
    # commit
    rds.commit()
    
    # close
    del rds
    