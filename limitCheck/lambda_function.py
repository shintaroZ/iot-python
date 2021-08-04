import json
import sys
import datetime
import configparser
import initCommon  # カスタムレイヤー
import rdsCommon  # カスタムレイヤー
import boto3
from botocore.exceptions import ClientError
import re # 正規表現
from enum import IntEnum
from pymysql import TIME

# 送信ステータス
class SendStatusEnum(IntEnum):
    Before = 0              # 送信前
    MailConvFailured = 1    # メール整形失敗
    SendSuccess = 2         # メール送信成功
    SendFailured = 3        # メール送信失敗

# 閾値成立回数条件
class LimitCountTypeEnum(IntEnum):
    Continue = 0    # 継続
    Save = 1        # 累積
    
# 後続アクション
class NextActionEnum(IntEnum):
    No = 0          # なし
    MailSend = 1    # メール通知

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

# 起動パラメータ定数
CLIENT_NAME = "clientName"
TIME_STAMP = "timeStamp"

# カラム名定数
LIMIT_HIT_MANAGED_SEQ = "limitHitManagedSeq"
LIMIT_HIT_STATUS = "limitHitStatus"
BEFORE_DETECTION_DATETIME = "beforeDetectionDateTime"
BEFORE_MAIL_SEND_DATETIME = "beforeMailSendDateTime"

DATA_COLLECTION_SEQ = "dataCollectionSeq"
DETECTION_DATETIME = "detectionDateTime"
LIMIT_SUB_NO = "limitSubNo"
MAIL_SEND_SEQ = "mailSendSeq"
SEND_STATUS = "sendStatus"

DEVICE_ID = "deviceId"
SENSOR_ID = "sensorId"
SENSOR_NAME = "sensorName"
SENSOR_UNIT = "sensorUnit"
STATUS_TRUE = "statusTrue"
STATUS_FALSE = "statusFalse"
UNIT = "unit"
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
LIMIT_JUDGE_TYPE = "limitJudgeType"
LIMIT_VALUE = "limitValue"

MAIL_SEND_ID = "mailSendId"
EMAIL_ADDRESS = "emailAddress"
SEND_WEEK_TYPE = "sendWeekType"
SEND_FREQUANCY = "sendFrequancy"
SEND_TIME_FROM = "sendTimeFrom"
SEND_TIME_TO = "sendTimeTo"
MAIL_SUBJECT = "mailSubject"
MAIL_TEXT_HEADER = "mailTextHeader"
MAIL_TEXT_BODY = "mailTextBody"
MAIL_TEXT_FOOTER = "mailTextFooter"
MAIL_TEXT = "mailText"

CREATED_AT = "createdAt"
UPDATED_AT = "updatedAt"
UPDATED_USER = "updatedUser"
VERSION = "version"

SENSOR_VALUE = "sensorValue"

LIMIT_CHECL_START_DATETIME = "limitCheckStartDateTime"

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
# メール整形
# メール本文_ボディを置き換え文字に置換して返却
# --------------------------------------------------
def convertMeilText(mailTextBody, record):
    resultMap = {}

    # メール本文を置き換え文字で置換
    for key, val in REPLACE_STR_MAP.items():

        # Null判定
        valStr = str(record[val]) if (val in record and record[val] is not None) else ""

        mailTextBody = mailTextBody.replace(key, valStr)

    return mailTextBody

# --------------------------------------------------
# デバイスIDとセンサIDのパラメータを作成して返却する。
# --------------------------------------------------
def createMasterMainteParams(deviceId, sensorId):
    
    params = {}
    params[DEVICE_ID] = deviceId
    params[SENSOR_ID] = sensorId
    
    return createCommonParams(params)

# --------------------------------------------------
# デバイスIDとセンサIDのパラメータを作成して返却する。
# --------------------------------------------------
def createPublicTimeSeriesParams(timeStamp):
    
    params = {}
    params[TIME_STAMP] = timeStamp
    
    return createCommonParams(params)

# --------------------------------------------------
# 閾値管理登録用のパラメータを作成して返却する。
# --------------------------------------------------
def createLimitHitManagedParams(limitHitManagedSeq, dataCollectionSeq, detectionDateTime, limitSubNo, limitHitStatus = 0):
    
    params = {}
    params[LIMIT_HIT_MANAGED_SEQ] = limitHitManagedSeq
    params[DATA_COLLECTION_SEQ] = dataCollectionSeq
    params[DETECTION_DATETIME] = detectionDateTime
    params[LIMIT_SUB_NO] = limitSubNo
    params[LIMIT_HIT_STATUS] = limitHitStatus
    
    return createCommonParams(params)

# --------------------------------------------------
# データ定義マスタシーケンスと閾値通番のパラメータを作成して返却する。
# --------------------------------------------------
def createSeqLimitSubParams(dataCollectionSeq, limitSubNo):
    
    params = {}
    params[DATA_COLLECTION_SEQ] = dataCollectionSeq
    params[LIMIT_SUB_NO] = limitSubNo
    
    return createCommonParams(params)

# --------------------------------------------------
# 起動パラメータに共通情報を付与して返却する。
# --------------------------------------------------
def createCommonParams(params):

    params[CREATED_AT] = initCommon.getSysDateJst()
    params[UPDATED_AT] = initCommon.getSysDateJst()
    return params

#####################
# main
#####################
def lambda_handler(event, context):

    # 初期処理
    initConfig(event["clientName"])
    setLogger(initCommon.getLogger(LOG_LEVEL))

    LOGGER.info('閾値判定機能開始 : %s' % event)

    # RDSコネクション作成
    rds = rdsCommon.rdsCommon(LOGGER, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, DB_CONNECT_TIMEOUT)

    # # 閾値判定対象データ取得
    # mRecords = rds.fetchall(initCommon.getQuery("sql/m_data_collection/findbyId.sql"))
    


    receivedMessages = event["receivedMessages"][0]
    sortedRecords = sorted(receivedMessages['records'], key=lambda x:x['sensorId'])
    beforeSensorId = ""
    limitInfoRecord = {}
    # 起動パラメータ分loop
    for i in range(len(sortedRecords)):
        print(sortedRecords[i])
        
        sensorId = sortedRecords[i][SENSOR_ID]
        # レコード一覧.タイムスタンプはUTCなのでJSTへ変換
        cnvTimeStamp = datetime.datetime.strptime(sortedRecords[i][TIME_STAMP], '%Y-%m-%d %H:%M:%S.%f')
        cnvTimeStamp = cnvTimeStamp + datetime.timedelta(seconds=32400)
        # yyyy/MM/dd HH:mm:ss.f形式の文字列に変更
        strTimeStamp = cnvTimeStamp.strftime('%Y/%m/%d %H:%M:%S.%f')
        
        # 初回 or センサが切り替わったタイミング
        if i == 0 or (beforeSensorId != sensorId):
            
            # 閾値情報取得
            limitInfoRecord = rds.fetchone(initCommon.getQuery("sql/m_data_collection/findbyId.sql")
                                           , createMasterMainteParams(receivedMessages[DEVICE_ID], sortedRecords[i][SENSOR_ID]))
            print(limitInfoRecord)
            
        if limitInfoRecord is None:
            LOGGER.warn("閾値情報の取得に失敗しました。[%s, %s]" % (receivedMessages[DEVICE_ID], sortedRecords[i][SENSOR_ID]))
            continue
        
        # 過去データ取得
        publicRecords = rds.fetchall(initCommon.getQuery("sql/t_public_timeseries/findbyId.sql")
                                     , createPublicTimeSeriesParams(strTimeStamp))
        print(publicRecords)
        
        # 閾値判定 todo
        isLimmit = False
        
        # 閾値成立判定したか
        if (False):
            LOGGER.debug("閾値成立なし [%s, %s]" % (limitInfoRecord[DATA_COLLECTION_SEQ], sortedRecords[i][TIME_STAMP]))
            continue
        
        # シーケンス取得
        limitHitManagedSeq = 0
        seqResult = rds.fetchone(initCommon.getQuery("sql/m_seq/nextval.sql"), {"p_seqType" : 3})
        limitHitManagedSeq = seqResult["nextSeq"]
        LOGGER.info("閾値成立管理シーケンスの新規採番 [%d]" % seqResult["nextSeq"])

        # 閾値成立管理へ登録
        rds.execute(initCommon.getQuery("sql/t_limit_hit_managed/upsert.sql")
                    , createLimitHitManagedParams(limitHitManagedSeq
                                                  , limitInfoRecord[DATA_COLLECTION_SEQ]
                                                  , strTimeStamp
                                                  , limitInfoRecord[LIMIT_SUB_NO])
                    , RETRY_MAX_COUNT, RETRY_INTERVAL)
        
        mailSendArray = []
        # 後続アクション判定
        if limitInfoRecord[NEXT_ACTION] == NextActionEnum.No:
            LOGGER.debug("後続アクションなし [%s, %s]" % (limitInfoRecord[DATA_COLLECTION_SEQ], sortedRecords[i][TIME_STAMP]))
            continue
        
        elif limitInfoRecord[NEXT_ACTION] == NextActionEnum.MailSend:

            # メール通知管理よりデータ定義マスタシーケンスと閾値通番が一致する最新の
            mailSendArray = rds.fetchall(initCommon.getQuery("sql/m_mail_send/findbyId.sql")
                                         ,createSeqLimitSubParams(limitInfoRecord[DATA_COLLECTION_SEQ]
                                                                  , limitInfoRecord[LIMIT_SUB_NO]))
            # # 通知間隔判定
            # currentDateTime = initCommon.getSysDateJst() 
            # currentDateTimeAppend = currentDateTime + datetime.timedelta(seconds=limitInfoRecord[ACTION_RANGE] * 60)

            # 通知先分loop
            for msRecord in mailSendArray:
                print(msRecord)
                
                # 閾値成立管理から
                LOGGER.debug("")
                msRecord[BEFORE_DETECTION_DATETIME]
                limitInfoRecord[ACTION_RANGE]
            # print("現在時刻 : %s  30s加算 : %s" % (currentDateTime, currentDateTimeAppend) )
            
            # if limitInfoRecord[ACTION_RANGE] ==
        
    # commit
    rds.commit()
    
    # close
    del rds
