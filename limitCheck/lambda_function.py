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

# 閾値判定区分
class LimitJudgeTypeEnum(IntEnum):
    Exceed = 0      # 超過
    Match = 1       # 一致
    Less = 2        # 不足
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
RECEIVED_DATETIME = "receivedDatetime"

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
# 閾値判定を行いMap形式で返却する。
# 閾値成立の場合　：データ定義シーケンス、閾値通番
# 閾値未成立の場合：None
# --------------------------------------------------
def limitJudge(publicRecords, limitInfoArray):
    resultMap = {}

    dataCollectionSeq = limitInfoArray[0][DATA_COLLECTION_SEQ]  # データ定義マスタシーケンス
    limitCountType = limitInfoArray[0][LIMIT_COUNT_TYPE]        # 閾値成立回数条件(0:継続,1:累積)
    limitCount = limitInfoArray[0][LIMIT_COUNT]                 # 閾値成立回数

    LOGGER.debug("データ定義マスタシーケンス:[%d], 閾値成立回数条件(0:継続,1:累積):[%d], 閾値成立回数:[%d]" %
                 (dataCollectionSeq, limitCountType, limitCount))
    limitCountSummary = 0
    limitResult = {}
    reResult = {}

    for pRecord in publicRecords:

        limitResult = {}
        for lRecord in limitInfoArray:
            # 閾値判定
            if isLimit(lRecord[LIMIT_JUDGE_TYPE], lRecord[LIMIT_VALUE], pRecord[SENSOR_VALUE]):
                limitResult[LIMIT_SUB_NO] = lRecord[LIMIT_SUB_NO]
                break

        # 閾値成立回数条件(0:継続,1:累積)が継続の場合は連続しないとクリア
        if limitCountType == LimitCountTypeEnum.Continue:
            limitCountSummary = 0 if 0 == len(limitResult) else limitCountSummary

        # 閾値成立回数をカウント
        limitCountSummary += 1 if 0 < len(limitResult) else 0

        # 閾値成立回数を判定
        if limitCount == limitCountSummary:
            reResult[DATA_COLLECTION_SEQ] = dataCollectionSeq
            reResult[LIMIT_SUB_NO] = limitResult[LIMIT_SUB_NO]
            break

    return reResult

# --------------------------------------------------
# 閾値判定区分に応じて閾値と値を比較してbool返却
# --------------------------------------------------
def isLimit(limitJudgeType, limitValue, value):
    isLimit = False

    if limitJudgeType == LimitJudgeTypeEnum.Exceed:
        isLimit = value > limitValue
    elif limitJudgeType == LimitJudgeTypeEnum.Match:
        isLimit = value == limitValue
    elif limitJudgeType == LimitJudgeTypeEnum.Less:
        isLimit = value < limitValue

    return isLimit


# --------------------------------------------------
# 閾値通番の抽出条件のパラメータを作成
# --------------------------------------------------
def createLimitSubNoWhereParam(limitCheckResult, name):

    param = ""
    if 0 < len(limitCheckResult):
        param = "and %s.LIMIT_SUB_NO = %d" % (name, limitCheckResult[LIMIT_SUB_NO])


    return param

# --------------------------------------------------
# 閾値成立回数リセットの抽出条件のパラメータを作成
# --------------------------------------------------
def createLimitCountResetRangeWhereParam(limitCountResetRange, cnvTimeStamp):

    param = ""
    # 閾値成立回数リセット
    if limitCountResetRange != 0:
        # センサの受信タイムスタンプから閾値成立回数リセット分の範囲条件を追加
        cnvTimeStamp = cnvTimeStamp - datetime.timedelta(seconds=limitCountResetRange * 60)
        strTimeStamp = cnvTimeStamp.strftime('%Y/%m/%d %H:%M:%S.%f')
        param = "and '%s' <= tpt.RECEIVED_DATETIME" % strTimeStamp


    return param


# --------------------------------------------------
# 可変型パラメータを作成して返却する。
# --------------------------------------------------
def createDefaultCommonParams(deviceId=None, sensorId=None, dataCollectionSeq=None
                              , limitSubNo=None, timeStamp=None, mailSendSeq=None
                              , sendStatus=None, whereParam=""):

    params = {}
    params[DEVICE_ID] = deviceId
    params[SENSOR_ID] = sensorId
    params[DATA_COLLECTION_SEQ] = dataCollectionSeq
    params[LIMIT_SUB_NO] = limitSubNo
    params[TIME_STAMP] = timeStamp
    params[MAIL_SEND_SEQ] = mailSendSeq
    params[SEND_STATUS] = sendStatus
    params["whereParam"] = whereParam

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
#     setLogger(initCommon.getLogger("DEBUG"))

    LOGGER.info('閾値判定機能開始 : %s' % event)

    # RDSコネクション作成
    rds = rdsCommon.rdsCommon(LOGGER, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, DB_CONNECT_TIMEOUT)

    # 起動パラメータソート
    sortedRecords = sorted(event['records'], key=lambda x:(x['dataCollectionSeq'], x["receivedDatetime"]))
    limitInfoArray = {}

    # 起動パラメータ分loop
    for i in range(len(sortedRecords)):
        LOGGER.debug("起動パラメータ.レコード一覧:%s" % sortedRecords[i])

        argsDataCollectionSeq = int(sortedRecords[i][DATA_COLLECTION_SEQ])
        argsReceivedDatetime = sortedRecords[i][RECEIVED_DATETIME]
        # 受信日時をstr→datetime変換
        cnvTimeStamp = datetime.datetime.strptime(argsReceivedDatetime, '%Y/%m/%d %H:%M:%S.%f')

        # 初回 or データ定義マスタシーケンスが切り替わったタイミング
        if i == 0 or (0 < i and argsDataCollectionSeq != int(sortedRecords[i -1][DATA_COLLECTION_SEQ])):

            # マスタメンテナンスの閾値情報取得
            limitInfoArray = rds.fetchall(initCommon.getQuery("sql/mastermainte/findbyId.sql")
                                           , createDefaultCommonParams(dataCollectionSeq = argsDataCollectionSeq))

            LOGGER.info("マスタメンテナンスの閾値情報:%s" % limitInfoArray)
        if len(limitInfoArray) == 0:
            LOGGER.warn("マスタメンテナンスの閾値情報の取得に失敗しました。[%d, %s]" % (argsDataCollectionSeq, argsReceivedDatetime))
            continue

        # 時系列より閾値判定対象の過去データ取得
        publicRecords = rds.fetchall(initCommon.getQuery("sql/t_public_timeseries/findbyId.sql")
                                     , createDefaultCommonParams(dataCollectionSeq = limitInfoArray[0][DATA_COLLECTION_SEQ]
                                                                 , timeStamp = argsReceivedDatetime
                                                                 , whereParam = createLimitCountResetRangeWhereParam(limitInfoArray[0][LIMIT_COUNT_RESET_RANGE], cnvTimeStamp)))
        if 0 < len(publicRecords):
            LOGGER.debug("閾値判定対象の過去データ:%s" % publicRecords)


        # 閾値判定
        limitCheckResult = limitJudge(publicRecords, limitInfoArray)

        # 閾値成立したか
        if 0 < len(limitCheckResult):
            LOGGER.info("閾値成立しました。閾値成立管理へ登録します。 [%s, %s, %s]" % (limitInfoArray[0][DATA_COLLECTION_SEQ]
                                                             , argsReceivedDatetime
                                                             , limitCheckResult[LIMIT_SUB_NO]))


            # 閾値成立管理へ登録
            rds.execute(initCommon.getQuery("sql/t_limit_hit_managed/insertIgnore.sql")
                        , createDefaultCommonParams(dataCollectionSeq = limitCheckResult[DATA_COLLECTION_SEQ]
                                                    , timeStamp = argsReceivedDatetime
                                                    , limitSubNo = limitCheckResult[LIMIT_SUB_NO])
                        , RETRY_MAX_COUNT, RETRY_INTERVAL)

        mailSendArray = []
        # 後続アクション判定
        if limitInfoArray[0][NEXT_ACTION] == NextActionEnum.No:
            LOGGER.debug("後続アクションなし [%s, %s]" % (limitInfoArray[0][DATA_COLLECTION_SEQ], sortedRecords[i][TIME_STAMP]))
            continue

        elif limitInfoArray[0][NEXT_ACTION] == NextActionEnum.MailSend:

            # メール通知管理より前回通知時刻取得
            mailSendArray = rds.fetchall(initCommon.getQuery("sql/m_mail_send/findbyId.sql")
                                         ,createDefaultCommonParams(dataCollectionSeq = limitInfoArray[0][DATA_COLLECTION_SEQ]
                                                                    , whereParam = createLimitSubNoWhereParam(limitCheckResult, "tmsm")))
            # 現在時刻取得（タイムゾーン解除の為、再変換）
            strDateTime = initCommon.getSysDateJst().strftime('%Y/%m/%d %H:%M:%S.%f')
            currentDateTime = datetime.datetime.strptime(strDateTime, '%Y/%m/%d %H:%M:%S.%f')

            # 通知先分loop
            for msRecord in mailSendArray:

                appendDateTime = msRecord[BEFORE_MAIL_SEND_DATETIME] + datetime.timedelta(seconds=limitInfoArray[0][ACTION_RANGE] * 60)
                LOGGER.debug("現在時刻:[%s], 前回通知:[%s], 通知間隔(分):[%s], 前回通知+通知間隔:[%s]" % (currentDateTime.strftime("%Y/%m/%d %H:%M:%S.%f")
                                                                                                         , msRecord[BEFORE_MAIL_SEND_DATETIME].strftime("%Y/%m/%d %H:%M:%S.%f")
                                                                                                         , limitInfoArray[0][ACTION_RANGE]
                                                                                                         , appendDateTime.strftime("%Y/%m/%d %H:%M:%S.%f")) )

                # 通知間隔判定
                if appendDateTime <= currentDateTime:
                    # selectInsert
                    limitHitArray = rds.execute(initCommon.getQuery("sql/m_mail_send/selectInsert.sql")
                                                , createDefaultCommonParams(dataCollectionSeq = limitInfoArray[0][DATA_COLLECTION_SEQ]
                                                                          , whereParam = createLimitSubNoWhereParam(limitCheckResult, "tlhm")
                                                                          , timeStamp = argsReceivedDatetime
                                                                          , mailSendSeq = msRecord[MAIL_SEND_SEQ]
                                                                          , sendStatus = int(SendStatusEnum.Before))
                                                , RETRY_MAX_COUNT
                                                , RETRY_INTERVAL)

    # commit
    rds.commit()

    # close
    del rds
