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

# 送信ステータス
class SendStatusEnum(IntEnum):
    Before = 0              # 送信前
    MailConvFailured = 1    # メール整形失敗
    SendSuccess = 2         # メール送信成功
    SendFailured = 3        # メール送信失敗

# 送信頻度
class SendFrequancyEnum(IntEnum):
    EachTime = 0    # 都度
    Summary = 1     # まとめて1回

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
SOURCE_ADDRESS = "hoge"

# カラム名定数
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

# データ型定数
MAX_TYNYINT_UNSIGNED = 255
MAX_SMALLINT_UNSIGNED = 65535

# 埋め込み文字辞書
REPLACE_STR_MAP = {
    "@検知日時@" : DETECTION_DATETIME
    , "@センサ値@" : SENSOR_VALUE
    , "@デバイスID@" : DEVICE_ID
    , "@センサID@" : SENSOR_ID
    , "@センサ名@" : SENSOR_NAME
    , "@単位@" : UNIT
    , "@閾値成立回数条件@" : LIMIT_COUNT_TYPE
    , "@閾値成立回数@" : LIMIT_COUNT
    , "@閾値成立回数リセット@" : LIMIT_COUNT_RESET_RANGE
    , "@通知間隔@" : ACTION_RANGE
    , "@閾値判定区分@" : LIMIT_JUDGE_TYPE
    , "@閾値@" : LIMIT_VALUE
    }

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


def setSourceAddress(sourceAddress):
    global SOURCE_ADDRESS
    SOURCE_ADDRESS = sourceAddress

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
        setSourceAddress(config_ini['ses setting']['sourceAddress'])
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
# メール通知管理テーブル更新用のパラメータ作成
# --------------------------------------------------
def createMailSendManagedParams(sendStatus, record, sendFrequancy):


    params = {}
    params[SEND_STATUS] = sendStatus
    params[UPDATED_AT] = initCommon.getSysDateJst()

    whereArray = []
    whereArray.append("MAIL_SEND_SEQ = %d" % record[MAIL_SEND_SEQ])
    # 送信頻度パラメータに応じて条件追記
    if sendFrequancy == SendFrequancyEnum.EachTime:
        whereArray.append("DATA_COLLECTION_SEQ = %d" % record[DATA_COLLECTION_SEQ])
        whereArray.append("DETECTION_DATETIME = '%s'" % record[DETECTION_DATETIME])
        whereArray.append("LIMIT_SUB_NO = %d" % record[LIMIT_SUB_NO])

    params["whereParams"] = (" AND ".join(whereArray))

    return params

# --------------------------------------------------
# メール送信
# --------------------------------------------------
def send_mail(sesClient, source, to, subject, body):

    response = sesClient.send_email(
        Source=source,
        Destination={
            'ToAddresses' :[
                    to,
                ]
            },
        Message={
            'Subject' : {
                'Data' : subject,
            }
            ,'Body' : {
                'Text' : {
                    'Data': body,
                    }
                }
            }
        )

    return response

#####################
# main
#####################
def lambda_handler(event, context):

    # 初期処理
    initConfig(event["clientName"])
    setLogger(initCommon.getLogger(LOG_LEVEL))

    LOGGER.info('メール通知機能開始 : %s' % event)

    # RDSコネクション作成
    rds = rdsCommon.rdsCommon(LOGGER, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, DB_CONNECT_TIMEOUT)

    # AmazonSESクライアント作成
    sesClient = boto3.client('ses', region_name='ap-northeast-1')

    # メール通知先
    meilSendRecords = rds.fetchall(initCommon.getQuery("sql/t_mail_send_managed/findAllforMailSend.sql"))

    for msRecord in meilSendRecords:

        LOGGER.info("通知先:[%s]" % msRecord)

        # メールソース取得
        sourceRecords = rds.fetchall(initCommon.getQuery("sql/t_mail_send_managed/findbyId.sql")
                                     , { MAIL_SEND_SEQ : msRecord[MAIL_SEND_SEQ]})



        summaryBodyArray = []
        mailTextArray = []
        for i in range(len(sourceRecords)):
            LOGGER.info("メールソース:[%s]" % sourceRecords[i])

            try:
                # メール整形
                resultMailBody = convertMeilText(msRecord[MAIL_TEXT_BODY], sourceRecords[i])
            except Exception as ex:
                LOGGER.warn("メール整形に失敗しました。[%s]" % ex)
                rds.execute(initCommon.getQuery("sql/t_mail_send_managed/update.sql")
                            ,createMailSendManagedParams(SendStatusEnum.MailConvFailured
                                                         , sourceRecords[i]
                                                         , SendFrequancyEnum.Summary))
                break

            # 送信頻度が1の場合は待避
            if (msRecord[SEND_FREQUANCY] == SendFrequancyEnum.Summary):
                summaryBodyArray.append(resultMailBody)

            # 送信頻度判定
            if (msRecord[SEND_FREQUANCY] == SendFrequancyEnum.EachTime):
                mailTextArray = []
                mailTextArray.append(msRecord[MAIL_TEXT_HEADER]) if msRecord[MAIL_TEXT_HEADER] is not None else 0
                mailTextArray.append(resultMailBody)
                mailTextArray.append(msRecord[MAIL_TEXT_FOOTER]) if msRecord[MAIL_TEXT_FOOTER] is not None else 0

            elif(msRecord[SEND_FREQUANCY] == SendFrequancyEnum.Summary and i == len (sourceRecords) -1):
                mailTextArray = []
                mailTextArray.append(msRecord[MAIL_TEXT_HEADER]) if msRecord[MAIL_TEXT_HEADER] is not None else 0
                mailTextArray.append("\r\n".join(summaryBodyArray))
                mailTextArray.append(msRecord[MAIL_TEXT_FOOTER]) if msRecord[MAIL_TEXT_FOOTER] is not None else 0

            if 0 < len(mailTextArray):
                LOGGER.info("メール送信します。[index:%d, 送信頻度:%s, 宛先:%s]"
                            % (i, msRecord[SEND_FREQUANCY], msRecord[EMAIL_ADDRESS]) )
                try:
                    # メール送信 SOURCE_ADDRESS
                    send_mail(sesClient
                              , SOURCE_ADDRESS
                              , msRecord[EMAIL_ADDRESS]
                              , msRecord[MAIL_SUBJECT]
                              , "\r\n".join(mailTextArray))
                    # ステータス更新
                    rds.execute(initCommon.getQuery("sql/t_mail_send_managed/update.sql")
                            ,createMailSendManagedParams(SendStatusEnum.SendSuccess, sourceRecords[i], msRecord[SEND_FREQUANCY]))

                except ClientError as ex:
                    LOGGER.warn("メール送信に失敗しました。[%s]" % ex)

                    rds.execute(initCommon.getQuery("sql/t_mail_send_managed/update.sql")
                            ,createMailSendManagedParams(SendStatusEnum.SendFailured, sourceRecords[i], msRecord[SEND_FREQUANCY]))


    # コミット
    rds.commit()

    # close
    del rds
