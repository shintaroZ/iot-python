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
MAIL_SEND_ID = "mailSendId"
SEND_STATUS = "sendStatus"

DEVICE_ID = "deviceId"
SENSOR_ID = "sensorId"
# DATA_COLLECTION_SEQ = "dataCollectionSeq"
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
# LIMIT_SUB_NO = "limitSubNo"
LIMIT_JUDGE_TYPE = "limitJudgeType"
LIMIT_VALUE = "limitValue"

# MAIL_SEND_ID = "mailSendId"
EMAIL_ADDRESS = "emailAddress"
SEND_WEEK_TYPE = "sendWeekType"
SEND_FREQUANCY = "sendFrequancy"
SEND_TIME_FROM = "sendTimeFrom"
SEND_TIME_TO = "sendTimeTo"
MAIL_SUBJECT = "mailSubject"
MAIL_TEXT = "mailText"

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
# Map形式でヘッダー部、ボディ部、フッター部と３分割で返却
# --------------------------------------------------
def convertMeilText(mailText, record):
    resultMap = {}

    # メール本文を改行(\r\n)で分割
    mailTextArray = mailText.split("\r\n")

    isHeaderAppend = False
    isFooterAppend = False
    isEnd = False
    bodyArray = []
    headerArray = []
    footerArray = []
    LOGGER.debug("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s" % ("isSharp", "isHeader", "isHeaderAppend", "isFooter", "isFooterAppend", "isEnd", "len(%)", "len($)", "メール本文"))
    for i in range(len(mailTextArray)):
        percentArray = re.findall("%.*?%", mailTextArray[i])
        dollArray = re.findall("\$.*?\$", mailTextArray[i])
        isSharp = True if 0 < len(re.findall("#.*#", mailTextArray[i])) else False

        isHeader = True if 0 < len(re.findall("#HEADER#", mailTextArray[i])) else False
        isFooter = True if 0 < len(re.findall("#FOOTER#", mailTextArray[i])) else False
        isEnd = True if 0 < len(re.findall("#END#", mailTextArray[i])) else False

        # #囲みはHEADとFOOTER判定
        if isHeader:
            isHeaderAppend = True
        if isFooter:
            isFooterAppend = True
        if isEnd:
            isHeaderAppend = False
            isFooterAppend = False

        LOGGER.debug("%s\t%s\t%s\t%s\t%s\t%s\t%d\t%d\t%s" % (isSharp, isHeader, isHeaderAppend, isFooter, isFooterAppend, isEnd, len(percentArray), len(dollArray), mailTextArray[i]))

        # $囲みはexecで実行
        for d in dollArray:
            # $を削除して実行
            exec(d.replace("$", ""))

        # %囲みはevalで値埋め込み
        for s in percentArray:
            # %を削除して実行
            result = eval(s.replace("%", ""))

            # データ型判定して文字列へキャスト
            convStr = ""
            if type(result) is str:
                convStr = result
            elif type(result) is int:
                convStr = str(result)
            elif type(result) is float:
                convStr = str(result)
            elif type(result) is datetime.datetime:
                convStr = result.strftime('%Y/%m/%d %H:%M:%S.%f')

            # 置換
            mailTextArray[i] = mailTextArray[i].replace(s, convStr)

        # header部の追加判定
        if (isHeaderAppend and isHeader == False):
            headerArray.append(mailTextArray[i])

        # body部の追加判定
        if (len(dollArray) == 0 and isHeaderAppend == False
            and isFooterAppend == False and isSharp == False):
            bodyArray.append(mailTextArray[i])

        # body部の追加判定
        if (isFooterAppend and isFooter == False):
            footerArray.append(mailTextArray[i])

    # 文字列で返却
    resultMap["header"] = "\r\n".join(headerArray)
    resultMap["body"] = "\r\n".join(bodyArray)
    resultMap["footer"] = "\r\n".join(footerArray)
    return resultMap

# --------------------------------------------------
# メール通知管理テーブル更新用のパラメータ作成
# --------------------------------------------------
def createMailSendManagedParams(sendStatus, record, sendFrequancy):

    params = {}
    params[SEND_STATUS] = sendStatus
    params[UPDATED_AT] = initCommon.getSysDateJst()

    whereArray = []
    whereArray.append("MAIL_SEND_ID = %d" % record[MAIL_SEND_ID])
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

    # メール通知条件取得
    mmsRecords = rds.fetchall(initCommon.getQuery("sql/m_mail_send/findAll.sql"))

    for mmsRecord in mmsRecords:

        LOGGER.info("通知先ユーザ取得結果:[通知ID:%d, 通知先メールアドレス:%s, 送信頻度:%d]"%
                    (mmsRecord[MAIL_SEND_ID], mmsRecord[EMAIL_ADDRESS], mmsRecord[SEND_FREQUANCY]))
        # メールソース取得
        sourceRecords = rds.fetchall(initCommon.getQuery("sql/t_mail_send_managed/findbyId.sql")
                                     , {MAIL_SEND_ID : mmsRecord[MAIL_SEND_ID]})

        resultBodyArray = []
        for i in range(len(sourceRecords)):
            LOGGER.info("メールソース取得結果:[データ定義マスタシーケンス:%d, 検知日時:%s, 閾値通番:%d, メール通知ID:%d]" %
                        (sourceRecords[i][DATA_COLLECTION_SEQ]
                         ,sourceRecords[i][DETECTION_DATETIME]
                         ,sourceRecords[i][LIMIT_SUB_NO]
                         ,sourceRecords[i][MAIL_SEND_ID]))

            # メール整形
            try:
                resultMap = convertMeilText(mmsRecord[MAIL_TEXT], sourceRecords[i])
                resultBodyArray.append(resultMap["body"])
            except Exception as ex:
                LOGGER.warn("メール整形に失敗しました。[%s]" % ex)
                rds.execute(initCommon.getQuery("sql/t_mail_send_managed/update.sql")
                            ,createMailSendManagedParams(SendStatusEnum.MailConvFailured
                                                         , sourceRecords[i]
                                                         , SendFrequancyEnum.Summary))
                break


            # 送信頻度判定
            if (mmsRecord[SEND_FREQUANCY] == SendFrequancyEnum.EachTime
                or (mmsRecord[SEND_FREQUANCY] == SendFrequancyEnum.Summary and i == len(sourceRecords) -1 )):


                mailBodyArray = []
                mailBodyArray.append(resultMap["header"]) if resultMap["header"] != "" else 0
                mailBodyArray.append("\r\n".join(resultBodyArray))
                mailBodyArray.append(resultMap["footer"]) if resultMap["footer"] != "" else 0

                LOGGER.info("メール送信します。[%s]" %(mmsRecord[EMAIL_ADDRESS]) )

                try:
                    # メール送信
                    send_mail(sesClient
                              , SOURCE_ADDRESS
                              , mmsRecord[EMAIL_ADDRESS]
                              , mmsRecord[MAIL_SUBJECT]
                              , "\r\n".join(mailBodyArray))
                    # ステータス更新
                    rds.execute(initCommon.getQuery("sql/t_mail_send_managed/update.sql")
                            ,createMailSendManagedParams(SendStatusEnum.SendSuccess
                                                         , sourceRecords[i]
                                                         , mmsRecord[SEND_FREQUANCY]))

                except ClientError as ex:
                    LOGGER.warn("メール送信に失敗しました。[%s]" % ex)

                    rds.execute(initCommon.getQuery("sql/t_mail_send_managed/update.sql")
                            ,createMailSendManagedParams(SendStatusEnum.SendFailured
                                                         , sourceRecords[i]
                                                         , mmsRecord[SEND_FREQUANCY]))


    # コミット
    rds.commit()

    # close
    del rds

