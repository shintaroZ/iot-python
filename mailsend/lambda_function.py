import json
import sys
import datetime
import configparser
import initCommon  # カスタムレイヤー
import rdsCommon  # カスタムレイヤー
import boto3
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
SNS_TOPIC_ARN = "hoge"

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


def setSnsTopicArn(snsTopicArn):
    global SNS_TOPIC_ARN
    SNS_TOPIC_ARN = snsTopicArn

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
        setSnsTopicArn(config_ini['rds setting']['snsTopicArn'])
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

    mailTextArray = []
    mailTextArray.append("#HEAD#")
    mailTextArray.append("閾値異常を検知いたしました。")
    mailTextArray.append("=============================")
    mailTextArray.append("#END#")
    mailTextArray.append("$swtStr='より超過しています' if record['limitJudgeType'] == 0 else 'と一致しています' if record['limitJudgeType'] == 1 else 'より下回っています'$")
    mailTextArray.append("【検知日時】%record['detectionDateTime']%")
    mailTextArray.append("【デバイスID】%record['deviceId']%")
    mailTextArray.append("【センサID】%record['sensorId']%")
    mailTextArray.append("【センサ名】%record['sensorName']%")
    mailTextArray.append("【値】%record['sensorValue']%:::%record['sensorUnit']%")
    mailTextArray.append("【閾値】%record['limitValue']%%swtStr%")
    mailTextArray.append("#FOOTER#")
    mailTextArray.append("=============================")
    mailTextArray.append("※この電子メールは、送信専用メールアドレスよりお送りしています。メールの返信は受け付けておりません。予めご了承下さい。")
    mailTextArray.append("#END#")

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

        isHeader = True if 0 < len(re.findall("#HEAD#", mailTextArray[i])) else False
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

    resultMap["header"] = headerArray
    resultMap["body"] = bodyArray
    resultMap["footer"] = footerArray
    return resultMap

def createMailSendManagedParams(sendStatus, record):

    params = {}
    params[SEND_STATUS] = sendStatus
    params[DATA_COLLECTION_SEQ] = record[DATA_COLLECTION_SEQ]
    params[DETECTION_DATETIME] = record[DETECTION_DATETIME]
    params[LIMIT_SUB_NO] = record[LIMIT_SUB_NO]
    params[MAIL_SEND_ID] = record[MAIL_SEND_ID]
    params[UPDATED_AT] = initCommon.getSysDateJst()
    return params

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
    setLogger(initCommon.getLogger("DEBUG"))

    LOGGER.info('メール送信機能開始 : %s' % event)


    # RDSコネクション作成
    rds = rdsCommon.rdsCommon(LOGGER, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, DB_CONNECT_TIMEOUT)

    # メール通知条件取得
    mmsRecords = rds.fetchall(initCommon.getQuery("sql/m_mail_send/findAll.sql"))

    for mmsRecord in mmsRecords:

        LOGGER.info("通知条件を満たしたので、処理を継続します。[%d : %s : %d]"%
                    (mmsRecord[MAIL_SEND_ID], mmsRecord[EMAIL_ADDRESS], mmsRecord[SEND_FREQUANCY]))
        # メールソース取得
        sourceRecords = rds.fetchall(initCommon.getQuery("sql/t_mail_send_managed/findbyId.sql")
                                     , {MAIL_SEND_ID : mmsRecord[MAIL_SEND_ID]})

        mailBodyArray = []
        for i in range(len(sourceRecords)):
            LOGGER.info("メール通知管理に紐付くデータの取得に成功しました。:[%d : %s : %d :%d]" %
                        (sourceRecords[i][DATA_COLLECTION_SEQ]
                         ,sourceRecords[i][DETECTION_DATETIME]
                         ,sourceRecords[i][LIMIT_SUB_NO]
                         ,sourceRecords[i][MAIL_SEND_ID]))

            # メール整形
            try:
                resultMap = convertMeilText(mmsRecord[MAIL_TEXT], sourceRecords[i])
                mailBodyArray.append(resultMap["body"])
            except Exception as ex:
                LOGGER.warn("メール整形に失敗しました。[%s]" % ex)
                rds.execute(initCommon.getQuery("sql/t_mail_send_managed/update.sql")
                            ,createMailSendManagedParams(SendStatusEnum.MailConvFailured, sourceRecords[i]))

            # 送信頻度判定
            if (mmsRecord[SEND_FREQUANCY] == SendFrequancyEnum.EachTime or i == len(sourceRecords)):
                # メール送信


                # ★
                for h in resultMap["header"]:
                    print(h)
                for b in resultMap["body"]:
                    print(b)
                for f in resultMap["footer"]:
                    print(f)

                # メール送信

#     # AmazonSESクライアント作成
#     sesClient = boto3.client('ses', region_name='ap-northeast-1')



    # close
    del rds

