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
USER_NAME = ""

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


def setUserName(userName):
    global USER_NAME
    USER_NAME = userName

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

    # ボディ部
    eBody = event["bodyRequest"]
    
    # 必須項目チェック
    noneErrArray = []
    # noneErrArray.append(MAIL_SEND_ID) if (MAIL_SEND_ID not in eBody) else 0
    noneErrArray.append(EMAIL_ADDRESS)if (EMAIL_ADDRESS not in eBody) else 0
    noneErrArray.append(SEND_WEEK_TYPE) if (SEND_WEEK_TYPE not in eBody) else 0
    noneErrArray.append(SEND_FREQUANCY) if (SEND_FREQUANCY not in eBody) else 0
    noneErrArray.append(SEND_TIME_FROM) if (SEND_TIME_FROM not in eBody) else 0
    noneErrArray.append(SEND_TIME_TO) if (SEND_TIME_TO not in eBody) else 0
    noneErrArray.append(MAIL_SUBJECT) if (MAIL_SUBJECT not in eBody) else 0
    noneErrArray.append(MAIL_TEXT_BODY) if (MAIL_TEXT_BODY not in eBody) else 0

    # 必須項目がない場合は例外スロー
    if 0 < len(noneErrArray):
        raise Exception ("Missing required request parameters. [%s]" % ",".join(noneErrArray))

    # 型チェック
    typeErrArray = []
    # typeErrArray.append(MAIL_SEND_ID) if (initCommon.isValidateNumber(eBody[MAIL_SEND_ID]) == False) else 0
    typeErrArray.append(SEND_WEEK_TYPE) if (initCommon.isValidateNumber(eBody[SEND_WEEK_TYPE]) == False) else 0
    typeErrArray.append(SEND_FREQUANCY) if (initCommon.isValidateNumber(eBody[SEND_FREQUANCY]) == False) else 0

    # 型異常の場合は例外スロー
    if 0 < len(typeErrArray):
        raise TypeError("The parameters is type invalid. [%s]" % ",".join(typeErrArray))

    # メール通知IDの範囲チェック
    rangeArray = []
    rangeArray.append(MAIL_SEND_ID) if(5 < event[MAIL_SEND_ID] or event[MAIL_SEND_ID] < 1) else 0

    # メール通知IDが範囲外の場合は例外スロー
    if 0 < len(rangeArray):
        raise Exception("The parameters is range invalid. [%s]" % ",".join(rangeArray))

    # データ長チェック
    lengthArray = []
    lengthArray.append(EMAIL_ADDRESS) if (256 < len(eBody[EMAIL_ADDRESS])) else 0
    lengthArray.append(SEND_TIME_FROM) if (6 < len(eBody[SEND_TIME_FROM])) else 0
    lengthArray.append(SEND_TIME_TO) if (6 < len(eBody[SEND_TIME_TO])) else 0
    lengthArray.append(MAIL_SUBJECT) if (30 < len(eBody[MAIL_SUBJECT])) else 0
    lengthArray.append(SEND_WEEK_TYPE) if (MAX_TYNYINT_UNSIGNED < eBody[SEND_WEEK_TYPE]) else 0
    lengthArray.append(SEND_FREQUANCY) if (MAX_TYNYINT_UNSIGNED < eBody[SEND_FREQUANCY]) else 0
    # マルチバイト文字は2バイト計算
    lengthArray.append(MAIL_TEXT_HEADER) if (MAIL_TEXT_HEADER in eBody and 65535 < len(eBody[MAIL_TEXT_HEADER].encode("shift-jis"))) else 0
    lengthArray.append(MAIL_TEXT_BODY) if (65535 < len(eBody[MAIL_TEXT_BODY].encode("shift-jis"))) else 0
    lengthArray.append(MAIL_TEXT_FOOTER) if (MAIL_TEXT_FOOTER in eBody and 65535 < len(eBody[MAIL_TEXT_FOOTER].encode("shift-jis"))) else 0

    # データ長異常の場合は例外スロー
    if 0 < len(lengthArray):
        raise TypeError("The parameters is length invalid. [%s]" % ",".join(lengthArray))

    # トークン取得
    token = event["idToken"]
    
    # ユーザ名／グループ名
    try:
        setUserName(initCommon.getPayLoadKey(token, "cognito:username")[:20] )
        groupList = initCommon.getPayLoadKey(token, "cognito:groups")
    
        # 顧客名がグループ名に含まれること
        if (event["clientName"] not in groupList):
            raise Exception("顧客名がグループ名と異なります。clientName:%s groupName:%s" % (event["clientName"], ",".join(USER_GROUP_LIST) ))
    except Exception as ex:
        raise Exception("Authentication Error. [%s]" %  ex)
    return

# --------------------------------------------------
# 起動パラメータに共通情報を付与して返却する。
# --------------------------------------------------
def createCommonParams(mailSendId, event, version, mailSendSeq):

    if MAIL_TEXT_HEADER in event:
        event["insert_%s" % MAIL_TEXT_HEADER] = ", `MAIL_TEXT_HEADER`"
        event["values_%s" % MAIL_TEXT_HEADER] = ", '%s'" % event[MAIL_TEXT_HEADER]
    else:
        event["insert_%s" % MAIL_TEXT_HEADER] = ""
        event["values_%s" % MAIL_TEXT_HEADER] = ""

    if MAIL_TEXT_FOOTER in event:
        event["insert_%s" % MAIL_TEXT_FOOTER] = ", `MAIL_TEXT_FOOTER`"
        event["values_%s" % MAIL_TEXT_FOOTER] = ", '%s'" % event[MAIL_TEXT_FOOTER]
    else:
        event["insert_%s" % MAIL_TEXT_FOOTER] = ""
        event["values_%s" % MAIL_TEXT_FOOTER] = ""

    event[MAIL_SEND_SEQ] = mailSendSeq
    event[MAIL_SEND_ID] = mailSendId
    event[CREATED_AT] = initCommon.getSysDateJst()
    event[UPDATED_AT] = initCommon.getSysDateJst()
    event[UPDATED_USER] = USER_NAME
    event[VERSION] = version
    return event


#####################
# main
#####################
def lambda_handler(event, context):

    # 初期処理
    initConfig(event["clientName"])
    setLogger(initCommon.getLogger(LOG_LEVEL))
    # setLogger(initCommon.getLogger("DEBUG"))

    LOGGER.info('マスタメンテナンス機能_メール通知マスタ更新開始 : %s' % event)

    # トークン取得
    token = event["idToken"]
    setUserName(initCommon.getPayLoadKey(token, "cognito:username")[:20] )

    # 入力チェック
    isArgument(event)

    # RDSコネクション作成
    rds = rdsCommon.rdsCommon(LOGGER, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, DB_CONNECT_TIMEOUT)

    # バージョン取得
    result = rds.fetchone(initCommon.getQuery("sql/m_mail_send/findbyId.sql")
                          , {MAIL_SEND_ID : event[MAIL_SEND_ID]})

    # バージョンのインクリメント
    version = 0
    if result is not None and VERSION in result:
        version = result[VERSION] + 1
        LOGGER.info("登録対象バージョン [%d]" % version)

    # シーケンス取得
    mailSendSeq = 0
    seqDcResult = rds.fetchone(initCommon.getQuery("sql/m_seq/nextval.sql"), {"p_seqType" : 1})
    LOGGER.info("メール通知マスタシーケンスの新規採番 [%d]" % seqDcResult["nextSeq"])
    mailSendSeq = seqDcResult["nextSeq"]

    try:
        # メール通知マスタのINSERT
        LOGGER.info("メール通知マスタのINSERT [mailSendId = %d]" % event[MAIL_SEND_ID])
        rds.execute(initCommon.getQuery("sql/m_mail_send/insert.sql"), createCommonParams(event[MAIL_SEND_ID], event["bodyRequest"], version, mailSendSeq) )
    except Exception as ex:
        LOGGER.error("登録に失敗しました。ロールバックします。")
        rds.rollBack()
        raise ex

    # commit
    rds.commit()

    # close
    del rds

