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
USER_NAME = ""

# カラム名定数
CLIENT_NAME = "clientName"

HIERARCHY_ID = "hierarchyId"
HIERARCHY_NAME = "hierarchyName"
HIERARCHY_LEVEL = "hierarchyLevel"


CREATED_AT = "createdAt"
UPDATED_AT = "updatedAt"
UPDATED_USER = "updatedUser"
VERSION = "version"

# データ型定数
MAX_TYNYINT_UNSIGNED = 255

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
    
    # body部
    eBody = event["bodyRequest"]

    # 必須項目チェック
    noneErrArray = []
    noneErrArray.append(HIERARCHY_NAME) if (HIERARCHY_NAME not in eBody) else 0
    noneErrArray.append(HIERARCHY_LEVEL) if (HIERARCHY_LEVEL not in eBody) else 0
    
    # 必須項目がない場合は例外スロー
    if 0 < len(noneErrArray):
        raise Exception ("Missing required request parameters. [%s]" % ",".join(noneErrArray))


    # 型チェック
    typeErrArray = []
    typeErrArray.append(HIERARCHY_LEVEL) if (initCommon.isValidateNumber(eBody[HIERARCHY_LEVEL]) == False) else 0
    
    # 型異常の場合は例外スロー
    if 0 < len(typeErrArray):
        raise TypeError("The parameters is type invalid. [%s]" % ",".join(typeErrArray))
    
    
    # データ長チェック
    lengthArray = []
    lengthArray.append(HIERARCHY_NAME) if (30 < len(eBody[HIERARCHY_NAME])) else 0
    lengthArray.append(HIERARCHY_LEVEL) if (MAX_TYNYINT_UNSIGNED < eBody[HIERARCHY_LEVEL]) else 0

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
            raise Exception("clientNameがグループに属していません。clientName:%s groupName:%s" % (event["clientName"], ",".join(groupList) ))
    except Exception as ex:
        raise Exception("Authentication Error. [%s]" %  ex)
        
        
    return

# --------------------------------------------------
# 設備マスタ参照の可変パラメータ作成
# --------------------------------------------------
def createWhereParam(event):

    whereStr = ""
    whereArray = []
    if HIERARCHY_ID in event:
        whereArray.append("AND mh.HIERARCHY_ID = '%s'" % event[HIERARCHY_ID])

    if 0 < len(whereArray):
        whereStr = " ".join(whereArray)

    return {"p_whereParams" : whereStr}

# --------------------------------------------------
# 起動パラメータに設備マスタ用のパラメータを付与して返却する。
# --------------------------------------------------
def createEquipmentParams(event, version, equipmentId):

    event[HIERARCHY_ID] = equipmentId

    return createCommonParams(event, version)

# --------------------------------------------------
# 起動パラメータに共通情報を付与して返却する。
# --------------------------------------------------
def createCommonParams(event, version):

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

    LOGGER.info('マスタメンテナンス機能_階層マスタ更新開始 : %s' % event)

    # 入力チェック
    isArgument(event)

    # RDSコネクション作成
    rds = rdsCommon.rdsCommon(LOGGER, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, DB_CONNECT_TIMEOUT)

    # バージョン取得
    result = rds.fetchone(initCommon.getQuery("sql/m_hierarchy/findbyId.sql")
                          , createWhereParam(event))

    # バージョンのインクリメント、
    version = 0
    if result is not None and VERSION in result:
        version = result[VERSION] + 1
        LOGGER.info("登録対象バージョン [%d]" % version)

    # body部
    eBody = event["bodyRequest"]
    try:

        # 階層マスタのINSERT
        LOGGER.info("階層マスタのINSERT [%s]" % (event[HIERARCHY_ID]) )
        rds.execute(initCommon.getQuery("sql/m_hierarchy/insert.sql"), createEquipmentParams(eBody, version, event[HIERARCHY_ID]))
    
    except Exception as ex:
        LOGGER.error("登録に失敗しました。ロールバックします。")
        rds.rollBack()
        raise ex

    # commit
    rds.commit()

    # close
    del rds

