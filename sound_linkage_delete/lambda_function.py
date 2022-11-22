import json
import sys
import datetime
import configparser
import boto3
import os
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
# データ定義マスタ参照の可変パラメータ作成
# --------------------------------------------------
def createWhereParam(event):

    whereStr = ""
    whereArray = []
    whereArray.append("DATA_COLLECTION_SEQ = '%s'" % event["dataCollectionSeq"])
    if "createdDateTime" in event:
        whereArray.append("AND CREATED_DATETIME = '%s'" % event["createdDateTime"])


    if 0 < len(whereArray):
        whereStr = " ".join(whereArray)

    return {"p_whereParams" : whereStr}

# --------------------------------------------------
# S3フォルダー削除処理
# --------------------------------------------------
def delete_all_keys_v2(bucket, prefix, dryrun=False):
    contents_count = 0
    next_token = ''
    s3 = boto3.client('s3')

    while True:
        if next_token == '':
            response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        else:
            response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, ContinuationToken=next_token)

        if 'Contents' in response:
            contents = response['Contents']
            contents_count = contents_count + len(contents)
            for content in contents:

                LOGGER.info("音ファイル削除: s3://%s/%s" % (bucket, content['Key']))
                s3.delete_object(Bucket=bucket, Key=content['Key'])

        if 'NextContinuationToken' in response:
            next_token = response['NextContinuationToken']
        else:
            break

    print(contents_count)

#####################
# main
#####################
def lambda_handler(event, context):

    # 初期処理
    initConfig(event["clientName"])
    setLogger(initCommon.getLogger(LOG_LEVEL))

    LOGGER.info('音連携機能_音データ削除開始 : %s' % event)

    # 入力チェック
    isArgument(event)
    
    
    # RDS削除
    # --------------------------------------------------
    # RDSコネクション作成
    rds = rdsCommon.rdsCommon(LOGGER, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, DB_CONNECT_TIMEOUT)

    try:
        # 音ファイル作成履歴削除
        rds.fetchall(initCommon.getQuery("sql/t_soundfile_history/delete.sql"), createWhereParam(event))
        
    except Exception as ex:
        LOGGER.error("削除に失敗しました。ロールバックします。")
        rds.rollBack()
        raise ex

    # commit
    rds.commit()

    # close
    del rds


    # S3削除
    # --------------------------------------------------
    bucket = event["clientName"]
    prefix = 'soundstrage/%s/' % event["edgeId"]

    s3 = boto3.client('s3')
    if "filePath" in event:
        LOGGER.info("音ファイル作成履歴削除 [%s, %s]" % (event["dataCollectionSeq"], event["createdDateTime"]))
        LOGGER.info("音ファイル削除: s3://%s/%s" % (bucket, event["filePath"]))
        s3.delete_object(Bucket=bucket, Key=event["filePath"])
    else:
        LOGGER.info("音ファイル作成履歴削除 [%s]" % (event["dataCollectionSeq"]))
        delete_all_keys_v2(bucket, prefix)
