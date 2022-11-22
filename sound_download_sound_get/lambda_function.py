import json
import boto3
import base64
from typing import List
from pydub import AudioSegment
import io
import configparser
import re
import datetime
import initCommon  # カスタムレイヤー
import rdsCommon  # カスタムレイヤー
from botocore.config import Config

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
EXPIRES_INTERVAL = ''


# パラメータ用定数
CLIENT_NAME = "clientName"
DATA_COLLECTION_SEQ = "dataCollectionSeq"
CREATED_DATETIME = "created_datetime"
FILE_TYPE = "fileType"
FILE_NAME = "fileName"
CREATED_AT = "created_at"

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
    
def setExpiresInterval(expires_interval):
    global EXPIRES_INTERVAL
    EXPIRES_INTERVAL = expires_interval

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
        
        setExpiresInterval(config_ini['s3 setting']['expires_interval'])
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
# 可変パラメータ作成
# --------------------------------------------------
def createWhereParam(event):
    startDateTime = getStartDateTime(event["collectionStartDate"], event["collectionStartTime"])
    endDateTime = getEndDateTime(event["collectionStartDate"], event["collectionEndTime"])

    whereStr = ""
    whereArray = []
    whereArray.append(" DATA_COLLECTION_SEQ = %d" % event[DATA_COLLECTION_SEQ])
    whereArray.append(" CREATED_DATETIME + INTERVAL extract.sec_size SECOND >= '%s'" % startDateTime)
    whereArray.append(" CREATED_DATETIME <= '%s'" % endDateTime)

    if 0 < len(whereArray):
        whereStr = "WHERE " + " AND ".join(whereArray)
 
    return {"p_whereParams" : whereStr}


# --------------------------------------------------
# ファイル開始日時取得
# --------------------------------------------------
def getFileStartDateTime(path: str):
    fileStr = path.split('/')[3].split('_')
    datetimeStr = fileStr[0]
    if datetimeStr[-1] == " ":
        datetimeStr = datetimeStr[:-1]
    fileStartDateTime = datetime.datetime.strptime(datetimeStr, "%Y-%m-%d %H:%M:%S")
    return fileStartDateTime

# --------------------------------------------------
# ファイル終了日時取得
# --------------------------------------------------
def getFileEndDateTime(path: str):
    fileStr = path.split('/')[3].split('_')
    datetimeStr = fileStr[0]
    collectionTimeStr = fileStr[1].split('.')[0]
    collectionMinute = collectionTimeStr.split(':')[0]
    collectionSecond = collectionTimeStr.split(':')[1]
    if datetimeStr[-1] == " ":
        datetimeStr = datetimeStr[:-1]
    fileStartDateTime = datetime.datetime.strptime(datetimeStr, "%Y-%m-%d %H:%M:%S")
    fileEndDateTime = fileStartDateTime + datetime.timedelta(minutes=int(collectionMinute)) + datetime.timedelta(seconds=int(collectionSecond))
    return fileEndDateTime

# --------------------------------------------------
# 収集開始日時取得
# --------------------------------------------------
def getStartDateTime(startDate: str, startTime: str):
    startDateTime = datetime.datetime.strptime(startDate + ' ' + startTime, "%Y-%m-%d %H:%M:%S")
    return startDateTime

# --------------------------------------------------
# 収集終了日時取得
# --------------------------------------------------
def getEndDateTime(startDate: str, endTime: str):
    endDateTime = datetime.datetime.strptime(startDate + ' ' + endTime, "%Y-%m-%d %H:%M:%S")
    return endDateTime

#####################
# main
#####################
def lambda_handler(event, context):
    
    # 初期処理
    initConfig(event["clientName"])
    setLogger(initCommon.getLogger(LOG_LEVEL))

    LOGGER.info('音ファイル取得機能_音ファイルダウンロード開始 : %s' % event)

    # 入力チェック
    isArgument(event)
    
    # RDSコネクション作成
    rds = rdsCommon.rdsCommon(LOGGER, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, DB_CONNECT_TIMEOUT)

    # マスタselect
    result = rds.fetchall(initCommon.getQuery("sql/t_soundfile_history/findId.sql")
                          , createWhereParam(event))
    
    if not result:
        raise Exception("音ファイルダウンロードに失敗しました。音ファイルが存在しません。")

    # 開始時間、終了時間成形
    startDateTime = getStartDateTime(event["collectionStartDate"], event["collectionStartTime"])
    endDateTime = getEndDateTime(event["collectionStartDate"], event["collectionEndTime"])

    # 音ファイル
    resultSound = AudioSegment.silent(duration=0)
    s3 = boto3.client('s3')
    for soundInfo in result:
        LOGGER.info('音ファイル情報 : %s' % soundInfo)
        fileStartDateTime = datetime.datetime.strptime(soundInfo["createdDateTime"], "%Y/%m/%d %H:%M:%S")
        fileEndDateTime = datetime.datetime.strptime(soundInfo["endDateTime"], "%Y/%m/%d %H:%M:%S")
        fileTime = (fileEndDateTime-fileStartDateTime).total_seconds()*1000

        #収集期間外（前）
        if fileEndDateTime < startDateTime :
            continue
        #収集期間外（後）
        if endDateTime < fileStartDateTime :
            continue

        s3_object = s3.get_object(Bucket=event["clientName"], Key=soundInfo["fileName"])['Body'].read()
        sound = AudioSegment.from_file(io.BytesIO(s3_object), format="wav")
        
        #収集対象（前カット）
        if fileStartDateTime <= startDateTime and fileEndDateTime <= endDateTime :
            LOGGER.info('収集対象（前カット）')
            offset = (startDateTime - fileStartDateTime).total_seconds()*1000
            resultSound = resultSound + sound[offset:]
            continue
        
        #収集対象（後カット）
        if startDateTime <= fileStartDateTime and endDateTime <= fileEndDateTime :
            LOGGER.info('収集対象（後カット）')
            offset = (fileEndDateTime - endDateTime).total_seconds()*1000
            resultSound = resultSound + sound[:fileTime-offset]
            continue
        
        #収集対象（前後カット）
        if fileStartDateTime <= startDateTime and endDateTime <= fileEndDateTime :
            LOGGER.info('収集対象（前後カット）')
            startOffset = (startDateTime - fileStartDateTime).total_seconds()*1000
            endOffset = (fileEndDateTime - endDateTime).total_seconds()*1000
            resultSound = resultSound + sound[startOffset:fileTime-endOffset]
            continue
        
        LOGGER.info('収集対象（カットなし）')
        #収集対象（カットなし）
        resultSound = resultSound + sound
    
    time = resultSound.duration_seconds
    LOGGER.info('音ファイル収録時間 : %s' % time)
    if time < 1 :
        raise Exception("音ファイルダウンロードに失敗しました。音ファイルが存在しません。")
    
    exportSound = resultSound.export(format="wav")
    
    # S3リソースのインスタンス作成
    s3 = boto3.resource('s3')
    
    # S3保存先
    fileTime = str(int(time))
    filename = event["collectionStartDate"] + " " + event["collectionStartTime"] + " " + fileTime + "sec.wav"
    key = "soundstrage/tmp/%s" % (filename)
    
    # 音ファイルをS3へput
    obj = s3.Object(event[CLIENT_NAME], key)
    obj.put( Body=exportSound, ContentType="audio/wav" )
    
    
    # S3クライアント生成
    my_config = Config(
            region_name = 'ap-northeast-1',
            signature_version = 's3v4'
            )
    s3 = boto3.client('s3',config=my_config)
    
    # s3からダウンロードURLの取得
    header_location = s3.generate_presigned_url(
        ClientMethod = 'get_object',
        Params = {'Bucket' : event["clientName"], 'Key' : key, \
                "ResponseContentDisposition" : "attachment;soundResult['fileName']"},
        ExpiresIn = EXPIRES_INTERVAL,
        HttpMethod = 'GET'
        )
    LOGGER.info("ダウンロード用の署名付きURL : %s" % header_location)
    result = {
        "key": key,
        "Location": header_location
        
    }
    
    return result

    # return base64.b64encode(exportSound.read()).decode('utf-8')