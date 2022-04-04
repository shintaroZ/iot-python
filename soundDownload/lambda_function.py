import json
import boto3
import sys
import configparser
import initCommon # カスタムレイヤー
import rdsCommon # カスタムレイヤー
import datetime
from botocore.config import Config

# global
LOGGER = None
LOG_LEVEL = "INFO"
DB_HOST = "localhost"
DB_PORT = 3306
DB_USER = "hoge"
DB_PASSWORD = "hoge"
DB_NAME = "hoge"
DB_CONNECT_TIMEOUT = 3
BUCKET_NAME = ''
CLIENT_NAME = ''
EXPIRES_INTERVAL = ''

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
def setBucketName(bucket_name):
    global BUCKET_NAME
    BUCKET_NAME = bucket_name
def setClientName(client_name):
    global CLIENT_NAME
    CLIENT_NAME = client_name
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
        # setBucketName(config_ini['bucket setting']['bucket_name'])
        # setClientName(config_ini['bucket setting']['client_name'])
        setExpiresInterval(config_ini['s3 setting']['expires_interval'])
    except Exception as e:
        print ('設定ファイルの読み込みに失敗しました。')
        raise(e)

# --------------------------------------------------
# 音ファイル作成履歴取得用のパラメータ生成
# --------------------------------------------------
def createSoundParam(dataCollectionSeq, createdDateTime, fileType):
    param = {}
    param["p_dataCollectionSeq"] = dataCollectionSeq
    param["p_createdDateTime"] = createdDateTime
    param["p_fileType"] = fileType
    return param

#####################
# main
#####################
def lambda_handler(event, context):

   # 初期処理
    initConfig(event["clientName"])
    setLogger(initCommon.getLogger(LOG_LEVEL))

    LOGGER.info("音ファイルダウンロード機能開始:%s" % event)

    # RDSコネクション作成
    rds = rdsCommon.rdsCommon(LOGGER, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, DB_CONNECT_TIMEOUT)
    
    #soundIdの取得(変数の設定を見直す)
    # soundId = event["soundId"]
    # パラメータ取得
    dataCollectionSeq = event["dataCollectionSeq"]
    createdDateTime = event["createdDateTime"]
    fileType = event["fileType"]
    playMode = event["playMode"]
    
    #soundIdからファイル名の取得
    soundResult = rds.fetchone(initCommon.getQuery("sql/sound/findById.sql"),
                                  createSoundParam(dataCollectionSeq, createdDateTime, fileType))

    #none判定
    if soundResult is None:
        errMsg = "ファイル名の取得に失敗しました。"
        raise Exception(errMsg)

    # date = soundResult['createdDatetime']
    # createDate = datetime.datetime(date.year, date.month, date.day)
    #

    # 日付文字列を日付型へ変換
    createdDt = datetime.datetime.strptime(createdDateTime, '%Y/%m/%d %H:%M:%S')
    createdYMD =  createdDt.strftime("%Y%m%d")
    print (createdDt)
    print (createdYMD)
    
    #
    # # バケット、ファイル名の設定
    # fileName = CLIENT_NAME + "/" + soundResult['edgeName'] + "/" \
    # + createDate.strftime("%Y%m%d")+ "/" + soundResult['fileName']

    #boto3.client()インスタンス取得
    my_config = Config(
            region_name = 'ap-northeast-1',
            signature_version = 's3v4'
            )
    s3 = boto3.client('s3',
            config=my_config)

    #s3存在判定
    # key = CLIENT_NAME + "/" + soundResult['edgeName'] + "/" + \
    # createDate.strftime("%Y%m%d") + "/" + soundResult['fileName']
    # todo 固定
    key = "soundstrage/EDGE_001/20210511/20210511104430.mp3"
    contentsresult = s3.list_objects(Bucket=event["clientName"], Prefix=key)
    if "Contents" in contentsresult:
        if event["playMode"] == 0:
        # s3からダウンロードURLの取得
            header_location = s3.generate_presigned_url(
                ClientMethod = 'get_object',
                Params = {'Bucket' : event["clientName"], 'Key' : key, \
                "ResponseContentDisposition" : "attachment;soundResult['fileName']"},
                ExpiresIn = EXPIRES_INTERVAL,
                HttpMethod = 'GET'
                )
            print(header_location)
            result = {"Location": header_location}
            return result
        else:
        # s3から署名付きURLの取得
            header_location = s3.generate_presigned_url(
                ClientMethod = 'get_object',
                Params = {'Bucket' : event["clientName"], 'Key' : key, \
                "ResponseContentDisposition": "inline",
                "ResponseContentType" : "audio/mp3"},

                ExpiresIn = EXPIRES_INTERVAL,
                HttpMethod = 'GET'
                )
            print(header_location)
            result = {"Location": header_location}
            return result    
    else:
        LOGGER.error("音ファイルの取得に失敗しました。")
        return {"message":"音ファイルの取得に失敗しました。"}
        sys.exit()
        
    


