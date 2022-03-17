import json
import configparser
import initCommon # カスタムレイヤー
import boto3
import datetime
from botocore.exceptions import ClientError

# global
LOG_LEVEL = "INFO"
BUCKET_NAME = ''
CLIENT_NAME = ''

# setter
def setLogLevel(loglevel):
    global LOG_LEVEL
    LOG_LEVEL = loglevel
def setBucketName(bucket_name):
    global BUCKET_NAME
    BUCKET_NAME = bucket_name
def setClientName(client_name):
    global CLIENT_NAME
    CLIENT_NAME = client_name

 
s3 = boto3.resource('s3')  #S3オブジェクトを取得
client = boto3.client('s3')

# --------------------------------------------------
# 設定ファイル読み込み
# --------------------------------------------------
def initConfig(filePath):
    try:
        config_ini = configparser.ConfigParser()
        config_ini.read(filePath, encoding='utf-8')

        setLogLevel(config_ini['logger setting']['loglevel'])
        setBucketName(config_ini['bucket setting']['bucket_name'])
        setClientName(config_ini['bucket setting']['client_name'])
    except Exception as e:
        print ('設定ファイルの読み込みに失敗しました。')
        raise(e)

#####################
# main
#####################
def lambda_handler(event, context):
    
    # 初期処理
    initConfig(event["setting"])
    logger = initCommon.getLogger(LOG_LEVEL)
    
    #dataTypeの判定
    if 0 == event["dataType"]:
        score = "score"
    else:
        score = "sound"

    # keyの作成　{clientName}/score/{yyyyMMdd}/ファイル名
    key = CLIENT_NAME + "/" + score + "/" + initCommon.getSysDateJst().strftime("%Y%m%d")  \
    + "/" + initCommon.getSysDateJst().strftime("%Y-%m-%d %H-%M-%S") + ".json"
    file_contents = json.dumps(event)  
    
    print(event)

    obj = s3.Object(BUCKET_NAME,key)     # ⑧バケット名とパスを指定
    obj.put( Body=file_contents )   # ⑨バケットにファイルを出力
    logger.info("---- backup put end ")

