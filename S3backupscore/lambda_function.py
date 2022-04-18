import json
import configparser
import initCommon  # カスタムレイヤー
import boto3
import datetime


# global
LOGGER = None
LOG_LEVEL = "INFO"


# setter
def setLogger(logger):
    global LOGGER
    LOGGER = logger


def setLogLevel(loglevel):
    global LOG_LEVEL
    LOG_LEVEL = loglevel


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
    except Exception as e:
        print ('設定ファイルの読み込みに失敗しました。')
        raise(e)


#####################
# main
#####################
def lambda_handler(event, context):
    
    # 初期処理
    initConfig(event["clientName"])
    setLogger(initCommon.getLogger(LOG_LEVEL))
    LOGGER.info("S3バックアップ開始 : %s" % event)
    
    # S3オブジェクトを取得
    s3 = boto3.resource('s3')  

    # 対象ファイルをJson→文字列化    
    file_contents = json.dumps(event)
    
    # 保存先
    nowDate = initCommon.getSysDateJst()
    key = "bkstrage/amqp/%s/%s.json" % (nowDate.strftime("%Y%m%d"), nowDate.strftime("%Y-%m-%d %H-%M-%S")) 
    LOGGER.info("S3保存先 [%s][%s]" % (event["clientName"], key))

    obj = s3.Object(event["clientName"], key)  # ⑧バケット名とパスを指定
    obj.put(Body=file_contents)  # ⑨バケットにファイルを出力
    LOGGER.info("S3バックアップ終了")

