import json
import boto3
import configparser
import initCommon # カスタムレイヤー

# global
LOGGER = None
LOG_LEVEL = "INFO"

# パラメータ用定数
CLIENT_NAME = "clientName"
KEY = "key"

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

#####################
# main
#####################
def lambda_handler(event, context):
    # 初期処理
    initConfig(event[CLIENT_NAME])
    setLogger(initCommon.getLogger(LOG_LEVEL))

    LOGGER.info('音ファイル取得機能_音ファイル削除開始 : %s' % event)
    
    s3 = boto3.resource('s3')
    s3.Object(event[CLIENT_NAME], event[KEY]).delete()