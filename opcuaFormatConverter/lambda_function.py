import json
import configparser
import initCommon  # カスタムレイヤー
import opcModel 
import dgwModel 
import boto3
import datetime
from datetime import datetime as dt

# global
LOGGER = None
LOG_LEVEL = "INFO"


# パラメータ用定数
CLIENT_NAME = "clientName"
RECEIVED_MESSAGES = "receivedMessages"
REQUEST_TIMESTAMP = "requestTimestamp"

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
    LOGGER.info("OPC-UAデータ加工処理開始 : %s" % event)
    
    outputArray = []
    
    # 起動パラメータ.receivedMessages分繰り返し
    for receivedMessage in event.get(RECEIVED_MESSAGES):
        
        # 作成元：OPCモデル
        opcMdl = opcModel.OpcModel(LOGGER, receivedMessage)
        deviceId = opcMdl.getAssetId()
        sensorId = opcMdl.getPropertyId()
        
        # 作成先：DGWモデル
        dgwMdl = dgwModel.DgwModel(LOGGER)
        
        dgwMdl.setDeviceId(deviceId)
        dgwMdl.setRequestTimeStamp(event[REQUEST_TIMESTAMP])
        
        
        # values分繰り返し
        for valueMdl in opcMdl.getValues():
    
            # タイムスタンプ・値取得
            timeStamp = valueMdl.getTimeStampStr()
            value = valueMdl.getValue()
            dgwMdl.addRecord(sensorId=opcMdl.getPropertyId()
                             , timeStamp=timeStamp
                             , value=value)
            
        outputArray.append(dgwMdl.getResult())
    
    resultDict = {
            CLIENT_NAME : event["clientName"]
            , RECEIVED_MESSAGES : outputArray
        }
    LOGGER.info("OPC-UAデータ加工処理終了 : %s", resultDict)
    return resultDict
