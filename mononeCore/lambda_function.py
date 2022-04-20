import json
import configparser
import initCommon  # カスタムレイヤー
import boto3
import datetime
from datetime import datetime as dt

# global
LOGGER = None
LOG_LEVEL = "INFO"

LATEST_ORDER_ARN = None
AMQP_CONSUMER_ARN = None
AMQP_PRODUCER_ARN = None
S3_BACKUPSCORE_ARN = None
SOUND_CREATER_ARN = None
LIMIT_JUDGE_ARN = None
MAIL_SENDER_ARN = None

# パラメータ用定数
CLIENT_NAME = "clientName"
RECEIVED_MESSAGES = "receivedMessages"
SEND_MESSAGES = "sendMessages"
DEVICE_ID = "deviceId"
REQUEST_TIMESTAMP = "requestTimeStamp"
ROUTING_KEY = "routingKey"
MESSAGE_BODY = "messageBody"
RECORDS = "records"
IDTOKEN = "idToken"
DATA_COLLECTION_SEQ = "dataCollectionSeq"
RECEIVED_DATETIME = "receivedDatetime"
QUEUE = "queue"
RECORDS_COUNT = "recordsCount"

TENANT_ID = "tenantId"
STATUS = "status"
FILENAME = "filename"
TIMESTAMP = "timestamp"
CHUNK_NO = "chunk_no"
CHUNK_TOTAL = "chunk_total"
DATA = "data"
SCORE = "score"


# setter
def setLogger(logger):
    global LOGGER
    LOGGER = logger


def setLogLevel(loglevel):
    global LOG_LEVEL
    LOG_LEVEL = loglevel


def setLatestOrderArn(latestOrderArn):
    global LATEST_ORDER_ARN
    LATEST_ORDER_ARN = latestOrderArn

    
def setAmqpconsumerArn(amqpconsumerArn):
    global AMQP_CONSUMER_ARN
    AMQP_CONSUMER_ARN = amqpconsumerArn

    
def setAmqpproducerArn(amqpproducerArn):
    global AMQP_PRODUCER_ARN
    AMQP_PRODUCER_ARN = amqpproducerArn

    
def setS3backupscoreArn(S3backupscoreArn):
    global S3_BACKUPSCORE_ARN
    S3_BACKUPSCORE_ARN = S3backupscoreArn

    
def setSoundCreaterArn(soundCreaterArn):
    global SOUND_CREATER_ARN
    SOUND_CREATER_ARN = soundCreaterArn

    
def setLimitJudgeArn(limitJudgeArn):
    global LIMIT_JUDGE_ARN
    LIMIT_JUDGE_ARN = limitJudgeArn

    
def setMailsenderArn(mailsenderArn):
    global MAIL_SENDER_ARN
    MAIL_SENDER_ARN = mailsenderArn


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
        setLatestOrderArn(config_ini['lambda setting']['latestOrderArn'])
        setAmqpconsumerArn(config_ini['lambda setting']['amqpconsumerArn'])
        setAmqpproducerArn(config_ini['lambda setting']['amqpproducerArn'])
        setS3backupscoreArn(config_ini['lambda setting']['S3backupscoreArn'])
        setSoundCreaterArn(config_ini['lambda setting']['soundCreaterArn'])
        setLimitJudgeArn(config_ini['lambda setting']['limitJudgeArn'])
        setMailsenderArn(config_ini['lambda setting']['mailsenderArn'])
    except Exception as e:
        print ('設定ファイルの読み込みに失敗しました。')
        raise(e)


# --------------------------------------------------
# Lambda実行
# --------------------------------------------------
def invokeLambda(lambdaFuncName, lambdaArn, param):

    LOGGER.info("%s 開始 : %s" % (lambdaFuncName, param))
    lambdaClient = boto3.client("lambda")
    result = lambdaClient.invoke(
                FunctionName=lambdaArn,
                Payload=convDictToStr(param)
             )
    resultJson = json.loads(result["Payload"].read()) 
    LOGGER.info("%s 終了 : %s" % (lambdaFuncName, resultJson))
    return resultJson


# --------------------------------------------------
# Dict型をJson形式の文字列に変換して返却
# --------------------------------------------------
def convDictToStr(param):

    resultStr = ""
    if isinstance(param, dict):
        resultStr = json.dumps(param, ensure_ascii=False, default=initCommon.json_serial)
    else:
        resultStr = param
    return resultStr


# --------------------------------------------------
# 公開D作成機能用のパラメータ作成
# --------------------------------------------------
def createLatestOrderParam(clientName, param):

    resultDict = {}
    
    childDict = {}
    childDict[DEVICE_ID] = param.get(DEVICE_ID)
    childDict[REQUEST_TIMESTAMP] = param.get(REQUEST_TIMESTAMP)
    
    reArray = []
    for record in param.get(RECORDS):
        reDict = {}
        reDict[TENANT_ID] = record.get(TENANT_ID)
        reDict[SCORE] = record.get(SCORE)
        
        reArray.append(reDict)
    childDict[RECORDS] = reArray
    
    resultDict[CLIENT_NAME] = clientName
    resultDict[RECEIVED_MESSAGES] = [childDict]  # 配列
    
    return resultDict


# --------------------------------------------------
# 現在音ファイル作成用のパラメータ作成
# --------------------------------------------------
def createCurrentSoundCreaterParam(clientName, param):

    resultDict = {}
    
    childDict = {}
    childDict[DEVICE_ID] = param.get(DEVICE_ID)
    childDict[REQUEST_TIMESTAMP] = param.get(REQUEST_TIMESTAMP)
    
    # 現在音はrecord配下を再整形
    reArray = []
    for record in param.get(RECORDS):
        reDict = {}
        reDict[TENANT_ID] = record.get(TENANT_ID)
        reDict[STATUS] = 2  # 現在音は2
        reDict[FILENAME] = record.get(FILENAME)
        reDict[TIMESTAMP] = None
        reDict[CHUNK_NO] = record.get(CHUNK_NO)
        reDict[CHUNK_TOTAL] = record.get(CHUNK_TOTAL)
        reDict[DATA] = record.get(DATA)
        
        reArray.append(reDict)
    childDict[RECORDS] = reArray
    
    resultDict[CLIENT_NAME] = clientName
    resultDict[RECEIVED_MESSAGES] = [childDict]  # 配列
    
    return resultDict


# --------------------------------------------------
# 異常音ファイル作成用のパラメータ作成
# --------------------------------------------------
def createErrSoundCreaterParam(clientName, param):

    resultDict = {}
    
    childDict = {}
    childDict[DEVICE_ID] = param.get(DEVICE_ID)
    childDict[REQUEST_TIMESTAMP] = param.get(REQUEST_TIMESTAMP)
    childDict[RECORDS] = param.get(RECORDS)
    
    resultDict[CLIENT_NAME] = clientName
    resultDict[RECEIVED_MESSAGES] = [childDict]  # 配列
    
    return resultDict


# --------------------------------------------------
# MQ送信機能（異常音フォルダ削除）用のパラメータ作成
# --------------------------------------------------
def createErrDelAmqpProducerParam(clientName, param):

    resultDict = {}
    
    childDict = {}
    childDict[DEVICE_ID] = param.get(DEVICE_ID)
    childDict[ROUTING_KEY] = "Clean_Error_Marking"
    childDict[RECORDS] = {}
    
    resultDict[CLIENT_NAME] = clientName
    resultDict[SEND_MESSAGES] = [childDict]  # 配列
    
    return resultDict


#####################
# main
#####################
def lambda_handler(event, context):
    
    # 初期処理
    initConfig(event["clientName"])
    setLogger(initCommon.getLogger(LOG_LEVEL))
    LOGGER.info("Monone一連処理開始 : %s" % event)
    
    mqResultDict = None
    
    # ****** AMQP受信機能 ****** 
    mqResultDict = invokeLambda(lambdaFuncName="AMQP受信機能"
                                , lambdaArn=AMQP_CONSUMER_ARN
                                , param=event)
    
    # 判定
    if 0 < len(mqResultDict[RECEIVED_MESSAGES]):
        # ****** S3バックアップ機能 ****** 
        s3 = invokeLambda(lambdaFuncName="S3バックアップ機能"
                          , lambdaArn=S3_BACKUPSCORE_ARN
                          , param=mqResultDict)
        
        # loop
        for message in mqResultDict[RECEIVED_MESSAGES]:
            
            # 取得対象のキュー判定
            if message[QUEUE] == "Last_Score":
                lsParam = createLatestOrderParam(event["clientName"], message)
                # ****** 公開DB作成機能 ******
                scResult = invokeLambda(lambdaFuncName="公開DB作成機能"
                                        , lambdaArn=LATEST_ORDER_ARN
                                        , param=lsParam)
                # ****** 閾値判定機能 ******
                limitResult = invokeLambda(lambdaFuncName="閾値判定機能"
                                           , lambdaArn=LIMIT_JUDGE_ARN
                                           , param=scResult)
                # # ****** メール通知機能 ******
                # mailResult = invokeLambda(lambdaFuncName="メール通知機能"
                #                           , lambdaArn=MAIL_SENDER_ARN
                #                           , param=limitResult)
                
            elif message[QUEUE] == "New_Error":
                neParam = createErrSoundCreaterParam(event["clientName"], message)
                # ****** 異常音ファイル作成機能 ******
                neResult = invokeLambda(lambdaFuncName="異常音ファイル作成機能"
                                        , lambdaArn=SOUND_CREATER_ARN
                                        , param=neParam)
                
                # ****** MQ送信機能 ******
                scResult = invokeLambda(lambdaFuncName="異常音ファイル作成機能"
                                        , lambdaArn=SOUND_CREATER_ARN
                                        , param=neParam)
                
                
            elif message[QUEUE] == "Restart_AI":
                pass
            elif message[QUEUE] == "Restart_Edge_Failed":
                pass
            elif message[QUEUE] == "Transfer_Current_Sound":
                scParam = createCurrentSoundCreaterParam(event["clientName"], message)
                # ****** 現在音ファイル作成機能 ******
                scResult = invokeLambda(lambdaFuncName="現在音ファイル作成機能"
                                        , lambdaArn=SOUND_CREATER_ARN
                                        , param=scParam)
                 
            print (message)
     
