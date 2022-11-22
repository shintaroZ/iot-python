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
LIMIT_JUDGE_ARN = None
MAIL_SENDER_ARN = None
OPC_FORMAT_CONVERTER_ARN = None

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

def setLimitJudgeArn(limitJudgeArn):
    global LIMIT_JUDGE_ARN
    LIMIT_JUDGE_ARN = limitJudgeArn

def setMailsenderArn(mailsenderArn):
    global MAIL_SENDER_ARN
    MAIL_SENDER_ARN = mailsenderArn

def setOpcuaFormatConverterArn(opcuaFormatConverterArn):
    global OPC_FORMAT_CONVERTER_ARN
    OPC_FORMAT_CONVERTER_ARN = opcuaFormatConverterArn

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
        setLimitJudgeArn(config_ini['lambda setting']['limitJudgeArn'])
        setMailsenderArn(config_ini['lambda setting']['mailsenderArn'])
        setOpcuaFormatConverterArn(config_ini['lambda setting']['opcuaFormatConverterArn'])
    except Exception as e:
        print ('設定ファイルの読み込みに失敗しました。')
        raise(e)


# --------------------------------------------------
# Lambda実行
# --------------------------------------------------
def invokeLambda(lambdaFuncName, lambdaArn, param):

    LOGGER.info("%s 開始 : %s" % (lambdaFuncName, param))
    resultJson = initCommon.invokeLambda(lambdaArn, param)
    if isException(resultJson):
        raise Exception (resultJson)
    else:
        LOGGER.info("%s 終了 : %s" % (lambdaFuncName, resultJson))
    return resultJson


# --------------------------------------------------
# Lambdaの戻り値からエラー判定
# --------------------------------------------------
def isException(resultDict):

    isError  =False
    if (type(resultDict) == dict and "errorType" in resultDict):
        isError = True
        LOGGER.error(resultDict)
    return isError


#####################
# main
#####################
def lambda_handler(event, context):
    
    # 初期処理
    initConfig(event["clientName"])
    setLogger(initCommon.getLogger(LOG_LEVEL))
    LOGGER.info("OPC-UA一連処理開始 : %s" % event)
    
    opcResultDict = None
    
    # ****** OPC-UAデータ変換処理 ****** 
    opcResultDict = invokeLambda(lambdaFuncName="OPC-UAデータ変換処理"
                                , lambdaArn=OPC_FORMAT_CONVERTER_ARN
                                , param=event)
    
    # ****** 公開DB作成機能 ******
    latestResultDict = invokeLambda(lambdaFuncName="公開DB作成機能"
                            , lambdaArn=LATEST_ORDER_ARN
                            , param=opcResultDict)
    
    # ****** 閾値判定機能 ******
    limitResult = invokeLambda(lambdaFuncName="閾値判定機能"
                               , lambdaArn=LIMIT_JUDGE_ARN
                               , param=latestResultDict)
    
    # ****** メール通知機能 ******
    mailResult = invokeLambda(lambdaFuncName="メール通知機能"
                              , lambdaArn=MAIL_SENDER_ARN
                              , param=limitResult)
    
     
