import json
import configparser
import initCommon # カスタムレイヤー
import rdsCommon # カスタムレイヤー
import mqCommon # カスタムレイヤー
import ssl
import re # 正規表現

# global
LOGGER = None
LOG_LEVEL = "INFO"
DB_CONNECT_TIMEOUT = 3
MQ_HOST = "localhost"
MQ_PORT = 3306
MQ_USER = "hoge"
MQ_PASSWORD = "hoge"

# 正規表現
PARAM_FILENAME = "GaussianMixture_ID[0-9]{2}.pkl|threshold_ID[0-9]{2}.json"

# setter
def setLogger(logger):
    global LOGGER
    LOGGER = logger
def setLogLevel(loglevel):
    global LOG_LEVEL
    LOG_LEVEL = loglevel
def setMqHost(mqHost):
    global MQ_HOST
    MQ_HOST = mqHost
def setMqPort(mqPort):
    global MQ_PORT
    MQ_PORT = int(mqPort)
def setMqUser(mqUser):
    global MQ_USER
    MQ_USER = mqUser
def setMqPassword(mqPassword):
    global MQ_PASSWORD
    MQ_PASSWORD = mqPassword
    
# --------------------------------------------------
# 起動パラメータチェック
# --------------------------------------------------
def isArgument(event):

    try:
        # TODO idTokenは後で復活
        # # トークン取得
        # token = event["idToken"]
        #
        # # グループ名
        # try:
        #     groupList = initCommon.getPayLoadKey(token, "cognito:groups")
        #
        #     # 顧客名がグループ名に含まれること
        #     if (event["clientName"] not in groupList):
        #         raise Exception("clientNameがグループに属していません。clientName:%s groupName:%s" % (event["clientName"], ",".join(groupList) ))

        for sendMsg in event["sendMessages"]:
            
            print (getSendMessageBody(sendMsg))
            msgBody = sendMsg["messageBody"]
            fileNameArray = []
            fileNameArrayReg = []
            
            # パラメータファイル入替の場合、起動パラメータをチェック
            if sendMsg["routingKey"] == "Replace_Edge_Setting" and  msgBody.get("records") is not None:
                for record in msgBody["records"]:
                    # 正規表現の完全一致
                    if re.fullmatch(PARAM_FILENAME, record["fileName"]) is None:
                        raise Exception("リクエスト内容に誤りがあります。パラメータファイル名が不正です。fileName:%s" % record["fileName"])
                    
                    # 数値部分を除いたファイル名を保持
                    fileNameArray.append(record["fileName"])
                    fileNameArrayReg.append(re.sub(r'[0-9]', "", record["fileName"]))
                
                if len(fileNameArray) != len(set(fileNameArrayReg)):
                    raise Exception("リクエスト内容に誤りがあります。同じ種類のパラメータファイルは指定出来ません。%s" % fileNameArray)
                    
    except Exception as ex:
        raise Exception("Authentication Error. [%s]" %  ex)
        
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

        # setMqHost(config_ini['mq setting']['host'])
        setMqHost(config_ini['mq setting']['host'])
        setMqPort(config_ini['mq setting']['port'])
        setMqUser(config_ini['mq setting']['user'])
        setMqPassword(config_ini['mq setting']['password'])
        
    except Exception as e:
        print ('設定ファイルの読み込みに失敗しました。')
        raise(e)
    
# --------------------------------------------------
# 起動パラメータからメッセージボディ部を取得
# 空の場合は空のJson形式の文字列を返却
# --------------------------------------------------
def getSendMessageBody(sendMsg):
    resultStr = ""
    resultArray = []
    if sendMsg["messageBody"].get("records") is not None:
        for record in sendMsg["messageBody"]["records"]:
            resultArray.append(record)
        resultStr = str(resultArray)
    else:
        resultStr = "{}"
        
    return resultStr
    
#####################
# main
#####################
def lambda_handler(event, context):
    
    # 初期処理
    initConfig(event["clientName"])
    setLogger(initCommon.getLogger(LOG_LEVEL))
    LOGGER.info("MQ送信機能開始")
    
    # 起動パラメータチェック
    isArgument(event)
    
    # MQコネクション生成
    mqConnection = mqCommon.mqCommon(LOGGER, MQ_HOST, MQ_PORT, MQ_USER, MQ_PASSWORD)
    
    # MQへPublish
    try:
        for sendMsg in event["sendMessages"]:
            exchangeName = mqConnection.getExchangesDwName() % sendMsg["deviceId"]
            routingKey = sendMsg["routingKey"]
            bodyMessage = getSendMessageBody(sendMsg)
            
            LOGGER.info("Publish to exchangeName[%s] routingKey[%s] bodyMessage[%s]" % (exchangeName, routingKey, bodyMessage))
            mqConnection.publishExchange(exchangeName, routingKey, bodyMessage)

    except Exception as ex:
        raise Exception("Publish Error. [%s]" %  ex)
    
    # クローズ
    del mqConnection