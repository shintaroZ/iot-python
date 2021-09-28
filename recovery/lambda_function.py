import boto3
import sys
import configparser
import datetime
import time
import json
import initCommon  # カスタムレイヤー
import rdsCommon  # カスタムレイヤー


# athena setting
ATENA_DATABASE = 'hoge'
ATENA_TABLE = 'hoge'
S3_OUTPUT = 's3://hoge'
RETRY_COUNT = 10
RETRY_INTERVAL = 200
LATERST_ORDER_ARN = "hoge"

# invoke時の引数最大サイズ(byte)、正確には6291456(byte)だが、余裕を持たせる
INVOKE_MAX_BODY_SIZE = 6200000

# logger setting
LOG_LEVEL = "INFO"
LOOGER = None
CLIENT_NAME = ""

# setter
# athena setting
def setAtenaDatabase(atenaDatabase):
    global ATENA_DATABASE
    ATENA_DATABASE = atenaDatabase
def setAtenaTable(atenaTable):
    global ATENA_TABLE
    ATENA_TABLE = atenaTable
def setS3Output(s3Output):
    global S3_OUTPUT
    S3_OUTPUT = s3Output
def setRetryCount(retryCount):
    global RETRY_COUNT
    RETRY_COUNT = int(retryCount)
def setStartDateTime(startDateTime):
    global START_DATE_TIME
    START_DATE_TIME = startDateTime
def setEndDateTime(endDateTime):
    global END_DATE_TIME
    END_DATE_TIME = endDateTime
def setRetryInterval(retryInterval):
    global RETRY_INTERVAL
    RETRY_INTERVAL = int(retryInterval)

# logger setting
def setLogLevel(loglevel):
    global LOG_LEVEL
    LOG_LEVEL = loglevel
def setLogger(logger):
    global LOGGER
    LOGGER = logger
def setClientName(clientName):
    global CLIENT_NAME
    CLIENT_NAME= clientName


def setLatestOrderArn(latestOrderArn):
    global LATERST_ORDER_ARN
    LATERST_ORDER_ARN = latestOrderArn
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

        # athena setting
        setAtenaDatabase(config_ini['athena setting']['database'])
        setAtenaTable(config_ini['athena setting']['table'])
        setS3Output(config_ini['athena setting']['output'])
        setRetryCount(config_ini['athena setting']['retryCount'])
        setRetryInterval(config_ini['athena setting']['retryInterval'])
        setLatestOrderArn(config_ini['athena setting']['latestOrderArn'])

        # logger setting
        setLogLevel(config_ini['logger setting']['loglevel'])
        

    except Exception as e:
        print ('設定ファイルの読み込みに失敗しました。')
        raise(e)

# --------------------------------------------------
# 抽出条件の作成
# --------------------------------------------------
def createWhereParam(event):

    # 必須判定付の起動パラメータ取得
    startDateTime = "1900-01-01 00:00:00" if "startDateTime" not in event else event["startDateTime"]
    endDateTime = "9999-12-31 23:59:59" if "endDateTime" not in event else event["endDateTime"]
    deviceId = "" if "deviceId" not in event else event["deviceId"]
    sensorId = "" if "sensorId" not in event else event["sensorId"]

    # 日時文字列から日付部分を抽出
    startDt = datetime.datetime.strptime(startDateTime, '%Y-%m-%d %H:%M:%S')
    endDt = datetime.datetime.strptime(endDateTime, '%Y-%m-%d %H:%M:%S')
    startDateInt = int(startDt.strftime('%Y%m%d'))
    endDateInt = int(endDt.strftime('%Y%m%d'))

    whereParamArray = []
    whereParamArray.append("createdt between %d and %d" % (startDateInt, endDateInt))
    whereParamArray.append("rMessages.requesttimestamp between CAST('%s' as timestamp) and CAST('%s' as timestamp)" % (startDateTime, endDateTime))
    if deviceId != "":
        whereParamArray.append("rMessages.deviceid = '%s'" % deviceId)
    if sensorId != "":
        whereParamArray.append("records.sensorid = '%s'" % sensorId)

    # and区切りの文字列返却
    return " AND ".join(whereParamArray)

# --------------------------------------------------
# S3からデータの取得
# --------------------------------------------------
def get_recovery_data(event):

    # 動的な抽出条件
    createWhereParam(event)

    # created query
    params = {
        "databaseName": ATENA_DATABASE
        ,"tableName": ATENA_TABLE
        ,"whereParam": createWhereParam(event)
    }
    queryString = initCommon.getQuery("sql/athena/find.sql") % params
    LOGGER.debug(queryString)
    # athena client
    client = boto3.client('athena')

    # Execution
    response = client.start_query_execution(
        QueryString=queryString,
        QueryExecutionContext={
            'Database': ATENA_DATABASE
        },
        ResultConfiguration={
            'OutputLocation': 's3://%s/%s' % (CLIENT_NAME, S3_OUTPUT),
        }
    )

    # get query execution id
    query_execution_id = response['QueryExecutionId']

    # get execution status
    LOGGER.info("==== get_query_execution start ====")
    for i in range(1, 1 + RETRY_COUNT):

        # get query execution
        query_status = client.get_query_execution(QueryExecutionId=query_execution_id)
        query_execution_status = query_status['QueryExecution']['Status']['State']
        LOGGER.info("get_query_execution status:[%s]" % query_execution_status)

        if query_execution_status == 'SUCCEEDED':
            break

        if query_execution_status == 'FAILED':
            raise Exception("STATUS:" + query_execution_status)

        else:
            time.sleep(RETRY_INTERVAL / 1000) # ミリ秒単位
    else:
        client.stop_query_execution(QueryExecutionId=query_execution_id)
        LOGGER.error('リトライ回数を超過しました。処理を終了します。')
        sys.exit()

    # get query results
    LOGGER.info("==== get_query_results start ====")

    resultSetRows = []
    result = client.get_query_results(QueryExecutionId=query_execution_id)

    # get_query_resultsの戻り値は最大1000件までしか取得出来ないので、APIを再起呼び出し
    try:
        while(True):
            resultSetRows.extend(result["ResultSet"]["Rows"])
            LOGGER.info("get_query_results RowCount:[%d]" % len(resultSetRows))
            result = client.get_query_results(QueryExecutionId=query_execution_id, NextToken=result["NextToken"])
    except Exception as ex:
        pass
    # get data
    list = []
    for row in resultSetRows:
        map = {
             "deviceId" : row["Data"][0]["VarCharValue"]
            ,"requestTimeStamp" : row["Data"][1]["VarCharValue"]
            ,"sensorId" : row["Data"][2]["VarCharValue"]
            ,"timeStamp" : row["Data"][3]["VarCharValue"]
            ,"value" : row["Data"][4]["VarCharValue"]
        }
        list.append(map)

    #ヘッダーの削除
    list.pop(0)

    if len(list) == 0:
        raise Exception ("対象のバックアップファイルが存在しませんでした。")
 
    return list

# --------------------------------------------------
# 戻り値の作成
# --------------------------------------------------
def getReceivedMessages(recoveryDataList):

    reEvent = None
    subTable = {}
    tempTable = {}
    recordsArray = []
    reReceivedMessages = []
    reEventTable = {}
    bkDeviceId = None
    recordsSize = 0

    # デバイスID,受信日時昇順ソート
    recoveryDataList.sort(key=lambda x:(x["deviceId"],x["requestTimeStamp"]))

    # 受信メッセージ単位
    for i in range(len(recoveryDataList)):
        receivedMessages = recoveryDataList[i]

        # デバイスIDが変わった or recordsサイズが上限を超えた場合、レコードを再形成
        if ((0 < i and receivedMessages["deviceId"] != bkDeviceId) or INVOKE_MAX_BODY_SIZE < recordsSize):
            reReceivedMessages.append(tempTable)

            # 一時テーブル初期化
            tempTable = {}
            recordsSize = 0
        
        # 各要素を一時保存
        if "deviceId" not in tempTable:
            tempTable["deviceId"] = receivedMessages["deviceId"]
        if "requestTimeStamp" not in tempTable:
            tempTable["requestTimeStamp"] = receivedMessages["requestTimeStamp"]

        # records要素は追記
        recordsArray = []
        recordsArray.append({
                "sensorId" : receivedMessages["sensorId"]
                , "timeStamp" : receivedMessages["timeStamp"]
                , "value" : receivedMessages["value"]
            })
        if "records" in tempTable:
            tempTable["records"].extend(recordsArray)
        else:
            tempTable["records"] = recordsArray
        
        # Size計算
        recordsSize += len(str(recordsArray))

        # 前回デバイスIDを待避
        bkDeviceId = receivedMessages["deviceId"]

    # 最終ループ用
    if tempTable:
        reReceivedMessages.append(tempTable)

    return reReceivedMessages

# --------------------------------------------------
# 公開DB作成機能の呼び出し
# --------------------------------------------------
def latestOrderInvokeConvert(recoveryDataList):
    
    # receivedMessagesの整形
    reReceivedMessages = getReceivedMessages(recoveryDataList)
    
    for rMessage in reReceivedMessages:
        
        # 親要素の整形
        reEventTable = {}
        reEventTable["clientName"] = CLIENT_NAME
        reEventTable["receveryFlg"] = 1
        reEventTable["receivedMessages"] = [rMessage]
        
        # JSON形式へパース
        initCommon.setJsonDateTimeFormat("%Y-%m-%d %H:%M:%S.%f")
        strJsonResult = json.dumps(reEventTable, ensure_ascii=False, default=initCommon.json_serial)
    
        # 公開DB作成機能の呼び出し
        LOGGER.info("公開db作成機能-開始 [deviceId:%s]" % rMessage["deviceId"])
        lambdaClient = boto3.client("lambda")
        lambdaClient.invoke(
                FunctionName=LATERST_ORDER_ARN,
                Payload=strJsonResult
             )
        LOGGER.info("公開db作成機能-終了 [deviceId:%s]" % rMessage["deviceId"])
        
#####################
# main
#####################
def lambda_handler(event, context):

    # 初期処理
    setClientName(event["clientName"])
    initConfig(event["clientName"])
    setLogger(initCommon.getLogger(LOG_LEVEL))

    LOGGER.info('リカバリー機能開始 : %s' % event)

    # リカバリーデータの取得
    recoveryDataList = get_recovery_data(event)
    LOGGER.info("リカバリーデータの取得終了(%d件)" % len(recoveryDataList))

    # 戻り値経由だとサイズ制限に引っかかるので、本lambda内でinvoke呼び出し
    latestOrderInvokeConvert(recoveryDataList)