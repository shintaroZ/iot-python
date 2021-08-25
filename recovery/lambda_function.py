import boto3
import sys
import configparser
import datetime
import time
import json
import initCommon  # カスタムレイヤー
import rdsCommon  # カスタムレイヤー
# from _tracemalloc import start


# athena setting
ATENA_DATABASE = 'hoge'
ATENA_TABLE = 'hoge'
S3_OUTPUT = 's3://hoge'
RETRY_COUNT = 10
RETRY_INTERVAL = 200

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
        
        # logger setting
        setLogLevel(config_ini['logger setting']['loglevel'])
        
    except Exception as e:
        LOGGER.error('設定ファイルの読み込みに失敗しました。')
        raise(e)


# --------------------------------------------------
# S3からデータの取得
# --------------------------------------------------
def get_recovery_data(startDateTime, endDateTime):
    
    # 日時文字列から日付部分を抽出
    startDt = datetime.datetime.strptime(startDateTime, '%Y-%m-%d %H:%M:%S')
    endDt = datetime.datetime.strptime(endDateTime, '%Y-%m-%d %H:%M:%S')
    startDateInt = int(startDt.strftime('%Y%m%d'))
    endDateInt = int(endDt.strftime('%Y%m%d'))
    
    # created query
    params = {
        "databaseName": ATENA_DATABASE
        ,"tableName": ATENA_TABLE
        ,"startDateTime": startDateTime
        ,"endDateTime": endDateTime
        ,"startDate" : startDateInt
        ,"endDate" : endDateInt
    }
    queryString = initCommon.getQuery("sql/athena/find.sql") % params

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
    for i in range(1, 1 + RETRY_COUNT):
        
        # get query execution
        query_status = client.get_query_execution(QueryExecutionId=query_execution_id)
        query_execution_status = query_status['QueryExecution']['Status']['State']

        if query_execution_status == 'SUCCEEDED':
            LOGGER.debug("STATUS:" + query_execution_status)
            break

        if query_execution_status == 'FAILED':
            LOGGER.debug("STATUS:" + query_execution_status)
            raise Exception("STATUS:" + query_execution_status)

        else:
            LOGGER.debug("STATUS:" + query_execution_status)
            time.sleep(RETRY_INTERVAL / 1000) # ミリ秒単位
    else:
        client.stop_query_execution(QueryExecutionId=query_execution_id)
        LOGGER.error('リトライ回数を超過しました。処理を終了します。')
        sys.exit()

    # get query results
    result = client.get_query_results(QueryExecutionId=query_execution_id)

    # get data
    list = []
    for row in result["ResultSet"]["Rows"]:
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
        LOGGER.error('対象のバックアップファイルが存在しませんでした。')
        sys.exit()
    
    return list

# --------------------------------------------------
# 戻り値の作成
# --------------------------------------------------
def data_molding(recoveryDataList):

    reEvent = None
    subTable = {}
    tempTable = {}
    recordsArray = []
    reReceivedMessages = []
    reEventTable = {}
    bkDeviceId = None
    
    # デバイスID,受信日時昇順ソート
    recoveryDataList.sort(key=lambda x:(x["deviceId"],x["requestTimeStamp"]))

    # 受信メッセージ単位
    for i in range(len(recoveryDataList)):
        receivedMessages = recoveryDataList[i]

        # デバイスIDが変わった場合、レコードを再形成
        if ((0 < i and receivedMessages["deviceId"] != bkDeviceId)):
            reReceivedMessages.append(tempTable)

            # 一時テーブル初期化
            tempTable = {}

        # 各要素を一時保存
        if "receveryFlg" not in tempTable:
            tempTable["receveryFlg"] = 1
        if "deviceId" not in tempTable:
            tempTable["deviceId"] = receivedMessages["deviceId"]
        if "requestTimeStamp" not in tempTable:
            tempTable["requestTimeStamp"] = receivedMessages["requestTimeStamp"]
            
        # records要素は追記
        recordsArray = []
        recordsArray.append({
                "sensorId" : receivedMessages["sensorId"]
                , "timeStamp" : receivedMessages["timeStamp"]
                , "value" : int(float(receivedMessages["value"]))
            })
        if "records" in tempTable:
            tempTable["records"].extend(recordsArray)
        else:
            tempTable["records"] = recordsArray

        # 前回デバイスIDを待避
        bkDeviceId = receivedMessages["deviceId"]

    # 最終ループ用
    if tempTable:
        reReceivedMessages.append(tempTable)

    # 親要素の整形
    reEventTable["clientName"] = CLIENT_NAME
    reEventTable["receivedMessages"] = reReceivedMessages

    # JSON形式へパース
    initCommon.setJsonDateTimeFormat("%Y-%m-%d %H:%M:%S.%f")
    return json.dumps(reEventTable, ensure_ascii=False, default=initCommon.json_serial)

#####################
# main
#####################
def lambda_handler(event, context):
    
    # 初期処理
    setClientName(event["clientName"])
    initConfig(event["clientName"])
    setLogger(initCommon.getLogger(LOG_LEVEL))

    LOGGER.info('リカバリー機能開始 : %s' % event)
    #
    # # ロガー設定
    # initLogger()
    # # 設定ファイルの読み込み
    # initConfig(event["configPath"])

    # リカバリーデータの取得
    recoveryDataList = get_recovery_data(event["startDateTime"], event["endDateTime"])
    LOGGER.info("リカバリーデータの取得終了.len:[%d]" % len(recoveryDataList))
    
    # リカバリーデータの成形
    resultData = data_molding(recoveryDataList)
    LOGGER.info("戻り値整形終了")
    


    return resultData
