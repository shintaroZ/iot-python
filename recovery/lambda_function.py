import time
import boto3
import os
import sys
import configparser
import datetime
import logging


# athena setting
ATENA_DATABASE = 'hoge'
ATENA_TABLE = 'hoge'
S3_OUTPUT = 's3://hoge'
RETRY_COUNT = 10

# logger setting
LOG_LEVEL = "INFO"


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

# logger setting
def setLogLevel(loglevel):
    global LOG_LEVEL
    LOG_LEVEL = loglevel

# --------------------------------------------------
# ロガー初期設定
# --------------------------------------------------
def initLogger():
    logger = logging.getLogger()
    
    # 2行出力される対策のため、既存のhandlerを削除
    for h in logger.handlers:
      logger.removeHandler(h)
    
    # handlerの再定義
    handler = logging.StreamHandler(sys.stdout)
    
    # 出力フォーマット
    strFormatter = '[%(levelname)s] %(asctime)s %(funcName)s %(message)s'
    formatter = logging.Formatter(strFormatter)
    formatter.converter = customTime
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    # ログレベルの設定
    logger.setLevel(LOG_LEVEL)
    
    logging.debug("logsetting completed")


# --------------------------------------------------
# 設定ファイル読み込み
# --------------------------------------------------
def initConfig(filePath):
    try:
        config_ini = configparser.ConfigParser()
        config_ini.read(filePath, encoding='utf-8')
        
        # athena setting
        setAtenaDatabase(config_ini['athena setting']['database'])
        setAtenaTable(config_ini['athena setting']['table'])
        setS3Output(config_ini['athena setting']['output'])
        setRetryCount(config_ini['athena setting']['retryCount'])
        setStartDateTime(config_ini['athena setting']['startDateTime'])
        setEndDateTime(config_ini['athena setting']['endDateTime'])
        
        # logger setting
        setLogLevel(config_ini['logger setting']['loglevel'])
        
    except Exception as e:
        logging.error('設定ファイルの読み込みに失敗しました。')
        raise(e)


# --------------------------------------------------
# S3からデータの取得
# --------------------------------------------------
def get_recovery_data(startDateTime, endDateTime):
    # created query
    params = {
        "databaseName": ATENA_DATABASE
        ,"tableName": ATENA_TABLE
        ,"startDateTime": startDateTime
        ,"endDateTime": endDateTime
    }
    query = get_query("sql/athena/find.sql")


    # athena client
    client = boto3.client('athena')

    # Execution
    response = client.start_query_execution(
        QueryString=query % params,
        QueryExecutionContext={
            'Database': ATENA_DATABASE
        },
        ResultConfiguration={
            'OutputLocation': S3_OUTPUT,
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
            logging.debug("STATUS:" + query_execution_status)
            break

        if query_execution_status == 'FAILED':
            logging.debug("STATUS:" + query_execution_status)
            raise Exception("STATUS:" + query_execution_status)

        else:
            logging.debug("STATUS:" + query_execution_status)
            time.sleep(i)
    else:
        client.stop_query_execution(QueryExecutionId=query_execution_id)
        logging.error('リトライ回数を超過しました。処理を終了します。')
        sys.exit()

    # get query results
    result = client.get_query_results(QueryExecutionId=query_execution_id)

    # get data
    list = []
    for row in result["ResultSet"]["Rows"]:
        map = {
             "setting" : row["Data"][0]["VarCharValue"]
            ,"deviceId" : row["Data"][1]["VarCharValue"]
            ,"requestTimeStamp" : row["Data"][2]["VarCharValue"]
            ,"sensorId" : row["Data"][3]["VarCharValue"]
            ,"timeStamp" : row["Data"][4]["VarCharValue"]
            ,"value" : row["Data"][5]["VarCharValue"]
        }
        list.append(map)

    #ヘッダーの削除
    list.pop(0)
    
    if len(list) == 0:
        logging.error('対象のバックアップファイルが存在しませんでした。')
        sys.exit()
    
    return list

# --------------------------------------------------
# データの成形
# --------------------------------------------------
def data_molding(recoveryDataList):
    resultList = []
    deviceMap = {}
    records = []
    checkSetting = ""
    checkDeviceId = ""
    checkRequestTimeStamp = ""
    sortData = sorted(recoveryDataList, key=lambda x:(x["deviceId"],x["requestTimeStamp"]))
    
    for row in sortData:
        setting = row["setting"]
        deviceId = row["deviceId"]
        requestTimeStamp = row["requestTimeStamp"]

        #新しいマップの作成
        if deviceId != checkDeviceId or requestTimeStamp != checkRequestTimeStamp or setting != checkSetting:
            deviceMap = {}
            deviceMap["receveryFlg"] = 1
            deviceMap["setting"] = setting
            deviceMap["deviceId"] = deviceId
            deviceMap["requestTimeStamp"] = requestTimeStamp
            records = []
            deviceMap["records"] = records
            checkSetting = setting
            checkDeviceId = deviceId
            checkRequestTimeStamp = requestTimeStamp

        #レコードの追加
        record = {
            "sensorId" : row["sensorId"]
            ,"timeStamp" : row["timeStamp"]
            ,"value" : float(row["value"])
        }
        deviceMap["records"].append(record)

        #JSONの作成
        if len(resultList) != 0:
            if resultList[-1]["deviceId"] == deviceMap["deviceId"] and resultList[-1]["requestTimeStamp"] == deviceMap["requestTimeStamp"] and resultList[-1]["setting"] == deviceMap["setting"]:
                resultList[-1] = deviceMap
            else:
                resultList.append(deviceMap)
        else:
            resultList.append(deviceMap)

    return resultList

# --------------------------------------------------
# クエリファイル読み込み
# --------------------------------------------------
def get_query(query_file_path, *args):
    with open(query_file_path, 'r', encoding='utf-8') as f:
        query = f.read()
    return query

# --------------------------------------------------
# JSTのtime.struct_timeを返却する
# --------------------------------------------------
def customTime(*args):
    utc = datetime.datetime.now(datetime.timezone.utc)
    jst = datetime.timezone(datetime.timedelta(hours=+9))
    return utc.astimezone(jst).timetuple()

#####################
# main
#####################
def lambda_handler(event, context):
    # ロガー設定
    initLogger()
    # 設定ファイルの読み込み
    initConfig(event["configPath"])

    #リカバリーデータの取得
    recoveryDataList = get_recovery_data(event["startDateTime"], event["endDateTime"])
    
    #リカバリーデータの成形
    resultData = data_molding(recoveryDataList)
    


    return resultData
