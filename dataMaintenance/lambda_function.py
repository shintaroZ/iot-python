# import base64
# import json
# import sys
# import os
# import pymysql
# import boto3
# import LOGGER
# import datetime
# import configparser
# import sqlite3
# import time
import json
import sys
import datetime
import configparser
import initCommon  # カスタムレイヤー
import rdsCommon  # カスタムレイヤー

# global
RDS = None
LOGGER = None
CONNECT = None
LOG_LEVEL = "INFO"
DB_HOST = "localhost"
DB_PORT = 3306
DB_USER = "hoge"
DB_PASSWORD = "hoge"
DB_NAME = "hoge"
DB_CONNECT_TIMEOUT = 3
RETRY_MAX_COUNT = 3
RETRY_INTERVAL = 500
PARTITION_FUTURE_RANGE = 5

# setter
def setRds(rds):
    global RDS
    RDS = rds
def setLogger(logger):
    global LOGGER
    LOGGER = logger
def setLogLevel(loglevel):
    global LOG_LEVEL
    LOG_LEVEL = loglevel
def setDbHost(dbHost):
    global DB_HOST
    DB_HOST = dbHost
def setDbPort(dbPort):
    global DB_PORT
    DB_PORT = int(dbPort)
def setDbUser(dbUser):
    global DB_USER
    DB_USER = dbUser
def setDbPassword(dbPassword):
    global DB_PASSWORD
    DB_PASSWORD = dbPassword
def setDbName(dbName):
    global DB_NAME
    DB_NAME = dbName
def setDbConnectTimeout(dbConnectTimeout):
    global DB_CONNECT_TIMEOUT
    DB_CONNECT_TIMEOUT = int(dbConnectTimeout)
def setPartitionFutureRange(partitionFutureRange):
    global PARTITION_FUTURE_RANGE
    PARTITION_FUTURE_RANGE = int(partitionFutureRange)

def setRetryMaxCount(retryMaxCount):
    global RETRY_MAX_COUNT
    RETRY_MAX_COUNT = int(retryMaxCount)


def setRetryInterval(retryInterval):
    global RETRY_INTERVAL
    RETRY_INTERVAL = int(retryInterval)


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
        setDbHost(config_ini['rds setting']['host'])
        setDbPort(config_ini['rds setting']['port'])
        setDbUser(config_ini['rds setting']['user'])
        setDbPassword(config_ini['rds setting']['password'])
        setDbName(config_ini['rds setting']['db'])
        setDbConnectTimeout(config_ini['rds setting']['connect_timeout'])
        setPartitionFutureRange(config_ini['rds setting']['partition_future_range'])
        setRetryMaxCount(config_ini['rds setting']['retryMaxcount'])
        setRetryInterval(config_ini['rds setting']['retryinterval'])
    except Exception as e:
        print ('設定ファイルの読み込みに失敗しました。')
        raise(e)

# --------------------------------------------------
# 保持期間マスタの取得
# --------------------------------------------------
def getMasterRetentionReriods():
    query = initCommon.getQuery("sql/m_retention_periods/findAll.sql")
    
    try:
        result = RDS.fetchall(query)
    except Exception  as e:
        LOGGER.error('保持期間マスタ取得時にエラーが発生しました。');
        raise(e)
    return result

# --------------------------------------------------
# 保持期間対象テーブルの取得
# --------------------------------------------------
def getMaintenancePartitions(paramTableName, paramPartitionColumnName):
    params = {
        "p_tableName": paramTableName
        ,"p_partitionColumnName": paramPartitionColumnName
    }
    query = initCommon.getQuery("sql/t_maintenance_tables/findByPartitionKey.sql")

    try:
        result = RDS.fetchall(query, params)
    except Exception  as e:
        LOGGER.error('保持期間対象テーブル取得時にエラーが発生しました。(%s / %s)' % (paramTableName, paramPartitionColumnName));
        raise(e)
    return result

# --------------------------------------------------
# パーティション情報の取得
# --------------------------------------------------
def getInformationSchemaPartitions(paramTableName, paramSchemaName):
    params = {
        "p_tableName": paramTableName
        ,"p_schemaName" : paramSchemaName
    }
    query = initCommon.getQuery("sql/information_schema/findByPartitionKey.sql")

    try:
        result = RDS.fetchall(query, params)
    except Exception  as e:
        LOGGER.error('パーティション情報取得時にエラーが発生しました。(%s)' % (paramTableName));
        raise(e)
    return result

# --------------------------------------------------
# 辞書型のListをPartitionKeyのListに変換
# --------------------------------------------------
def convertPartitionList(paramTable):

    # partitionKeyのList形式に変換
    resultList = []
    for m in paramTable:
        if ("partitionKey" in m) and (m["partitionKey"] is not None):
            resultList.append(m["partitionKey"])
    return resultList

# --------------------------------------------------
# 未来日のパーティション取得
# --------------------------------------------------
def getFuturePartitions(paramFutureRange):

    # 戻り値List
    resultList = []

    # システム日付の00:00:00取得
    nowdateTime = initCommon.getSysDateJst()
    nowdate = datetime.datetime(nowdateTime.year, nowdateTime.month, nowdateTime.day, 0, 0, 0)

    # range関数は要素0～
    for x in range(paramFutureRange + 1):
        tMap = {}

        # value要素の未来日は当日も含む
        futureDate = nowdate + datetime.timedelta(days=x)

        # key要素のパーティションキーは "p" + value値の前日
        keyDate = futureDate + datetime.timedelta(days=-1)
        keyStr = keyDate.strftime('p%Y%m%d')

        # map作成 および List追加
        tMap["partitionKey"] = keyStr
        tMap["partitionDate"] = futureDate
        resultList.append(tMap)

    return resultList;

# --------------------------------------------------
# ALTER TABLE パーティション句の構文作成
# --------------------------------------------------
def createSqlPartiton(paramPartitionTable):

    resultList = []

    baseStr = "PARTITION %s VALUES LESS THAN (TO_DAYS('%s'))"

    for i, e in enumerate(paramPartitionTable):
        dateFormat = e["partitionDate"].strftime("%Y/%m/%d %H:%M:%S")
        resultList.append(baseStr % (e["partitionKey"], dateFormat))

    # カンマ区切りで返却
    return ",\r\n".join(resultList)

# --------------------------------------------------
# パーティション情報の新規作成
# --------------------------------------------------
def createNewPartition(paramTableName, paramPartitionColumnName, paramPartitioStr):
    params = {
        "p_tableName": paramTableName
        ,"p_partitionColumnName": paramPartitionColumnName
        ,"p_partitionStr": paramPartitioStr
    }
    query = initCommon.getQuery("sql/t_maintenance_tables/createNewPartition.sql")

    try:
        RDS.execute(query, params, RETRY_MAX_COUNT, RETRY_INTERVAL)
    except Exception  as e:
        LOGGER.error('パーティション作成時にエラーが発生しました。(%s)' % (paramTableName));
        raise(e)

# --------------------------------------------------
# パーティション情報の追加
# --------------------------------------------------
def addPartition(paramTableName, paramPartitionStr):
    params = {
        "p_tableName": paramTableName
        ,"p_partitionStr": paramPartitionStr
    }
    query = initCommon.getQuery("sql/t_maintenance_tables/addPartition.sql")

    try:
        RDS.execute(query, params, RETRY_MAX_COUNT, RETRY_INTERVAL)
    except Exception  as e:
        LOGGER.error('パーティション追加時にエラーが発生しました。(%s)' % (paramTableName));
        raise(e)

# --------------------------------------------------
# パーティション削除
# --------------------------------------------------
def dropPartition(paramTableName, paramDropPartitionStr):
    params = {
        "p_tableName": paramTableName
        ,"p_dropPartitionStr": paramDropPartitionStr
    }
    query = initCommon.getQuery("sql/t_maintenance_tables/dropPartition.sql")

    try:
        RDS.execute(query, params, RETRY_MAX_COUNT, RETRY_INTERVAL)
    except Exception  as e:
        LOGGER.error('パーティション削除時にエラーが発生しました。(%s)' % (paramTableName));
        raise(e)

# --------------------------------------------------
# 2種類の辞書型Listを重複を除いて結合する
# --------------------------------------------------
def getMargeTable(paramBaseTable, paramAddTable):

    convTable = []
    convTable.extend(paramBaseTable)
    for m in paramAddTable:
        isAdd = True

        for n in paramBaseTable:
            if m["partitionKey"] == n["partitionKey"]:
                isAdd = False
                break
        if isAdd:
            convTable.append(m)
    return convTable

# --------------------------------------------------
# diffテーブル取得
# --------------------------------------------------
def getDiffTable(paramBaseTable, paramPartitionList):

    # 差分List作成
    diffList = []
    for x in paramBaseTable:
        pKey = x["partitionKey"]
        
        if len(paramPartitionList) == 0:
            diffList.append(x)
        elif (pKey not in paramPartitionList) and (max(paramPartitionList) < pKey):
            diffList.append(x)

    # キー項目で昇順にソート
    return sorted(diffList, key=lambda x:x['partitionKey'])

# --------------------------------------------------
# 保持期間外のpartitonKeyの取得
# --------------------------------------------------
def getDropPartition(paramBaseTable, paramPartitionList, paramRetentionRange):

    resultList = []

    # partitonKeyのListへ変換
    nowPartitionList = convertPartitionList(paramBaseTable)
    nowPartitionList.extend(paramPartitionList)

    # システム日付を基準に保持期間分過去にさかのぼったpartitionKeyを取得
    nowdateTime = initCommon.getSysDateJst()
    nowdate = datetime.datetime(nowdateTime.year, nowdateTime.month, nowdateTime.day, 0, 0, 0)
    oldDate = nowdate + datetime.timedelta(days=paramRetentionRange * -1)
    oldPartitionKey = oldDate.strftime('p%Y%m%d')

    for m in nowPartitionList:
        if m < oldPartitionKey:
            resultList.append(m)

    # カンマ区切りで返却
    return ",".join(resultList)
#####################
# main
#####################
def lambda_handler(event, context):

    # 初期処理
    initConfig(event["clientName"])
    setLogger(initCommon.getLogger(LOG_LEVEL))
    
    # # 初期処理
    # init(event['setting'])

    LOGGER.info('データメンテナンス開始 : %s' % event)

    # RDSコネクション作成
    rds = rdsCommon.rdsCommon(LOGGER, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, DB_CONNECT_TIMEOUT)
    setRds(rds)
    
    # 保持期間対象テーブルの取得
    result = rds.fetchall(initCommon.getQuery("sql/m_retention_periods/findAll.sql"))

    # 保持期間マスタのレコード分繰り返し
    for record in result:

        tableName = record['tableName']
        partitionColumnName = record['partitionColumnName']
        retentionDayUnit = record['retentionDayUnit']
        LOGGER.info('削除テーブル名:%s カラム名:%s 保持期間(日):%s' % (tableName, partitionColumnName, retentionDayUnit))

        # 1)保持期間対象テーブルの取得
        maintenanceDataTable = getMaintenancePartitions(tableName, partitionColumnName)
        
        # 2)パーティション情報の取得
        partitionList = convertPartitionList(getInformationSchemaPartitions(tableName, DB_NAME))

        # 3)未来日のパーティション取得
        futureTable = getFuturePartitions(PARTITION_FUTURE_RANGE)

        # 1と3結合
        convTable = getMargeTable(futureTable, maintenanceDataTable)

        # 2に存在しない差分tableを作成
        diffTable = getDiffTable(convTable, partitionList)

        # パーティション作成 / 追加
        if len(partitionList) == 0:
            LOGGER.info('パーティション新規作成 : %s' % diffTable)
            createNewPartition(tableName, partitionColumnName, createSqlPartiton(diffTable))
        elif len(diffTable) != 0:
            LOGGER.info('パーティション追加 : %s' % diffTable)
            addPartition(tableName, createSqlPartiton(diffTable))
        else:
            LOGGER.info('パーティション作成なし')

        # パーティション削除
        dropPartitionStr = getDropPartition(diffTable, partitionList, retentionDayUnit)
        if len(dropPartitionStr) != 0:
            LOGGER.info('パーティション削除 : %s' % dropPartitionStr)
            dropPartition(tableName, dropPartitionStr)
