import json
import configparser
import initCommon # カスタムレイヤー
import rdsCommon # カスタムレイヤー
import boto3
import datetime
import base64

# global
LOGGER = None
LOG_LEVEL = "INFO"
DB_HOST = "localhost"
DB_PORT = 3306
DB_USER = "hoge"
DB_PASSWORD = "hoge"
DB_NAME = "hoge"
DB_CONNECT_TIMEOUT = 3
BUCKET_NAME = "hoge"
CLIENT_NAME = "hoge"

# パラメータ用定数
CLIENT_NAME = "clientName"
RECEIVED_MESSAGES = "receivedMessages"
DEVICE_ID = "deviceId"
REQUEST_TIMESTAMP = "requestTimeStamp"
RECORDS = "records"
TENANT_ID = "tenantId"
STATUS = "status"
FILENAME = "filename"
TIMESTAMP = "timestamp"
CHUNK_NO ="chunk_no"
CHUNK_TOTAL = "chunk_total"
DATA = "data"

DATA_COLLECTION_SEQ = "dataCollectionSeq"
CREATED_DATETIME = "created_datetime"
FILE_TYPE = "fileType"
FILE_NAME = "fileName"
CREATED_AT = "created_at"

# setter
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
        
    except Exception as e:
        print ('設定ファイルの読み込みに失敗しました。')
        raise(e)
        
# --------------------------------------------------
# パラメータ再構成
# チャンクが分割している場合にdata部を結合する。
# --------------------------------------------------
def getParamReConv(records):
    
    resultTable = []
    newDict = {}
    newFlg = False
    key = ""
    bk_key = ""
    
    # 設備ID,ファイル名,チャンクトータル,チャンクNoで昇順ソート
    sortedRecords = sorted(records,key=lambda x:(x['tenantId'], x['filename'], x['chunk_total'], x['chunk_no']))
    for i in range(len(sortedRecords)):
        record = sortedRecords[i]
        
        # 同ファイルのチャンク判定
        # チャンクNo +1 とチャンクトータルが一致する場合に辞書を再作成する。
        if ((int(record["chunk_no"]) + 1 == int(record["chunk_total"]))):
            LOGGER.debug("新規")
            newFlg = True
        else:
            LOGGER.debug("継続")
            newFlg = False
        
        if ("data" in newDict):
            newDict["data"] = newDict["data"] + record["data"]
        else:
            newDict["data"] = record["data"]
            
        if newFlg:
            newDict["tenantId"] = record["tenantId"]
            newDict["status"] = record["status"]
            newDict["filename"] = record["filename"]
            newDict["timestamp"] = record["timestamp"]
            newDict["chunk_no"] = record["chunk_no"]
            newDict["chunk_total"] = record["chunk_total"]
            resultTable.append(newDict)
            newDict ={}
        
    return resultTable
     
# --------------------------------------------------
# エポックミリ秒をdatetimeへ変換
# --------------------------------------------------
def convertEpokMillSecToDateTime(timestamp):
    return datetime.datetime.fromtimestamp(timestamp / 1000, datetime.timezone(datetime.timedelta(hours=9)) )
     

#####################
# main
#####################
def lambda_handler(event, context):

    # 初期処理
    initConfig(event[CLIENT_NAME])
    setLogger(initCommon.getLogger(LOG_LEVEL))
    # setLogger(initCommon.getLogger("DEBUG"))
    
    LOGGER.info("音ファイル作成機能開始")
    
    # RDSコネクション作成
    rds = rdsCommon.rdsCommon(LOGGER, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, DB_CONNECT_TIMEOUT)
    
    # S3リソースのインスタンス作成
    s3 = boto3.resource('s3')
    
    # 起動パラメータ.受信メッセージ一覧分繰り返し
    for messageDict in event[RECEIVED_MESSAGES]:
        
        # レコード再形成
        convRecords = getParamReConv(messageDict[RECORDS])
    
        # レコード分繰り返し
        for record in convRecords:
            
            # パラメータ取得
            tenantId = record["tenantId"]
            status = record["status"]
            filename = record["filename"]
            timestamp = record["timestamp"]
            data = record["data"]
            LOGGER.info("param(tenantId:%s, status:%s, filename:%s, timestamp:%s, len(data):%d)" % 
                        (tenantId, status, filename, timestamp, len(data)))
            
            # エポックミリ秒→datetime変換
            ceatedDateTime = convertEpokMillSecToDateTime(timestamp)
            
            # S3保存先
            key = "soundstrage/%s/%s/%s" % (tenantId
                                            , ceatedDateTime.strftime("%Y%m%d")
                                            , filename)
            LOGGER.info("S3保存先[%s]" % key)
            
            # データ定義マスタシーケンスの取得
            dataCollectionResult = rds.fetchone(initCommon.getQuery("sql/m_data_collection/findById.sql"),
                                                {
                                                    DEVICE_ID : messageDict[DEVICE_ID]
                                                })
            if (dataCollectionResult is None):
                LOGGER.warn("データ定義マスタが存在しないため、スキップします。[tenantId:%s]" % messageDict[DEVICE_ID] )
                continue
            LOGGER.info("データ定義マスタシーケンス[%d]" % dataCollectionResult[DATA_COLLECTION_SEQ])
            
            # 音ファイル作成履歴の追加
            rds.execute(initCommon.getQuery("sql/sound/upsert.sql")
                        ,{
                            DATA_COLLECTION_SEQ : dataCollectionResult[DATA_COLLECTION_SEQ]
                            , CREATED_DATETIME : ceatedDateTime
                            , FILE_TYPE : status
                            , FILE_NAME : key
                            , CREATED_AT : initCommon.getSysDateJst()
                            })
            
            # dataのデコード
            decodeData = base64.b64decode(data)
            
            # 音ファイルをS3へput
            obj = s3.Object(event[CLIENT_NAME], key)
            obj.put( Body=decodeData, ContentType="audio/mp3" )
        
    # コミット
    rds.commit()
    
    # 後処理
    del rds