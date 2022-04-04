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
def setBucketName(bucketName):
    global BUCKET_NAME
    BUCKET_NAME = bucketName
def setClientName(clientName):
    global CLIENT_NAME
    CLIENT_NAME = clientName
    
# --------------------------------------------------
# 設定ファイル読み込み
# --------------------------------------------------
def initConfig(filePath):
    try:
        config_ini = configparser.ConfigParser()
        config_ini.read(filePath, encoding='utf-8')

        setLogLevel(config_ini['logger setting']['loglevel'])
        setDbHost(config_ini['rds setting']['host'])
        setDbPort(config_ini['rds setting']['port'])
        setDbUser(config_ini['rds setting']['user'])
        setDbPassword(config_ini['rds setting']['password'])
        setDbName(config_ini['rds setting']['db'])
        setDbConnectTimeout(config_ini['rds setting']['connect_timeout'])
        
        setBucketName(config_ini['bucket setting']['bucket_name'])
        setClientName(config_ini['bucket setting']['client_name'])
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
     
# --------------------------------------------------
# エッジマスタ取得用のパラメータ生成
# --------------------------------------------------
def createMasterEdgeParam(tenantId):
    param = {}
    param["p_edge_id"] = tenantId
    return param
    
# --------------------------------------------------
# 音ファイル作成履歴更新用のパラメータ生成
# --------------------------------------------------
def createSoundParam(ceatedDateTime, edgeName, status, filename):
    param = {}
    param["p_created_datetime"] = ceatedDateTime
    param["p_edge_name"] = edgeName
    param["p_status"] = status
    param["p_file_name"] = filename
    param["p_created_at"] = initCommon.getSysDateJst()
    return param

#####################
# main
#####################
def lambda_handler(event, context):

    # 初期処理
    initConfig(event["setting"])
    setLogger(initCommon.getLogger(LOG_LEVEL))
    
    LOGGER.info("音ファイル作成機能開始")
    
    # RDSコネクション作成
    rds = rdsCommon.rdsCommon(LOGGER, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, DB_CONNECT_TIMEOUT)
    
    # S3リソースのインスタンス作成
    s3 = boto3.resource('s3')
    
    # レコード再形成
    convRecords = getParamReConv(event['records'])
    
    # レコード分繰り返し
    for record in convRecords:
        
        # パラメータ取得
        tenantId = record["tenantId"]
        status = record["status"]
        filename = record["filename"]
        timestamp = record["timestamp"]
        # chunk_no = record["chunk_no"]
        # chunk_total = record["chunk_total"]
        data = record["data"]
        LOGGER.info("param(tenantId:%s, status:%s, filename:%s, timestamp:%s, len(data):%d)" % 
                    (tenantId, status, filename, timestamp, len(data)))
        
        # エポックミリ秒→datetime変換
        ceatedDateTime = convertEpokMillSecToDateTime(timestamp)
        
        # エッジマスタ検索
        edgeRecord = rds.fetchone(initCommon.getQuery("sql/m_edge/findById.sql"),
                                  createMasterEdgeParam(tenantId))
        if (edgeRecord is None):
            errMsg = "エッジマスタに存在しない設備IDが連携されました。:%s" %  tenantId
            LOGGER.error(errMsg)
            return
        
        # 音ファイル作成履歴の追加
        rds.execute(initCommon.getQuery("sql/sound/insert.sql"),
                    createSoundParam(ceatedDateTime,
                                     edgeRecord["edgeName"],
                                     status,
                                     filename))
        
        # S3のオブジェクトkey生成
        key = "%s/%s/%s/%s" % (CLIENT_NAME, 
                               edgeRecord["edgeName"],
                               ceatedDateTime.strftime("%Y%m%d"),
                               filename)
                               
        # dataのデコード
        decodeData = base64.b64decode(data)
        
        # 音ファイルをS3へput
        obj = s3.Object(BUCKET_NAME, key)
        obj.put( Body=decodeData, ContentType="audio/mp3" )
        
    # コミット
    rds.commit()
    
    # 後処理
    del rds