import json
import configparser
import initCommon # カスタムレイヤー
import rdsCommon # カスタムレイヤー
from enum import IntEnum
import datetime
import sys

class ScoreEnum(IntEnum):
    Date = 0
    Time = 1
    Flag = 2
    Min = 3
    Max = 4
    Value = 5
    Threshold = 6
    SlidingUpper = 7
    SlidingLower = 8

    
# global
LOGGER = None
LOG_LEVEL = "INFO"
DB_HOST = "localhost"
DB_PORT = 3306
DB_USER = "hoge"
DB_PASSWORD = "hoge"
DB_NAME = "hoge"
DB_CONNECT_TIMEOUT = 3

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
    except Exception as e:
        print ('設定ファイルの読み込みに失敗しました。')
        raise(e)

# --------------------------------------------------
# エッジマスタ取得用のパラメータ生成
# --------------------------------------------------
def createMasterEdgeParam(tenantId):
    param = {}
    param["p_edge_id"] = tenantId
    return param
    
# --------------------------------------------------
# スコアデータ登録用のパラメータ生成
# --------------------------------------------------
def createRegisterScoreParam(scoreArray, edgeName):
    param = {}
    param["p_edge_name"] = edgeName
    strDateTime = "%s %s" % (scoreArray[ScoreEnum.Date], scoreArray[ScoreEnum.Time])
    param["p_detection_datetime"] = datetime.datetime.strptime(strDateTime, "%Y/%m/%d %H:%M:%S")
    param["p_detection_date"] = scoreArray[ScoreEnum.Date]
    param["p_detection_time"] = scoreArray[ScoreEnum.Time]
    param["p_detection_flag"] = scoreArray[ScoreEnum.Flag]
    param["p_detection_min"] = float(scoreArray[ScoreEnum.Min])
    param["p_detection_max"] = float(scoreArray[ScoreEnum.Max])
    param["p_detection_value"] = float(scoreArray[ScoreEnum.Value])
    param["p_detection_threshold"] = float(scoreArray[ScoreEnum.Threshold])
    param["p_sliding_upper"] = float(scoreArray[ScoreEnum.SlidingUpper])
    param["p_sliding_lower"] = float(scoreArray[ScoreEnum.SlidingLower])
    param["p_created_at"] = initCommon.getSysDateJst()
    return param
    
# --------------------------------------------------
# スコアID取得用のパラメータ生成
# --------------------------------------------------
def createScoreIdParam(edgeId, oneHourAgoDateTime):
    param = {}
    param["p_edge_id"] = edgeId
    param["p_one_hour_ago_datetime"] = oneHourAgoDateTime
    return param
    
# --------------------------------------------------
# 設備状態更新用のパラメータ生成
# --------------------------------------------------
def createUpsertEquipmentStatusParam(scoreResult):
    param = {}
    param["p_edge_name"] = scoreResult["edgeName"]
    
    if scoreResult["oldErrScoreId"] is None:
        param["p_score_id"] = int(scoreResult["latestScoreId"])
        param["p_equipment_status"] = 0
    else:
        param["p_score_id"] = int(scoreResult["oldErrScoreId"])
        param["p_equipment_status"] = 1
        
    param["p_register_at"] = initCommon.getSysDateJst()
    return param
    
#####################
# main
#####################
def lambda_handler(event, context):

    # 初期処理
    initConfig(event["setting"])
    setLogger(initCommon.getLogger(LOG_LEVEL))

    LOGGER.info("スコアデータ作成機能開始:%s" % event)
    
    # RDSコネクション作成
    rds = rdsCommon.rdsCommon(LOGGER, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, DB_CONNECT_TIMEOUT)
    
    edgeDict = {}
    for record in event["records"]:
        
        tenantId = record["tenantId"]
        
        # None判定
        if record["score"] is None:
            LOGGER.warn("scoreデータがNullの為、処理をスキップします。")
            continue
        
        # スコアデータは改行を除く半角スペース区切り
        scoreArray = record["score"].replace("\n", "").split(" ")
        
        # スコアデータ要素が9分割でなければエラー
        if not len(scoreArray) == 9:
            LOGGER.warn("scoreデータのフォーマットが不正の為、処理をスキップします。 : %s" % scoreArray)
            continue
        
        # エッジ名の取得
        edgeResult = rds.fetchone(initCommon.getQuery("sql/m_edge/findById.sql"),
                                  createMasterEdgeParam(tenantId))
        if edgeResult is None:
            LOGGER.error("エッジ名の取得に失敗しました。設備ID：%s" % tenantId)
            sys.exit()
        edgeDict[tenantId] = edgeResult["edgeName"]
        
        # スコアデータの登録
        rds.execute(initCommon.getQuery("sql/t_score/insert.sql"),
                    createRegisterScoreParam(scoreArray, edgeResult["edgeName"]))
    
    # 現在時刻から1h前
    oneHourAgoDateTime = initCommon.getSysDateJst() + datetime.timedelta(hours=-1)
    for edgeId in edgeDict.keys():
        # 現在時刻から1h前のスコアデータを対象にエラーが発生した最古のスコアID(oldErrScoreId)と
        # 最新のスコアID(latestScoreId)を取得する。
        scoreResult= rds.fetchone(initCommon.getQuery("sql/t_score/getScoreId.sql"),
                                  createScoreIdParam(edgeId, oneHourAgoDateTime))
                                  
        if (scoreResult["latestScoreId"] is None) and (scoreResult["oldErrScoreId"] is None):
            LOGGER.warn("該当のスコアデータが存在しないため、%sの設備状態の更新をスキップします。" % scoreResult["edgeName"])
            continue
        else:
            # 設備情報のUpsert
            rds.execute(initCommon.getQuery("sql/t_equipment_status/upsert.sql"),
                        createUpsertEquipmentStatusParam(scoreResult))
    
    # コミット
    rds.commit()
    
    # 後処理
    del rds