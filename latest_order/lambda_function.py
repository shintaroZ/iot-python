import json
import sys
import datetime
import configparser
import initCommon  # カスタムレイヤー
import rdsCommon  # カスタムレイヤー
from enum import IntEnum


# 収集データ区分要素
class CollectionValueTypeEnum(IntEnum):
    Number = 0
    Boolean = 1


# 蓄積有無要素
class SavingEnum(IntEnum):
    Invalid = 0
    Valid = 1


# 閾値判定有無要素
class LimitCheckEnum(IntEnum):
    Invalid = 0
    Valid = 1


# エッジ区分要素
class EdgeTypeEnum(IntEnum):
    DeviceGateway = 1
    Monone = 2


# スコアデータ要素
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

# パラメータ用定数
CLIENT_NAME = "clientName"
RECEVERY_FLG = "receveryFlg"
RECEIVED_MESSAGES = "receivedMessages"
DEVICE_ID = "deviceId"
REQUEST_TIMESTAMP = "requestTimeStamp"
RECORDS = "records"
SENSOR_ID = "sensorId"
TIMESTAMP = "timeStamp"
VALUE = "value"
TENANT_ID = "tenantId"
SCORE = "score"


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
        setRetryMaxCount(config_ini['rds setting']['retryMaxcount'])
        setRetryInterval(config_ini['rds setting']['retryinterval'])
    except Exception as e:
        print ('設定ファイルの読み込みに失敗しました。')
        raise(e)


# --------------------------------------------------
# データ定義マスタの取得
# --------------------------------------------------
def getMasterDataCollection(rds, paramDeviceId, paramSensorId):
    paramArray = []
    paramArray.append("mdc.DEVICE_ID = '%s'" % paramDeviceId)
    if not paramSensorId is None:
        paramArray.append("mdc.SENSOR_ID = '%s'" % paramSensorId)
    
    params = { "whereParam" : " and ".join(paramArray)}
    query = initCommon.getQuery("sql/m_data_collection/findbyId.sql")
    try:
        result = rds.fetchone(query, params)
        if result is None:
            LOGGER.error("センサ振分けマスタに未定義のデータを受信しました。(%s / %s)" % (paramDeviceId, paramSensorId));
            del rds
            sys.exit()
    except Exception  as e:
        LOGGER.error('センサ振分けマスタ取得時にエラーが発生しました。(%s / %s)' % (paramDeviceId, paramSensorId));
        del rds
        raise(e)

    return result


# --------------------------------------------------
# 登録先テーブルの件数取得
# --------------------------------------------------
def getRegTableCount(rds, even):
    params = {
        "dataCollectionSeq": dataCollectionSeq
        , "receivedDateTime": receivedDateTime
    }
    query = initCommon.getQuery("sql/t_public_timeseries/findbyId.sql")
    result = rds.fetchone(query, params)

    return result


# --------------------------------------------------
# records配下のパラメータチェックし、辞書型で返却
# --------------------------------------------------
def isArgmentRecord(deviceId, requestTimeStamp, record, dataCollectionResultDict):
    
    resultMap = {}
    resultMap["isErr"] = False
    resultMap["errorMessage"] = ""
    resultMap["cnvTimeStamp"] = ""
    resultMap["cnvValue"] = ""
    
    try:
        # timeStampが指定ありの場合のみチェック
        if not record.get(TIMESTAMP) is None:
            if initCommon.validateTimeStamp(record.get(TIMESTAMP)):
                resultMap["cnvTimeStamp"] = datetime.datetime.strptime(record[TIMESTAMP], '%Y-%m-%d %H:%M:%S.%f')
                resultMap["cnvTimeStamp"] = resultMap["cnvTimeStamp"] + datetime.timedelta(seconds=32400)
            else:
                resultMap["errorMessage"] = "センサの受信タイムスタンプが不正です。(デバイスID:%s 送信日時:%s センサID:%s タイムスタンプ:%s)" % \
                                                                                    (
                                                                                        deviceId
                                                                                        , requestTimeStamp
                                                                                        , record.get(SENSOR_ID)
                                                                                        , record.get(TIMESTAMP)) 
                raise Exception(resultMap["errorMessage"])

        # valueが指定ありの場合のみチェック
        if not record.get(VALUE) is None:
            # 収集データ区分=0（数値）の場合、起動パラメータ.レコード一覧.値が数値であること。
            if (dataCollectionResultDict["collectionValueType"] == CollectionValueTypeEnum.Number) and (initCommon.isValidateNumber(record.get(VALUE))):
                resultMap["cnvValue"] = round((float(record.get(VALUE)) * dataCollectionResultDict["revisionMagification"]), 2)

            # 収集データ区分=1（Boolean）の場合、起動パラメータ.レコード一覧.値がBoolean型であること。    
            elif (dataCollectionResultDict["collectionValueType"] == CollectionValueTypeEnum.Boolean) and initCommon.isValidateBoolean(record.get(VALUE)):
                # 型判定
                if (isinstance(record.get(VALUE), str) and record.get(VALUE).upper() == "FALSE") \
                   or (isinstance(record.get(VALUE), bool) and value == False):
                    resultMap["cnvValue"] = 1
                else:
                    resultMap["cnvValue"] = 0
            else:
                resultMap["errorMessage"] = "センサの値が不正です。(デバイスID:%s 送信日時:%s センサID:%s 値:%s)" % \
                                                                    (
                                                                        deviceId
                                                                        , requestTimeStamp
                                                                        , record.get(SENSOR_ID)
                                                                        , record.get(VALUE)) 
                raise Exception(resultMap["errorMessage"])
                
        # エッジ区分＝2（Monone）の場合のみチェック
        if dataCollectionResultDict["edgeType"] == EdgeTypeEnum.Monone:
            
            # 存在チェック
            if record.get(TENANT_ID) is None:
                resultMap["errorMessage"] = "TENANT_IDが存在しません。(デバイスID:%s 送信日時:%s)" % (deviceId, requestTimeStamp) 
                raise Exception(resultMap["errorMessage"])
            if record.get(SCORE) is None:
                resultMap["errorMessage"] = "スコアデータが存在しません。(デバイスID:%s 送信日時:%s)" % (deviceId, requestTimeStamp) 
                raise Exception(resultMap["errorMessage"])
            
            # スペース区切りで分割し、要素数が9個であること。
            scoreArray = record.get(SCORE).replace("\n", "").split(" ")
            if not len(scoreArray) == 9:
                resultMap["errorMessage"] = "スコアデータのフォーマットが不正です。(デバイスID:%s 送信日時:%s スコアデータ:%s)" % \
                                                                                    (
                                                                                        deviceId
                                                                                        , requestTimeStamp
                                                                                        , record.get(SCORE)) 
                raise Exception(resultMap["errorMessage"])
            
            # 第１要素と第２要素を結合
            strDateTime = "%s %s" % (scoreArray[ScoreEnum.Date], scoreArray[ScoreEnum.Time])
            resultMap["cnvTimeStamp"] = datetime.datetime.strptime(strDateTime, "%Y/%m/%d %H:%M:%S")
                
    except Exception as ex:
        resultMap["isErr"] = True
        LOGGER.warn("isArgmentRecord Error : %s" , ex)
    return resultMap

    
# --------------------------------------------------
# 時系列テーブル登録用のValuesパラメータ作成
# --------------------------------------------------
def createPublicTableValues(dataCollectionSeq, receivedDateTime, sensorValue, createdDateTime):
    return "(%d, '%s', %f, '%s')" % (dataCollectionSeq
                                     , receivedDateTime
                                     , sensorValue
                                     , createdDateTime)


# --------------------------------------------------
# スコアデータテーブル登録用のValuesパラメータ作成
# --------------------------------------------------
def createScoreTableValues(dataCollectionSeq, cnvTimeStamp, score, createdDateTime):
    
    scoreArray = score.replace("\n", "").split(" ")
    return "(%d, '%s', '%s', '%s', '%s', %f, %f, %f, %f, %f, %f, '%s')" % (dataCollectionSeq
                                     , cnvTimeStamp
                                     , scoreArray[ScoreEnum.Date]
                                     , scoreArray[ScoreEnum.Time]
                                     , scoreArray[ScoreEnum.Flag]
                                     , float(scoreArray[ScoreEnum.Min])
                                     , float(scoreArray[ScoreEnum.Max])
                                     , float(scoreArray[ScoreEnum.Value])
                                     , float(scoreArray[ScoreEnum.Threshold])
                                     , float(scoreArray[ScoreEnum.SlidingUpper])
                                     , float(scoreArray[ScoreEnum.SlidingLower])
                                     , createdDateTime)


# --------------------------------------------------
# テーブル更新（BULK INSERT用）
# --------------------------------------------------
def bulkInsert(rds, valuesArray, query):
    if 0 < len(valuesArray):
        p_values = ",".join(valuesArray)
        params = {'p_values': p_values}
        rds.execute(query, params, RETRY_MAX_COUNT, RETRY_INTERVAL)
    else:
        LOGGER.debug("更新対象なし")


# --------------------------------------------------
# 監視テーブル登録用のValuesパラメータ作成
# --------------------------------------------------
def createSurveillanceValues(paramOccurredDatetime, paramFunctionName, paramMessage, registerDateTime):
    return "('%s', '%s', '%s', '%s')" % (paramOccurredDatetime,
                                         paramFunctionName,
                                         paramMessage,
                                         registerDateTime)


#####################
# main
#####################
def lambda_handler(event, context):

    # 初期処理
    initConfig(event[CLIENT_NAME])
    setLogger(initCommon.getLogger(LOG_LEVEL))
    # setLogger(initCommon.getLogger("DEBUG"))

    LOGGER.info('公開DB作成開始 : %s' % event)

    # RDSコネクション作成
    rds = rdsCommon.rdsCommon(LOGGER, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, DB_CONNECT_TIMEOUT)

    # リカバリ判定
    isRecevery = True if (RECEVERY_FLG in event and event[RECEVERY_FLG] == 1) else False

    reReceivedMessages = []
    reRecords = []
    for e in event[RECEIVED_MESSAGES]:

        # 親要素の取得
        deviceId = e[DEVICE_ID]
        requestTimeStamp = e[REQUEST_TIMESTAMP]

        # センサID もしくは TENANT_IDで昇順ソート
        sortedRecords = e[RECORDS]
        if not e.get(SENSOR_ID) is None:
            sortedRecords = sorted(e[RECORDS], key=lambda x:x[SENSOR_ID])
        if not e.get(TENANT_ID) is None:
            sortedRecords = sorted(e[RECORDS], key=lambda x:x[TENANT_ID])
        
        beforeSensorId = ""

        # BULK INSERT用配列
        publicTableValues = []
        surveillanceValues = []
        scoreTableValues = []

        for record in sortedRecords:
            # 現在時刻取得
            nowDateTime = initCommon.getSysDateJst()

            # 各センサの要素取得
            sensorId = record.get(SENSOR_ID)
            timeStamp = record.get(TIMESTAMP)
            value = record.get(VALUE)
            tenantId = record.get(TENANT_ID)
            score = record.get(SCORE)

            # 初回 or センサが切り替わったタイミングでマスタ取得
            if sensorId != beforeSensorId or tenantId is not None:
                # データ定義マスタの取得
                dataCollectionResultDict = getMasterDataCollection(rds, deviceId, sensorId)
                dataCollectionSeq = int(dataCollectionResultDict['dataCollectionSeq'])
                collectionValueType = dataCollectionResultDict['collectionValueType']
                revisionMagification = 1 if (dataCollectionResultDict['revisionMagification'] is None) else float(dataCollectionResultDict['revisionMagification'])
                savingFlg = int(dataCollectionResultDict['savingFlg'])
                limitCheckFlg = int(dataCollectionResultDict['limitCheckFlg'])
                LOGGER.info("データ定義マスタ取得:%s" % dataCollectionResultDict)
                        
            # リカバリ時のみセンサが切り替わったタイミングで中間コミット
            if isRecevery and beforeSensorId != "" and sensorId != beforeSensorId:
                LOGGER.info("%sの処理完了のため、中間コミットします。" % beforeSensorId)
                bulkInsert(rds, publicTableValues, initCommon.getQuery("sql/t_public_timeseries/insert.sql"))
                bulkInsert(rds, surveillanceValues, initCommon.getQuery("sql/t_surveillance/insert.sql"))
                bulkInsert(rds, scoreTableValues, initCommon.getQuery("sql/t_score/insert.sql"))
                rds.commit()

                # 一時配列クリア
                publicTableValues = []
                surveillanceValues = []
                scoreTableValues = []

            # 前回値退避
            beforeSensorId = sensorId
            
            # 起動パラメータチェック
            isArgumentResultMap = isArgmentRecord(deviceId, requestTimeStamp, record, dataCollectionResultDict)
            
            # 公開DBのカウント
            if dataCollectionResultDict['edgeType'] == EdgeTypeEnum.DeviceGateway:
                resultPubDict = rds.fetchone(initCommon.getQuery("sql/t_public_timeseries/findbyId.sql")
                                      , {
                                            "dataCollectionSeq": dataCollectionSeq
                                            , "receivedDateTime": isArgumentResultMap["cnvTimeStamp"]
                                        })
                if resultPubDict["count"] != 0:
                    isArgumentResultMap["errorMessage"] = "センサの欠損を検知しました。(デバイスID:%s 送信日時:%s センサID:%s タイムスタンプ:%s)" % \
                                                                                        (
                                                                                            deviceId
                                                                                            , requestTimeStamp
                                                                                            , record.get(SENSOR_ID)
                                                                                            , record.get(TIMESTAMP)) 

            elif dataCollectionResultDict['edgeType'] == EdgeTypeEnum.Monone:
                resultPubDict = rds.fetchone(initCommon.getQuery("sql/t_score/findbyId.sql")
                                      , {
                                            "dataCollectionSeq": dataCollectionSeq
                                            , "detectionDateTime": isArgumentResultMap["cnvTimeStamp"]
                                        })
                if resultPubDict["count"] != 0:
                    isArgumentResultMap["errorMessage"] = "センサの欠損を検知しました。(TENANT_ID:%s 送信日時:%s)" % \
                                                                                        (
                                                                                            deviceId
                                                                                            , requestTimeStamp) 
            else:
                LOGGER.warn("エッジ区分が不正です。[%d]" % dataCollectionResultDict['edgeType'])
                continue
            
            # ---------------------------
            # センサ登録/リカバリー判定
            # ---------------------------            
            # 蓄積有無判定
            if savingFlg != SavingEnum.Valid:
                LOGGER.info("蓄積対象外の為、スキップします。(%s / %s)" % (deviceId, sensorId))
                continue
         
            # 更新先テーブルを分岐
            if isArgumentResultMap["errorMessage"] == "" or isRecevery: 
                if dataCollectionResultDict['edgeType'] == EdgeTypeEnum.DeviceGateway:
                    # 更新先：時系列テーブル
                    LOGGER.info("時系列テーブル更新[ %d / %s / %s ]" % (dataCollectionSeq
                                                                     , isArgumentResultMap["cnvTimeStamp"]
                                                                     , isArgumentResultMap["cnvValue"]))
                    publicTableValues.append(createPublicTableValues(dataCollectionSeq
                                                                     , isArgumentResultMap["cnvTimeStamp"]
                                                                     , isArgumentResultMap["cnvValue"]
                                                                     , nowDateTime))
                    
                elif dataCollectionResultDict['edgeType'] == EdgeTypeEnum.Monone:
                    # 更新先：スコアテーブル
                    LOGGER.info("スコアテーブル更新[ %d / %s / %s ]" % (dataCollectionSeq
                                                                     , isArgumentResultMap["cnvTimeStamp"]
                                                                     , record[SCORE]))
                    scoreTableValues.append(createScoreTableValues(dataCollectionSeq
                                                                   , isArgumentResultMap["cnvTimeStamp"]
                                                                   , record[SCORE]
                                                                   , nowDateTime))
                
                # 閾値判定ありの要素のみ戻り値に含める
                if limitCheckFlg == LimitCheckEnum.Valid:
                    reMap = {}
                    reMap["dataCollectionSeq"] = str(dataCollectionSeq)
                    reMap["receivedDatetime"] = isArgumentResultMap["cnvTimeStamp"].strftime('%Y/%m/%d %H:%M:%S.%f')
                    reRecords.append(reMap)
            else:
                # 更新先：監視テーブル
                LOGGER.info("監視テーブル更新[ %s / %s / %s ]" % (nowDateTime
                                                                 , "公開DB作成機能"
                                                                 , isArgumentResultMap["errorMessage"]))
                surveillanceValues.append(createSurveillanceValues(nowDateTime
                                                                   , "公開DB作成機能"
                                                                   , isArgumentResultMap["errorMessage"]
                                                                   , nowDateTime))
        
        # commit
        bulkInsert(rds, publicTableValues, initCommon.getQuery("sql/t_public_timeseries/insert.sql"))
        bulkInsert(rds, surveillanceValues, initCommon.getQuery("sql/t_surveillance/insert.sql"))
        bulkInsert(rds, scoreTableValues, initCommon.getQuery("sql/t_score/insert.sql"))
        rds.commit()

    # close
    del rds

    # 戻り値整形
    reEventTable = {}
    if isRecevery == False:
        reEventTable[CLIENT_NAME] = event[CLIENT_NAME]
        reEventTable[RECORDS] = reRecords
    return reEventTable
