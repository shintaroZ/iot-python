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
    params = {
        "p_deviceId": paramDeviceId
        , "p_sensorId": paramSensorId
    }
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
# 公開DBの取得
# --------------------------------------------------
def getPublicTable(rds, p_dataCollectionSeq, p_receivedDateTime):
    params = {
        "p_dataCollectionSeq": p_dataCollectionSeq
        , "p_receivedDateTime": p_receivedDateTime
    }
    query = initCommon.getQuery("sql/t_public_timeseries/findbyId.sql")
    result = rds.fetchone(query, params)

    return result


# --------------------------------------------------
# 公開DB登録用のValuesパラメータ作成
# --------------------------------------------------
def createPublicTableValues(p_dataCollectionSeq, p_receivedDateTime, p_sensorValue, p_createdDateTime):
    return "(%d, '%s', %f, '%s')" % (p_dataCollectionSeq,
                                     p_receivedDateTime,
                                     p_sensorValue,
                                     p_createdDateTime)


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


# --------------------------------------------------
# 日付妥当性チェック(true:正常、false:異常）
# --------------------------------------------------
def validateTimeStamp(strTimeStamp):

    result = False
    try:
        # 文字列⇒日付変換で妥当性チェック
        datetime.datetime.strptime(strTimeStamp, '%Y-%m-%d %H:%M:%S.%f')
        result = True
    except ValueError:
        LOGGER.error('validate error (%s)' % strTimeStamp)

    return result


# --------------------------------------------------
# 浮動小数点数値チェック(true:正常、false:異常）
# --------------------------------------------------
def validateNumber(value):

    result = False
    try:
        # float型へのキャストで妥当性チェック
        float(value)
        result = True
    except ValueError:
        LOGGER.error('validate error (%s)' % value)

    return result


# --------------------------------------------------
# Boolean型チェック(true:正常、false:異常）
# --------------------------------------------------
def validateBoolean(value):

    result = False
    try:
        # 型判定
        if isinstance(value, str):
            boolStrList = ["TRUE", "FALSE"]
            if value.upper() in boolStrList:
                result = True
        elif isinstance(value, bool):
            result = True

    except ValueError:
        LOGGER.error('validate error (%s)' % value)

    return result


# --------------------------------------------------
# 起動パラメータの再形成
# --------------------------------------------------
def getEventListReConv(event):

    reEvent = None
    recordList = []
    subTable = {}
    tempTable = {}
    reReceivedMessages = []
    reEventTable = {}
    bkDeviceId = None

    # デバイスID昇順ソート
    event["receivedMessages"].sort(key=lambda x:x['deviceId'])

    # 受信メッセージ単位
    for i in range(len(event["receivedMessages"])):
        receivedMessages = event["receivedMessages"][i]

        # デバイスIDが変わった場合、レコードを再形成
        if ((0 < i and receivedMessages["deviceId"] != bkDeviceId)):
            reReceivedMessages.append(tempTable)

            # 一時テーブル初期化
            tempTable = {}

        # 各要素を一時保存
        if "deviceId" not in tempTable:
            tempTable["deviceId"] = receivedMessages["deviceId"]
        if "requestTimeStamp" not in tempTable:
            tempTable["requestTimeStamp"] = receivedMessages["requestTimeStamp"]
        # records要素は追記
        if "records" in tempTable:
            tempTable["records"] = tempTable["records"] + receivedMessages["records"]
        else:
            tempTable["records"] = receivedMessages["records"]

        # 前回デバイスIDを待避
        bkDeviceId = receivedMessages["deviceId"]

    # 最終ループ用
    if tempTable:
        reReceivedMessages.append(tempTable)

    # 親要素の整形
    reEventTable["clientName"] = event["clientName"]
    if "receveryFlg" in event:
        reEventTable["receveryFlg"] = event["receveryFlg"]
    reEventTable["receivedMessages"] = reReceivedMessages

    return reEventTable


#####################
# main
#####################
def lambda_handler(event, context):

    # 初期処理
    initConfig(event["clientName"])
    setLogger(initCommon.getLogger(LOG_LEVEL))

    LOGGER.info('公開DB作成開始 : %s' % event)

    # RDSコネクション作成
    rds = rdsCommon.rdsCommon(LOGGER, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, DB_CONNECT_TIMEOUT)

    # eventパラメータの再形成
    # eventList = getEventListReConv(event)

    # リカバリ判定
    isRecevery = False if "receveryFlg" not in event else True

    reReceivedMessages = []
    for e in event["receivedMessages"]:

        # 親要素の取得
        deviceId = e['deviceId']
        requestTimeStamp = e['requestTimeStamp']

        # センサIDで昇順ソート
        sortedRecords = sorted(e['records'], key=lambda x:x['sensorId'])
        beforeSensorId = ""

        # BULK INSERT用配列
        publicTableValues = []
        surveillanceValues = []

        reRecords = []
        for record in sortedRecords:
            # 現在時刻取得
            nowDateTime = initCommon.getSysDateJst()

            # 各センサの要素取得
            sensorId = record['sensorId']
            timeStamp = record['timeStamp']
            value = record['value']

            # 中間コミット（リカバリ時）
            if isRecevery and beforeSensorId != "" and sensorId != beforeSensorId:
                LOGGER.debug("%sの処理完了のため、中間コミットします。" % beforeSensorId)
                bulkInsert(rds, publicTableValues, initCommon.getQuery("sql/t_public_timeseries/insert.sql"))
                bulkInsert(rds, surveillanceValues, initCommon.getQuery("sql/t_surveillance/insert.sql"))
                rds.commit

                # 一時配列クリア
                publicTableValues = []
                surveillanceValues = []

            # データ定義マスタの取得
            res = getMasterDataCollection(rds, deviceId, sensorId)
            dataCollectionSeq = int(res['dataCollectionSeq'])
            collectionValueType = res['collectionValueType']
            revisionMagification = 1 if (res['revisionMagification'] is None) else float(res['revisionMagification'])
            savingFlg = int(res['savingFlg'])
            limitCheckFlg = int(res['limitCheckFlg'])

            # 単位合わせ用に加工
            cnvTimeStamp = None
            cnvValue = None
            if validateTimeStamp(timeStamp):
                # レコード一覧.タイムスタンプはUTCなのでJSTへ変換
                cnvTimeStamp = datetime.datetime.strptime(timeStamp, '%Y-%m-%d %H:%M:%S.%f')
                cnvTimeStamp = cnvTimeStamp + datetime.timedelta(seconds=32400)

            # 数値・Boolean判定
            if ((collectionValueType == CollectionValueTypeEnum.Number) and validateNumber(value)):
                cnvValue = round((float(value) * revisionMagification), 2)
            elif ((collectionValueType == CollectionValueTypeEnum.Boolean) and validateBoolean(value)):
                # 型判定
                if (isinstance(value, str) and value.upper() == "FALSE") or (isinstance(value, bool) and value == False):
                    cnvValue = 1
                else:
                    cnvValue = 0

            # 公開DBの取得
            resPub = getPublicTable(rds, dataCollectionSeq, cnvTimeStamp)

            # 閾値判定ありの要素のみ戻り値に含める
            if limitCheckFlg == LimitCheckEnum.Valid:
                reMap = {}
                reMap["sensorId"] = sensorId
                reMap["timeStamp"] = timeStamp
                reMap["value"] = value
                reRecords.append(reMap)

            # 蓄積有無判定
            if savingFlg != SavingEnum.Valid:
                LOGGER.info("蓄積対象外の為、スキップします。(%s / %s)" % (deviceId, sensorId))
                continue

            # センサ登録判定
            errMsg = ""
            if resPub["count"] != 0:
                errMsg = "センサの欠損を検知しました。(デバイスID:%s 送信日時:%s センサID:%s タイムスタンプ:%s)" % (deviceId, requestTimeStamp, sensorId, cnvTimeStamp)
            elif cnvTimeStamp is None:
                errMsg = "センサの受信タイムスタンプが不正です。(デバイスID:%s 送信日時:%s センサID:%s タイムスタンプ:%s)" % (deviceId, requestTimeStamp, sensorId, cnvTimeStamp)
            elif cnvValue is None:
                errMsg = "センサの値が不正です。(デバイスID:%s 送信日時:%s センサID:%s 値:%s)" % (deviceId, requestTimeStamp, sensorId, value)

            if len(errMsg) == 0:
                LOGGER.debug('★登録対象 (%d / %s / %f / %s)' % (dataCollectionSeq, cnvTimeStamp, cnvValue, nowDateTime))
                publicTableValues.append(createPublicTableValues(dataCollectionSeq, cnvTimeStamp, cnvValue, nowDateTime))
            else:
                if ("receveryFlg" in e and int(e["receveryFlg"]) == 1):
                    LOGGER.debug("リカバリーの為、監視テーブルの更新をスキップします。(%s / %s / %s)" % (deviceId, requestTimeStamp, sensorId))
                else:
                    LOGGER.warn(errMsg)
                    surveillanceValues.append(createSurveillanceValues(nowDateTime, "公開DB作成機能", errMsg, nowDateTime))

            # 前回センサID
            beforeSensorId = sensorId

        # commit
        bulkInsert(rds, publicTableValues, initCommon.getQuery("sql/t_public_timeseries/insert.sql"))
        bulkInsert(rds, surveillanceValues, initCommon.getQuery("sql/t_surveillance/insert.sql"))
        rds.commit()

        # 戻り値用に退避
        tempTable = {}
        tempTable["deviceId"] = deviceId
        tempTable["requestTimeStamp"] = requestTimeStamp
        tempTable["records"] = reRecords
        reReceivedMessages.append(tempTable)

    # close
    del rds

    # 戻り値整形
    reEventTable = {}
    reEventTable["clientName"] = event["clientName"]
    reEventTable["receivedMessages"] = reReceivedMessages
    return reEventTable
