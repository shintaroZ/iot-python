import json
import sys
import datetime
import configparser
import initCommon # カスタムレイヤー
import rdsCommon # カスタムレイヤー

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
ROW_LOCK_TIMEOUT = 50
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
def setRowLockTimeout(rowLockTimeout):
    global ROW_LOCK_TIMEOUT
    ROW_LOCK_TIMEOUT = int(rowLockTimeout)
def setRetryMaxCount(retryMaxCount):
    global RETRY_MAX_COUNT
    RETRY_MAX_COUNT = int(retryMaxCount)
def setRetryInterval(retryInterval):
    global RETRY_INTERVAL
    RETRY_INTERVAL = int(retryInterval)

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
        setRowLockTimeout(config_ini['rds setting']['innodb_lock_wait_timeout'])
        setRetryMaxCount(config_ini['rds setting']['retryMaxcount'])
        setRetryInterval(config_ini['rds setting']['retryinterval'])
    except Exception as e:
        print ('設定ファイルの読み込みに失敗しました。')
        raise(e)

# --------------------------------------------------
# センサ振分けマスタの取得
# --------------------------------------------------
def getMasterSensorDistribution(rds, paramDeviceId, paramSensorId):
    params = {
        "p_deviceId": paramDeviceId
        ,"p_sensorId": paramSensorId
    }
    query = initCommon.getQuery("sql/m_senrosr_distribution/findbyId.sql")
    try:
        # 一度に複数クエリは発行出来ないので別出し
        rds.execute("set innodb_lock_wait_timeout = %d" % int(ROW_LOCK_TIMEOUT))
        rds.execute("begin")
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
def getPublicTable(rds, paramCorrectionType, paramReceivedDateTime, paramSensorName):
    params = {
        "p_collectionType": paramCorrectionType
        ,"p_receivedDateTime": paramReceivedDateTime
        ,"p_sensorName" : paramSensorName
    }
    query = initCommon.getQuery("sql/t_public_timeseries/findbyId.sql")
    result = rds.fetchone(query, params)

    return result

# --------------------------------------------------
# 公開DBの登録
# --------------------------------------------------
def insertPublicTable(rds, paramCorrectionType, paramReceivedDateTime, paramSensorName, paramSensorValue, paramSensorUnit, registerDateTime):
    params = {
        "p_collectionType": paramCorrectionType
        ,"p_receivedDateTime": paramReceivedDateTime
        ,"p_sensorName": paramSensorName
        ,"p_sensorValue": paramSensorValue
        ,"p_sensorUnit": paramSensorUnit
        ,"p_createdDateTime": registerDateTime
    }
    query = initCommon.getQuery("sql/t_public_timeseries/insert.sql")
    rds.execute(query, params)

# --------------------------------------------------
# 監視テーブルの更新
# --------------------------------------------------
def insertSurveillance(rds, paramOccurredDatetime, paramFunctionName, paramMessage, registerDateTime):
    params = {
        "p_occurredDateTime": paramOccurredDatetime
        ,"p_functionName": paramFunctionName
        ,"p_message": paramMessage
        ,"p_createdDateTime": registerDateTime
    }
    query = initCommon.getQuery("sql/t_surveillance/insert.sql")
    rds.execute(query, params)

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
# 起動パラメータの再形成
# --------------------------------------------------
def getEventListReConv(event):

    reEvent = None
    recordList = []
    subTable = {}
    reReceivedMessages = []
    reEventTable = {}
    bkDeviceId = None

    # デバイスID昇順ソート
    event["receivedMessages"].sort(key=lambda x:x['deviceId'])

    for i in range(len(event["receivedMessages"])):

        receivedMessages = event["receivedMessages"][i]
        recordList = recordList + receivedMessages["records"]

        # デバイスIDが変わった場合、レコードを再形成
        if (i == len(event["receivedMessages"]) -1 or
            (0 < i and receivedMessages["deviceId"] != bkDeviceId)):

            # 必須項目でないもの
            if ("receveryFlg" in receivedMessages):
                subTable["receveryFlg"] = receivedMessages["receveryFlg"]
            subTable["deviceId"] = receivedMessages["deviceId"]
            subTable["requestTimeStamp"] = receivedMessages["requestTimeStamp"]
            subTable["records"] = recordList
            reReceivedMessages.append(subTable)

            # 一時テーブル初期化
            subTable = {}
            recordList = []

        # 前回デバイスIDを待避
        bkDeviceId = receivedMessages["deviceId"]

    # 親要素の整形
    reEventTable["clientName"] = event["clientName"]
    reEventTable["receivedMessages"] = reReceivedMessages

    event = reEventTable
#####################
# main
#####################
def lambda_handler(event, context):

    # 引数のデータ型判定
    eventList = []
    if isinstance(event, list):
        eventList.extend(event)
    else:
        eventList.append(event)

    for e in eventList:

        # 初期処理
        initConfig(e["setting"])
        setLogger(initCommon.getLogger(LOG_LEVEL))

        LOGGER.info('公開DB作成開始 : %s' % e)

        # RDSコネクション作成
        rds = rdsCommon.rdsCommon(LOGGER, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, DB_CONNECT_TIMEOUT)

        # 親要素の取得
        deviceId = e['deviceId']
        requestTimeStamp = e['requestTimeStamp']

        # センサIDで昇順ソート
        sortedRecords = sorted(e['records'],key=lambda x:x['sensorId'])
        beforeSensorId = ""
        for record in sortedRecords:
            # 現在時刻取得
            nowDateTime = initCommon.getSysDateJst()

            # 各センサの要素取得
            sensorId = record['sensorId']
            timeStamp = record['timeStamp']
            value = record['value']

            # 中間コミット
            if beforeSensorId != "" and sensorId != beforeSensorId:
                LOGGER.debug("%sの処理完了のため、中間コミットします。" % beforeSensorId)
                rds.commit

            # 内部DB(センサ振分けマスタ)の取得
            res = getMasterSensorDistribution(rds, deviceId, sensorId)
            tableName = res['tableName']
            sensorName = res['sensorName']
            correctionMagnification = float(res['correctionMagnification'])
            correctionType = res['correctionType']
            sensorUnit = res['sensorUnit']

            # 単位合わせ用に加工
            cnvTimeStamp = None
            cnvValue = None
            if validateTimeStamp(timeStamp):
                cnvTimeStamp = datetime.datetime.strptime(timeStamp, '%Y-%m-%d %H:%M:%S.%f')
                cnvTimeStamp = cnvTimeStamp + datetime.timedelta(seconds=32400)
            if validateNumber(value):
                cnvValue = round((float(value) * correctionMagnification), 2)

            # 公開DBの取得
            resPub = getPublicTable(rds, correctionType, cnvTimeStamp, sensorName)

            # センサ登録判定
            errMsg = ""
            if resPub["count"] != 0:
                errMsg = "センサの欠損を検知しました。(デバイスID:%s 送信日時:%s センサ名:%s タイムスタンプ:%s)" % (deviceId, requestTimeStamp, sensorName, cnvTimeStamp)
            elif cnvTimeStamp is None:
                errMsg = "センサの受信タイムスタンプが不正です。(デバイスID:%s 送信日時:%s センサ名:%s タイムスタンプ:%s)" % (deviceId, requestTimeStamp, sensorName, cnvTimeStamp)
            elif cnvValue is None:
                errMsg = "センサの値が不正です。(デバイスID:%s 送信日時:%s センサ名:%s 値:%s)" % (deviceId, requestTimeStamp, sensorName, value)

            if len(errMsg) == 0:
                LOGGER.debug('★登録対象 (%s / %s / %s / %s / %s)' % (tableName, cnvTimeStamp, sensorName, cnvValue, nowDateTime))
                insertPublicTable(rds, correctionType, cnvTimeStamp, sensorName, cnvValue, sensorUnit, nowDateTime)
            else:
                if ("receveryFlg" not in e) or (cnvTimeStamp is not None) or (cnvValue is not None):
                    LOGGER.error(errMsg)
                    insertSurveillance(rds, nowDateTime, "公開DB作成機能", errMsg, nowDateTime)
                else:
                    LOGGER.info("リカバリーの為、監視テーブルの更新をスキップします。")

            # 前回センサID
            beforeSensorId = sensorId
        # commit
        rds.commit()

    # close
    del rds