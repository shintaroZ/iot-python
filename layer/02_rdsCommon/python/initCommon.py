import logging
import datetime
import sys
import boto3
import botocore
from botocore.config import Config
import base64
import json

JSON_DATETIME_FORMAT = "%Y/%m/%d %H:%M:%S.%f"
# setter
def setJsonDateTimeFormat(format):
    global JSON_DATETIME_FORMAT
    JSON_DATETIME_FORMAT = format

# --------------------------------------------------
# JSTのtime.struct_timeを返却する
# --------------------------------------------------
def customTime(*args):
    utc = datetime.datetime.now(datetime.timezone.utc)
    jst = datetime.timezone(datetime.timedelta(hours=+9))
    return utc.astimezone(jst).timetuple()

# --------------------------------------------------
# JSTの現在日時を返却する
# --------------------------------------------------
def getSysDateJst():
    utc = datetime.datetime.now(datetime.timezone.utc)
    jst = datetime.timezone(datetime.timedelta(hours=+9))
    return utc.astimezone(jst)

# --------------------------------------------------
# ファイル読み込み
# query_file_path(str)  : ファイルパス
# --------------------------------------------------
def getQuery(query_file_path):
    with open(query_file_path, 'r', encoding='utf-8') as f:
        query = f.read()
    return query
# --------------------------------------------------
# ロガー初期設定
# loglevel(str)  : ログレベル
# --------------------------------------------------
def getLogger(loglevel):
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
    logger.setLevel(loglevel)

    return logger

# --------------------------------------------------
# base64urlへデコード
# --------------------------------------------------
def decodeBase64Url(s):

    return base64.urlsafe_b64decode(s + '=' * (-len(s) % 4))

# --------------------------------------------------
# JWTを分解しボディ部（ペイロード）をJson形式で返却
# token(str)  : トークン
# --------------------------------------------------
def getPayLoad(token):
    # ピリオド単位で分割
    tokenArray = token.split(".")

    # 3分割でなければ終了
    if len(tokenArray) != 3:
        raise Exception("トークンのフォーマット不正")

    # ボディ部をBase64urlのByte型へ変換
    body = decodeBase64Url(tokenArray[1])

    # Byte→Str(UTF-8)へ変換
    bodyStr = body.decode("UTF-8")

    # json形式へパースして返却
    return json.loads(bodyStr.replace("'", "\""))

# --------------------------------------------------
# JWTを分解しボディ部（ペイロード）を取得
# token(str)  : トークン
# key(str)    : ペイロードのKey
# --------------------------------------------------
def getPayLoadKey(token, key="cognito:groups"):

    result = getPayLoad(token)

    if key in result:
        return result[key]
    else:
        raise Exception("Keyが存在しません。key:%s json:%s" % (key, result))

# --------------------------------------------------
# S3からオブジェクト取得
# bucketName(str)       : バケット名
# key(str)              : オブジェクト名
# regionName(str)       : リージョン名
# signatureVersion(str) : 署名付バージョン
# --------------------------------------------------
def getS3Object(bucketName, key, regionName="ap-northeast-1", signatureVersion="s3v4"):

    s3Config = Config(
            region_name = regionName,
            signature_version = signatureVersion
            )
    s3 = boto3.client('s3', config=s3Config)
    result = s3.get_object(Bucket=bucketName, Key=key)
    return result["Body"].read().decode("UTF-8")

# --------------------------------------------------
# 日付妥当性チェック(true:正常、false:異常）
# strTimeStamp(str)　: 日時文字列(yyyy-MM-dd HI:mm:ss)
# --------------------------------------------------
def validateTimeStamp(strTimeStamp):

    result = False
    try:
        # 文字列⇒日付変換で妥当性チェック
        datetime.datetime.strptime(strTimeStamp, '%Y-%m-%d %H:%M:%S.%f')
        result = True
    except ValueError:
        print('validate error (%s)' % strTimeStamp)

    return result


# --------------------------------------------------
# 浮動小数点数値チェック(true:正常、false:異常）
# value(obj)　: 入力値
# --------------------------------------------------
def isValidateFloat(value):

    result = False
    try:
        # float型へのキャストで妥当性チェック
        float(value)
        result = True
    except ValueError:
        print('validate error (%s)' % value)

    return result

# --------------------------------------------------
# 数値チェック(true:正常、false:異常）
# value(obj)　: 入力値
# --------------------------------------------------
def isValidateNumber(value):

    result = False
    try:
        # int型へのキャストで妥当性チェック
        int(value)
        result = True
    except ValueError:
        print('validate error (%s)' % value)

    return result


# --------------------------------------------------
# Boolean型チェック(true:正常、false:異常）
# value(obj)　: 入力値
# --------------------------------------------------
def isValidateBoolean(value):

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
        print('validate error (%s)' % value)

    return result

# --------------------------------------------------
# datetimeの変換関数
# 利用方法  json.dumps(hoge, default=json_serial)
# obj(obj)　: 入力値
# --------------------------------------------------
def json_serial(obj):
    # 日付型の場合には、文字列に変換します
    if isinstance(obj,datetime.datetime):
        # return obj.isoformat()
        
        return obj.strftime(JSON_DATETIME_FORMAT)
    # 上記以外はサポート対象外.
    raise TypeError ("Type %s not serializable" % type(obj))
