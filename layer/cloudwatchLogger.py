import datetime
import logging

# --------------------------------------------------
# JSTのtime.struct_timeを返却する
# --------------------------------------------------
def customTime(*args):
    utc = datetime.datetime.now(datetime.timezone.utc)
    jst = datetime.timezone(datetime.timedelta(hours=+9))
    return utc.astimezone(jst).timetuple()

# --------------------------------------------------
# ロガー初期設定
# --------------------------------------------------
def initLogger(loglevel):
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
