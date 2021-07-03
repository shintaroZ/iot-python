import datetime

# --------------------------------------------------
# JSTの現在日時を返却する
# --------------------------------------------------
def getSysDateJst():
    utc = datetime.datetime.now(datetime.timezone.utc)
    jst = datetime.timezone(datetime.timedelta(hours=+9))
    return utc.astimezone(jst)
