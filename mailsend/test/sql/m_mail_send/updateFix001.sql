update M_MAIL_SEND mms
set
    mms.EMAIL_ADDRESS = 's_otoi@matisse.co.jp'
    , mms.SEND_WEEK_TYPE = 1
    , mms.SEND_FREQUANCY = 0
    , mms.SEND_TIME_FROM = '070000'
    , mms.SEND_TIME_TO = '230000'
    , mms.MAIL_SUBJECT = '閾値mailSubjectUnitTest'
    , mms.MAIL_TEXT = concat(
        '#HEADER#\r\n'
        , '閾値異常を検知いたしました。\r\n'
        , '=============================\r\n'
        , '#END#\r\n'
        , '$limitJudgeTypeStr=\'より超過しています\' if record[\'limitJudgeType\'] == 0 else \'と一致しています\' if record[\'limitJudgeType\'] == 1 else \'より下回っています\'$\r\n'
        , '【検知日時】%record[\'detectionDateTime\']%\r\n'
        , '【デバイスID】%record[\'deviceId\']%\r\n'
        , '【センサID】%record[\'sensorId\']%\r\n'
        , '【センサ名】%record[\'sensorName\']%\r\n'
        , '【値】%record[\'sensorValue\']%%record[\'sensorUnit\']%\r\n'
        , '【閾値】%record[\'limitValue\']%%limitJudgeTypeStr%\r\n'
        , '\r\n'
        , '#FOOTER#\r\n'
        , '=============================\r\n'
        , '※この電子メールは、送信専用メールアドレスよりお送りしています。メールの返信は受け付けておりません。予めご了承下さい。\r\n'
        , '#END#'
    )
    , mms.UPDATED_AT = CURRENT_TIMESTAMP
    , mms.UPDATED_USER = ' unitTest '
    , mms.VERSION = mms.VERSION + 1
where
    MAIL_SEND_ID = 1
    and DELETE_COUNT = 0
