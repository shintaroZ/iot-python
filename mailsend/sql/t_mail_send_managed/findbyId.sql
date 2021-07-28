select
    tmsm.DATA_COLLECTION_SEQ as dataCollectionSeq
    , tmsm.DETECTION_DATETIME as detectionDateTime
    , tmsm.LIMIT_SUB_NO as limitSubNo
    , tmsm.MAIL_SEND_SEQ as mailSendSeq
    , tpt.SENSOR_VALUE as sensorValue
    , mdc.DEVICE_ID as deviceId
    , mdc.SENSOR_ID as sensorId
    , mdc.SENSOR_NAME as sensorName
    , case
        when mdc.COLLECTION_VALUE_TYPE = 0
        then mdc.SENSOR_UNIT
        when mdc.COLLECTION_VALUE_TYPE = 1 and tpt.SENSOR_VALUE = 0
        then mdc.STATUS_FALSE
        else mdc.STATUS_TRUE
      end as unit
    , mlc.LIMIT_COUNT_TYPE as limitCountType
    , mlc.LIMIT_COUNT as limitCount
    , mlc.LIMIT_COUNT_RESET_RANGE as limitCountResetRange
    , mlc.ACTION_RANGE as actionRange
    , ml.LIMIT_JUDGE_TYPE as limitJudgeType
    , ml.LIMIT_VALUE as limitValue
--    , mms.sendTimeFromDt
--    , mms.sendTimeToDt

from
    T_MAIL_SEND_MANAGED tmsm
    inner join M_DATA_COLLECTION2 mdc
        on tmsm.DATA_COLLECTION_SEQ = mdc.DATA_COLLECTION_SEQ
        and mdc.DELETE_FLG = 0
    inner join T_PUBLIC_TIMESERIES tpt
        on tmsm.DETECTION_DATETIME = tpt.RECEIVED_DATETIME
        and tmsm.DATA_COLLECTION_SEQ = tpt.DATA_COLLECTION_SEQ
    inner join M_LIMIT_CHECK mlc
        on mlc.DATA_COLLECTION_SEQ = mdc.DATA_COLLECTION_SEQ
    inner join M_LIMIT ml
        on ml.LIMIT_CHECK_SEQ = mlc.LIMIT_CHECK_SEQ
        and ml.LIMIT_SUB_NO = tmsm.LIMIT_SUB_NO
    inner join (
        select
            mmsSub.MAIL_SEND_SEQ as mailSendSeq
            , mmsSub.DELETE_FLG as deleteFlg
            , mmsSub.VERSION as version
            , mmsSub.EMAIL_ADDRESS as emailAddress
            , mmsSub.SEND_WEEK_TYPE as sendWeekType
            , mmsSub.SEND_FREQUANCY as sendFrequancy
            , str_to_date(concat(DATE_FORMAT(CURRENT_DATE, '%%Y%%m%%d'), mmsSub.SEND_TIME_FROM), '%%Y%%m%%d%%k%%i%%s') as sendTimeFromDt
            , case
                when (mmsSub.SEND_TIME_FROM <= mmsSub.SEND_TIME_TO)
                then str_to_date(concat(DATE_FORMAT(CURRENT_DATE, '%%Y%%m%%d'), mmsSub.SEND_TIME_TO), '%%Y%%m%%d%%k%%i%%s')
                else str_to_date(concat(DATE_FORMAT(DATE_ADD(CURRENT_DATE, INTERVAL 1 DAY), '%%Y%%m%%d'), mmsSub.SEND_TIME_TO), '%%Y%%m%%d%%k%%i%%s')
              end as sendTimeToDt
            , mmsSub.MAIL_SUBJECT as mailSubject
            , mmsSub.MAIL_TEXT_HEADER as mailTextHeader
            , mmsSub.MAIL_TEXT_BODY as mailTextBody
            , mmsSub.MAIL_TEXT_FOOTER as mailTextFooter
        from
            M_MAIL_SEND2 mmsSub
    ) mms
        on tmsm.MAIL_SEND_SEQ = mms.mailSendSeq
     and mms.deleteFlg = 0
     and now() between mms.sendTimeFromDt and mms.sendTimeToDt
     and not exists(
        select
            1
        from
            M_PUBLIC_HOLIDAY mph
        where
            mms.sendWeekType = 1
        and CURRENT_DATE = mph.PUBLIC_HOLIDAY_DATE
     )
order by
    tmsm.DETECTION_DATETIME
    , tmsm.DATA_COLLECTION_SEQ;

