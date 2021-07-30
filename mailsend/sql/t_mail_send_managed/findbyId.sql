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

from
    T_MAIL_SEND_MANAGED tmsm
    inner join M_DATA_COLLECTION mdc
        on tmsm.DATA_COLLECTION_SEQ = mdc.DATA_COLLECTION_SEQ
        and mdc.DELETE_FLG = 0
    inner join T_PUBLIC_TIMESERIES tpt
        on tmsm.DETECTION_DATETIME = tpt.RECEIVED_DATETIME
        and tmsm.DATA_COLLECTION_SEQ = tpt.DATA_COLLECTION_SEQ
    inner join M_LIMIT_CHECK mlc
        on mlc.DATA_COLLECTION_SEQ = mdc.DATA_COLLECTION_SEQ
    inner join M_LIMIT ml
        on ml.DATA_COLLECTION_SEQ = mlc.DATA_COLLECTION_SEQ
        and ml.LIMIT_SUB_NO = tmsm.LIMIT_SUB_NO
where
	tmsm.MAIL_SEND_SEQ = %(mailSendSeq)d
order by
    tmsm.DETECTION_DATETIME
    , tmsm.DATA_COLLECTION_SEQ
;
