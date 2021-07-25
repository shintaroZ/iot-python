select
    tmsm.DATA_COLLECTION_SEQ as dataCollectionSeq
    , tmsm.DETECTION_DATETIME as detectionDateTime
    , tmsm.LIMIT_SUB_NO as limitSubNo
    , tmsm.MAIL_SEND_ID as mailSendId
    , tmsm.SEND_STATUS as sendStatus
    , mdc.DEVICE_ID as deviceId
    , mdc.SENSOR_ID as sensorId
    , mdc.SENSOR_NAME as sensorName
    , mdc.SENSOR_UNIT as sensorUnit
    , mdc.STATUS_TRUE as statusTrue
    , mdc.STATUS_FALSE as statusFalse
    , mdc.COLLECTION_VALUE_TYPE as collectionValueType
    , mdc.COLLECTION_TYPE as collectionType
    , mdc.REVISION_MAGNIFICATION as revisionMagnification
    , mdc.X_COORDINATE as xCoordinate
    , mdc.Y_COORDINATE as yCoordinate
    , mlf.SAVING_FLG as savingFlg
    , mlf.LIMIT_CHECK_FLG as limitCheckFlg
    , mlc.LIMIT_CHECK_SEQ as limitCheckSeq
    , mlc.LIMIT_COUNT_TYPE as limitCountType
    , mlc.LIMIT_COUNT as limitCount
    , mlc.LIMIT_COUNT_RESET_RANGE as limitCountResetRange
    , mlc.ACTION_RANGE as actionRange
    , mlc.NEXT_ACTION as nextAction
    , ml.LIMIT_JUDGE_TYPE as limitJudgeType
    , ml.LIMIT_VALUE as limitValue
    , tpt.SENSOR_VALUE as sensorValue
from
    T_MAIL_SEND_MANAGED tmsm
    inner join M_DATA_COLLECTION mdc
        on tmsm.DATA_COLLECTION_SEQ = mdc.DATA_COLLECTION_SEQ
    inner join M_LINK_FLG mlf
        on mdc.DATA_COLLECTION_SEQ = mlf.DATA_COLLECTION_SEQ
    inner join T_PUBLIC_TIMESERIES tpt
        on tmsm.DETECTION_DATETIME = tpt.RECEIVED_DATETIME
        and tmsm.DATA_COLLECTION_SEQ = tpt.DATA_COLLECTION_SEQ
    inner join M_LIMIT_CHECK mlc
        on mlc.DATA_COLLECTION_SEQ = mdc.DATA_COLLECTION_SEQ
    inner join M_LIMIT ml
        on ml.LIMIT_CHECK_SEQ = mlc.LIMIT_CHECK_SEQ
        and ml.LIMIT_SUB_NO = tmsm.LIMIT_SUB_NO
where
    mdc.DELETE_COUNT = 0
and tmsm.MAIL_SEND_ID = %(mailSendId)d
order by
    tmsm.DETECTION_DATETIME
    , tmsm.DATA_COLLECTION_SEQ;
