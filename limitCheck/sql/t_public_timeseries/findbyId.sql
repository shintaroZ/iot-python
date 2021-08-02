select
    tpt.DATA_COLLECTION_SEQ as dataCollectionSeq
    , tpt.RECEIVED_DATETIME as receivedDatetime
    , tpt.SENSOR_VALUE as sensorValue
    , tpt.CREATED_AT as createdAt

    , mlc.LIMIT_COUNT_TYPE as limitCountType
    , mlc.LIMIT_COUNT as limitCount
    , mlc.LIMIT_COUNT_RESET_RANGE as limitCountResetRange
    , mlc.ACTION_RANGE as actionRange
    , mlc.NEXT_ACTION as nextAction

    , ml.LIMIT_SUB_NO as limitSubNo
    , ml.LIMIT_JUDGE_TYPE as limitJudgeType
    , ml.LIMIT_VALUE as limitValue
from
    T_PUBLIC_TIMESERIES tpt
    inner join M_DATA_COLLECTION mdc
        on tpt.DATA_COLLECTION_SEQ = mdc.DATA_COLLECTION_SEQ
    inner join M_LINK_FLG mlf
        on tpt.DATA_COLLECTION_SEQ = mlf.DATA_COLLECTION_SEQ
        and mlf.LIMIT_CHECK_FLG = 1
    inner join M_LIMIT_CHECK mlc
        on tpt.DATA_COLLECTION_SEQ = mlc.DATA_COLLECTION_SEQ
    inner join M_LIMIT ml
        on tpt.DATA_COLLECTION_SEQ = ml.DATA_COLLECTION_SEQ
    left outer join T_LIMIT_CHECK_TEMP tlct
        on tpt.DATA_COLLECTION_SEQ = tlct.DATA_COLLECTION_SEQ

where
    ifnull(tlct.LIMIT_CHECK_START_DATE , DATE_FORMAT('19000101000000', '%%Y%%m%%d%%k%%i%%s') ) < tpt.RECEIVED_DATETIME
order by
    tpt.DATA_COLLECTION_SEQ
    , tpt.RECEIVED_DATETIME;
