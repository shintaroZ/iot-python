select
    mdc.EDGE_TYPE as edgeType
    , mdc.EQUIPMENT_ID as equipmentId
    , mlc.DATA_COLLECTION_SEQ as dataCollectionSeq
    , mlc.LIMIT_COUNT_TYPE as limitCountType
    , mlc.LIMIT_COUNT as limitCount
    , mlc.LIMIT_COUNT_RESET_RANGE as limitCountResetRange
    , mlc.ACTION_RANGE as actionRange
    , mlc.NEXT_ACTION as nextAction
    , ml.LIMIT_SUB_NO as limitSubNo
    , ml.LIMIT_JUDGE_TYPE as limitJudgeType
    , ml.LIMIT_VALUE as limitValue
from
	M_DATA_COLLECTION mdc 
    inner join M_LIMIT_CHECK mlc
        on mlc.DATA_COLLECTION_SEQ = mdc.DATA_COLLECTION_SEQ
    inner join M_LIMIT ml
        on ml.DATA_COLLECTION_SEQ = mdc.DATA_COLLECTION_SEQ
where
    mdc.DATA_COLLECTION_SEQ = %(dataCollectionSeq)d
;