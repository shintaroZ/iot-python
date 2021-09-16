select
    mlc.DATA_COLLECTION_SEQ as dataCollectionSeq
    , mlc.LIMIT_COUNT_TYPE as limitCountType
    , mlc.LIMIT_COUNT as limitCount
    , mlc.LIMIT_COUNT_RESET_RANGE as limitCountResetRange
    , mlc.ACTION_RANGE as actionRange
    , mlc.NEXT_ACTION as nextAction
    , ml.LIMIT_SUB_NO as limitSubNo
    , ml.LIMIT_JUDGE_TYPE as limitJudgeType
    , ml.LIMIT_VALUE as limitValue
from
    M_LIMIT_CHECK mlc
    inner join M_LIMIT ml
        on mlc.DATA_COLLECTION_SEQ = ml.DATA_COLLECTION_SEQ
where
    mlc.DATA_COLLECTION_SEQ = %(dataCollectionSeq)d
;