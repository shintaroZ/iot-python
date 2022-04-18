insert ignore
into `T_SCORE`( 
    `DATA_COLLECTION_SEQ`
    , `DETECTION_DATETIME`
    , `DETECTION_DATE`
    , `DETECTION_TIME`
    , `DETECTION_FLAG`
    , `DETECTION_MIN`
    , `DETECTION_MAX`
    , `DETECTION_VALUE`
    , `DETECTION_THRESHOLD`
    , `SLIDING_UPPER`
    , `SLIDING_LOWER`
    , `CREATED_AT`
) 
values  
     %(p_values)s
;