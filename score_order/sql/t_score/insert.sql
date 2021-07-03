insert 
into `T_SCORE`( 
    `EDGE_NAME`
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
values ( 
    '%(p_edge_name)s'
    , '%(p_detection_datetime)s'
    , '%(p_detection_date)s'
    , '%(p_detection_time)s'
    , '%(p_detection_flag)s'
    , %(p_detection_min)f
    , %(p_detection_max)f
    , %(p_detection_value)f
    , %(p_detection_threshold)f
    , %(p_sliding_upper)f
    , %(p_sliding_lower)f
    , '%(p_created_at)s'
);
