insert ignore
into `T_PUBLIC_TIMESERIES`(
    `DATA_COLLECTION_SEQ`
    , `RECEIVED_DATETIME`
    , `SENSOR_VALUE`
    , `CREATED_AT`
)
values
     %(p_values)s
;
