insert
into `T_PUBLIC_TIMESERIES`(
    `DATA_COLLECTION_SEQ`
    , `RECEIVED_DATETIME`
    , `SENSOR_VALUE`
    , `CREATED_AT`
)
values (
      %(p_dataCollectionSeq)d
     ,'%(p_receivedDateTime)s'
     ,%(p_sensorValue)f
     ,'%(p_createdDateTime)s'
)
ON DUPLICATE KEY UPDATE
    `SENSOR_VALUE` = %(p_sensorValue)f
;
