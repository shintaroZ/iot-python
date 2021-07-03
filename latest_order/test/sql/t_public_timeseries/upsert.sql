insert
into `T_PUBLIC_TIMESERIES`(
    `COLLECTION_TYPE`
    , `RECEIVED_DATETIME`
    , `SENSOR_NAME`
    , `SENSOR_VALUE`
    , `SENSOR_UNIT`
    , `CREATED_AT`
    , `UPDATED_AT`
    , `VERSION`
)
values (
      '%(p_collectionType)s'
     ,'%(p_receivedDateTime)s'
     ,'%(p_sensorName)s'
     ,%(p_sensorValue)f
     ,'%(p_sensorUnit)s'
     ,'%(p_createdDateTime)s'
    , null
    , 0
)
ON DUPLICATE KEY UPDATE
    `SENSOR_NAME` = '%(p_sensorName)s'
    ,`SENSOR_VALUE` = %(p_sensorValue)f
    ,`SENSOR_UNIT` = '%(p_sensorUnit)s'
    ,`UPDATED_AT` = '%(p_createdDateTime)s'
    ,`VERSION` = `VERSION` + 1
;
