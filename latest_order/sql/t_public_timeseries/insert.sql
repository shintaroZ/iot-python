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
);
