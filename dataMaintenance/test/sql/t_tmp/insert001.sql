insert
into `TMP_TEMPERATURE`(
    `RECEIVED_DATETIME`
    , `SENSOR_NAME`
    , `SENSOR_VALUE`
    , `CREATED_AT`
    , `UPDATED_AT`
    , `VERSION`
)
values (
    '%(receivedDatetime)s'
    , 'testセンサ'
    , 61.58
    , sysdate()
    , null
    , 0
);
