/* 設備ID あり */
insert
into `M_DATA_COLLECTION`(
    `DATA_COLLECTION_SEQ`
    , `EQUIPMENT_ID`
    , `DEVICE_ID`
    , `SENSOR_ID`
    , `DELETE_FLG`
    , `VERSION`
    , `SENSOR_NAME`
    , `SENSOR_UNIT`
    , `STATUS_TRUE`
    , `STATUS_FALSE`
    , `COLLECTION_VALUE_TYPE`
    , `COLLECTION_TYPE`
    , `REVISION_MAGNIFICATION`
    , `CREATED_AT`
    , `UPDATED_AT`
    , `UPDATED_USER`
)
values (
    0
    , 'X0001'
    , 'UT_deviceId001'
    , 'sss001'
    , '0'
    , 0
    , '温度testSensor'
    , '℃'
    , ''
    , ''
    , '0'
    , 1
    , 0.01
    , '2021/07/28 16:40:25'
    , null
    , 'devUser'
)
ON DUPLICATE KEY UPDATE
    EQUIPMENT_ID = values(EQUIPMENT_ID)

;