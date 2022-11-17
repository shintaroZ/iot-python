insert 
into `M_RETENTION_PERIODS` ( 
    `TABLE_NAME`
    , `PARTITION_COLUMN_NAME`
    , `RETENTION_DAY_UNIT`
    , `CREATED_AT`
    , `UPDATED_AT`
    , `VERSION`
) 
values ( 
    'TMP_TEMPERATURE'
    , 'RECEIVED_DATETIME'
    , 5
    , sysdate()
    , null
    , 0
) 
ON DUPLICATE KEY UPDATE 
    `PARTITION_COLUMN_NAME` = 'RECEIVED_DATETIME'
    , `RETENTION_DAY_UNIT` = 5
    , `UPDATED_AT` = CURRENT_TIMESTAMP
    , `VERSION` = `VERSION` + 1
;