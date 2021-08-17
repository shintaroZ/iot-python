SELECT
    setting,
    deviceid,
    requesttimestamp,
    record.sensorid,
    record.timestamp,
    record.VALUE
FROM
    %(databaseName)s.%(tableName)s,
    UNNEST(records) t(record)
WHERE
    requestTimeStamp BETWEEN CAST('%(startDateTime)s' AS TIMESTAMP)
AND CAST('%(endDateTime)s' AS TIMESTAMP)
;