SELECT
    temp.receivedMessages.deviceId,
    temp.receivedMessages.requestTimeStamp,
    record.sensorid,
    record.timestamp,
    record.VALUE
FROM
    %(databaseName)s.%(tableName)s as temp
CROSS JOIN UNNEST
	(temp.receivedMessages.records) as t(record)
WHERE
	%(whereParam)s
;