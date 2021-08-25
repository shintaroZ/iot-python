SELECT
    deviceid,
    requesttimestamp,
    record.sensorid,
    record.timestamp,
    record.VALUE
FROM
    %(databaseName)s.%(tableName)s
CROSS JOIN UNNEST
	(records) as t(record)
WHERE
	createdt between %(startDate)d and %(endDate)d
AND requestTimeStamp BETWEEN CAST('%(startDateTime)s' AS TIMESTAMP)
AND CAST('%(endDateTime)s' AS TIMESTAMP)
;