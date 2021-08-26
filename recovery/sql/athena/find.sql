SELECT
    temp.deviceid,
    temp.requesttimestamp,
    record.sensorid,
    record.timestamp,
    record.VALUE
FROM
    %(databaseName)s.%(tableName)s as temp
CROSS JOIN UNNEST
	(records) as t(record)
WHERE
	%(whereParam)s
;