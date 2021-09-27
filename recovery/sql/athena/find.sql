SELECT 
	rMessages.deviceid
	, rMessages.requesttimestamp
	, records.sensorid
	, records.timestamp
	, records.value
FROM
    %(databaseName)s.%(tableName)s
CROSS JOIN UNNEST
	(receivedMessages) as t(rMessages)
CROSS JOIN UNNEST
	(rMessages.records) as t(records)	
WHERE
	%(whereParam)s
;