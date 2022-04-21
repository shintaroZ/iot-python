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
	createdt between %(startDateInt)d and %(endDateInt)d 
AND rMessages.requesttimestamp between CAST('%(startDateTime)s' as timestamp) and CAST('%(endDateTime)s' as timestamp) 
	%(whereParam)s
;