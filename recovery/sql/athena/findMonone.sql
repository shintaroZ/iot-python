SELECT 
	rMessages.deviceid
	, rMessages.requesttimestamp
	, records.tenantId
	, records.score
FROM
    %(databaseName)s.%(tableName)s
CROSS JOIN UNNEST
	(receivedMessages) as t(rMessages)
CROSS JOIN UNNEST
	(rMessages.records) as t(records)	
WHERE
	createdt between %(startDateInt)d and %(endDateInt)d 
AND rMessages.requesttimestamp between CAST('%(startDateTime)s' as timestamp) and CAST('%(endDateTime)s' as timestamp) 
AND rMessages.queue = 'Last_Score'
    %(whereParam)s
;