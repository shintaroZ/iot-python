SELECT 
	"SUB"."deviceId"
	, "SUB"."requestTimeStamp"
	, "SUB"."sensorId"
	, "SUB"."timeStamp"
	, "SUB"."value"
FROM
(
	SELECT 
		"rMessages"."deviceId" as deviceId
		, "rMessages"."requestTimeStamp" as requestTimeStamp
		, "records"."sensorId" as sensorId
		, "records"."timeStamp" as timeStamp
		, "records"."value" as value
	FROM
	    %(databaseName)s.%(tableName)s
	CROSS JOIN UNNEST
		(receivedMessages) as t(rMessages)
	CROSS JOIN UNNEST
		(rMessages.records) as t(records)	
	WHERE
		"createdt" between %(startDateInt)d and %(endDateInt)d
    AND "rMessages"."requestTimeStamp" between CAST('%(startDateTime)s' as timestamp) and CAST('%(endDateTime)s' as timestamp) 
) SUB
WHERE
	%(whereParam)s
;