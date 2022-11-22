SELECT 
	"SUB"."deviceId"
	, "SUB"."requestTimeStamp"
	, "SUB"."tenantId"
	, "SUB"."score"
FROM
(
	SELECT 
		"rMessages"."deviceId" as deviceId
		, "rMessages"."queue" as queue
		, "rMessages"."requestTimeStamp" as requestTimeStamp
		, "records"."tenantId" as tenantId
		, "records"."score" as score
	FROM
	    %(databaseName)s.%(tableName)s
	CROSS JOIN UNNEST
		(receivedMessages) as t(rMessages)
	CROSS JOIN UNNEST
		(rMessages.records) as t(records)	
	WHERE
		"createdt" between %(startDateInt)d and %(endDateInt)d
    AND "rMessages"."requestTimeStamp" between CAST('%(startDateTime)s' as timestamp) and CAST('%(endDateTime)s' as timestamp) 
    AND "rMessages"."queue" = 'Last_Score'
) SUB
WHERE
    %(whereParam)s
;