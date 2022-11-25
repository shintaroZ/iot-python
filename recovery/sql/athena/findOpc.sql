SELECT
	"SUB"."deviceId" as deviceId
	,"SUB"."requestTimeStamp" as requestTimeStamp
	,"SUB"."sensorId" as sensorId
	,cast("SUB"."ts" as timeStamp) as timeStamp
	,case 
	   when ("SUB"."integerValue") IS NOT NULL then cast("SUB"."integerValue" as varchar)
	   when ("SUB"."stringValue") IS NOT NULL then cast("SUB"."stringValue" as varchar)
	   when ("SUB"."booleanValue") IS NOT NULL then cast("SUB"."booleanValue" as varchar)
	   when ("SUB"."doubleValue") IS NOT NULL then cast("SUB"."doubleValue" as varchar)
	 end as value
FROM
	(
	SELECT 
	    "createdt"
	    ,"rMessages"."type" as type
	    ,"rMessages"."payload"."assetId" as deviceId
	    ,"rMessages"."payload"."propertyId" as sensorId
	    ,"vals"."timestamp"."timeInSeconds" as timeInSeconds
	    ,from_unixtime("vals"."timestamp"."timeInSeconds" + cast("vals"."timestamp"."offsetInNanos" as double) /1000000000, 'Asia/Tokyo') as ts
	    ,"vals"."quality" as quality
	    ,"vals"."value"."integerValue" as integerValue
	    ,"vals"."value"."stringValue" as stringValue
	    ,"vals"."value"."booleanValue" as booleanValue
	    ,"vals"."value"."doubleValue" as doubleValue
	    ,"requestTimeStamp"
	FROM backupdataopc
	CROSS JOIN UNNEST ("receivedMessages") as t(rMessages)
	CROSS JOIN UNNEST ("rMessages"."payload"."values") as t(vals)
	WHERE
		"createdt" between %(startDateInt)d and %(endDateInt)d
    AND "requestTimeStamp" between CAST('%(startDateTime)s' as timestamp) and CAST('%(endDateTime)s' as timestamp) 
	) SUB
WHERE
	%(whereParam)s

	
;