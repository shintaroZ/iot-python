SELECT
    PARTITION_NAME as partitionKey
FROM
    INFORMATION_SCHEMA.PARTITIONS 
WHERE
    TABLE_SCHEMA = '%(p_schemaName)s' 
AND TABLE_NAME = '%(p_tableName)s'
;
