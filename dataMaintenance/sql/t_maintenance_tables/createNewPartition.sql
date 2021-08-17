ALTER TABLE %(p_tableName)s
PARTITION BY RANGE (TO_DAYS(%(p_partitionColumnName)s)) (
%(p_partitionStr)s
);
