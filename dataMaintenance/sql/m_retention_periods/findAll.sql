select
    TABLE_NAME as tableName
    ,PARTITION_COLUMN_NAME as partitionColumnName
    ,RETENTION_DAY_UNIT as retentionDayUnit
from
    M_RETENTION_PERIODS
;
