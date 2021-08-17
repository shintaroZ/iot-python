select
    DATE_FORMAT(DATE_SUB(%(p_partitionColumnName)s, INTERVAL 1 DAY), 'p%%Y%%m%%d') as partitionKey
    , cast(%(p_partitionColumnName)s as date) as partitionDate
from
    %(p_tableName)s
group by
    partitionKey
;