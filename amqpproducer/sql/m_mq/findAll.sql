select
    EXCHANGE_NAME as exchangeName
    , EXCHANGE_TYPE as exchangeType
from
    M_MQ
where
    DATA_TYPE = '%(p_dataType)s' 
and QUEUE_NAME REGEXP '%(p_regexpTenantId)s'
