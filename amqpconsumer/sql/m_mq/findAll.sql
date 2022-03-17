select
    QUEUE_NAME as queueName
from
    M_MQ
where
    DATA_TYPE = '%(p_dataType)s' 
