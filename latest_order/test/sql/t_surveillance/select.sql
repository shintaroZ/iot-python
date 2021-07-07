select
    OCCURRED_DATETIME as occurredDateTime
    ,FUNCTION_NAME as functionName
    ,MESSAGE as message
    ,CREATED_AT as createdDateTime
    ,UPDATED_AT as updateDateTime
    ,VERSION as version
from
    T_SURVEILLANCE
where
    OCCURRED_DATETIME between '%(p_occurredDateTimeStart)s' and '%(p_occurredDateTimeEnd)s'
order by
    OCCURRED_DATETIME
;