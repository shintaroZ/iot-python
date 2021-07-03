insert into T_SURVEILLANCE
    (
     OCCURRED_DATETIME
     ,FUNCTION_NAME
     ,MESSAGE
     ,CREATED_AT
    )
values
    (
     '%(p_occurredDateTime)s'
     ,'%(p_functionName)s'
     ,'%(p_message)s'
     ,'%(p_createdDateTime)s'
    )
;