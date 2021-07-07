insert into T_SURVEILLANCE
    (
     OCCURRED_DATETIME
     ,FUNCTION_NAME
     ,MESSAGE
     ,CREATED_AT
    )
values
     %(p_values)s
;