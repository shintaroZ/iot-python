insert 
into `T_EQUIPMENT_STATUS` ( 
    `EDGE_NAME`
    , `SCORE_ID`
    , `EQUIPMENT_STATUS`
    , `CREATED_AT`
    , `UPDATED_AT`
    , `VERSION`
) 
values ( 
    '%(p_edge_name)s'
    , %(p_score_id)d
    , %(p_equipment_status)d
    , '%(p_register_at)s'
    , null
    , 0
) 
ON DUPLICATE KEY UPDATE 
    `SCORE_ID` = %(p_score_id)d
    , `EQUIPMENT_STATUS` = %(p_equipment_status)d
    , `UPDATED_AT` = '%(p_register_at)s'
    , `VERSION` = `VERSION` + 1
;