update `M_EQUIPMENT` set
    `DELETE_FLG` = %(deleteFlg)d
    , `UPDATED_AT` = '%(updatedAt)s'
    , `UPDATED_USER` = '%(updatedUser)s'
where
    EQUIPMENT_ID = '%(equipmentId)s'
and DELETE_FLG = 0
;