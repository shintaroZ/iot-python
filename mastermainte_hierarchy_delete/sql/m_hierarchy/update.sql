update `M_HIERARCHY` set
    `DELETE_FLG` = %(deleteFlg)d
    , `UPDATED_AT` = '%(updatedAt)s'
    , `UPDATED_USER` = '%(updatedUser)s'
where
    HIERARCHY_ID = '%(hierarchyId)s'
and DELETE_FLG = 0
;