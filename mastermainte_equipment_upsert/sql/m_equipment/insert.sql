insert
into `M_EQUIPMENT`(
    `EQUIPMENT_ID`
    , `EQUIPMENT_NAME`
    , `DELETE_FLG`
    , `VERSION`
    %(insert_xCoordinate)s
    %(insert_yCoordinate)s
    , `CREATED_AT`
    , `UPDATED_USER`
)
values (
    '%(equipmentId)s'
    , '%(equipmentName)s'
    , 0
    , %(version)d
    %(values_xCoordinate)s
    %(values_yCoordinate)s
    , '%(createdAt)s'
    , '%(updatedUser)s'
);