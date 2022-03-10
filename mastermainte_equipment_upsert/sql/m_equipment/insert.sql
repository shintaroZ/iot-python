insert
into `M_EQUIPMENT`(
    `EQUIPMENT_ID`
    , `EQUIPMENT_NAME`
    , `DELETE_FLG`
    , `VERSION`
    , `X_COORDINATE`
    , `Y_COORDINATE`
    , `CREATED_AT`
    , `UPDATED_USER`
)
values (
    '%(equipmentId)s'
    , '%(equipmentName)s'
    , 0
    , %(version)d
    , %(xCoordinate)s
    , %(yCoordinate)s
    , '%(createdAt)s'
    , '%(updatedUser)s'
);