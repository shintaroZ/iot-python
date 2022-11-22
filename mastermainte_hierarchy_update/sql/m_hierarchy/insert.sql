insert
into `M_HIERARCHY`(
    `HIERARCHY_ID`
    , `HIERARCHY_NAME`
    , `HIERARCHY_LEVEL`
    , `DELETE_FLG`
    , `VERSION`
    , `CREATED_AT`
    , `UPDATED_USER`
)
values (
    '%(hierarchyId)s'
    , '%(hierarchyName)s'
    , %(hierarchyLevel)d
    , 0
    , %(version)d
    , '%(createdAt)s'
    , '%(updatedUser)s'
);