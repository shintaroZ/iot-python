insert
into `M_HIERARCHY`( 
    `HIERARCHY_ID`
    , `VERSION`
    , `DELETE_FLG`
    , `HIERARCHY_NAME`
    , `HIERARCHY_LEVEL`
    , `CREATED_AT`
    , `UPDATED_AT`
    , `UPDATED_USER`
) 
values ( 
    'TH0001'
    , 0
    , '0'
    , '階層deltest1'
    , 1
    , '2022/01/07 22:00:00'
    , null
    , 'devuser'
)

ON DUPLICATE KEY UPDATE
    HIERARCHY_ID = values(HIERARCHY_ID)
    , VERSION = values(VERSION)