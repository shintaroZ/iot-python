insert
into `M_EQUIPMENT`( 
    `EQUIPMENT_ID`
    , `VERSION`
    , `DELETE_FLG`
    , `EQUIPMENT_NAME`
    , `X_COORDINATE`
    , `Y_COORDINATE`
    , `HIERARCHY_ID1`
    , `HIERARCHY_ID2`
    , `HIERARCHY_ID3`
    , `CREATED_AT`
    , `UPDATED_AT`
    , `UPDATED_USER`
) 
values ( 
    'X0001'
    , 0
    , '0'
    , '設備名deltest1'
    , 111.22
    , 222.33
    , 'TH0001'
    , ''
    , ''
    , '2022/01/07 22:00:00'
    , null
    , 'devuser'
)

ON DUPLICATE KEY UPDATE
    EQUIPMENT_ID = values(EQUIPMENT_ID)
    , VERSION = values(VERSION)
