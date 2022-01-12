insert
into `M_EQUIPMENT`( 
    `EQUIPMENT_ID`
    , `VERSION`
    , `DELETE_FLG`
    , `EQUIPMENT_NAME`
    , `X_COORDINATE`
    , `Y_COORDINATE`
    , `CREATED_AT`
    , `UPDATED_AT`
    , `UPDATED_USER`
) 
values ( 
    'X0001'
    , 1
    , '0'
    , '設備名deltest2'
    , 111.22
    , 222.33
    , '2022/01/07 22:00:00'
    , null
    , 'devuser'
)

ON DUPLICATE KEY UPDATE
    EQUIPMENT_ID = values(EQUIPMENT_ID)
    , VERSION = values(VERSION)
