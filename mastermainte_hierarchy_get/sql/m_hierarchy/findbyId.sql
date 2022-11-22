select
    mh.HIERARCHY_ID as hierarchyId
    , mh.HIERARCHY_NAME as hierarchyName
    , mh.HIERARCHY_LEVEL as hierarchyLevel
    , me.EQUIPMENT_ID as equipmentId
    , me.EQUIPMENT_NAME as equipmentName
    , DATE_FORMAT(mh.CREATED_AT, '%%Y/%%m/%%d %%H:%%i:%%s') as createdAt
    , ifnull( DATE_FORMAT(mh.UPDATED_AT, '%%Y/%%m/%%d %%H:%%i:%%s'), '') as updatedAt
    , ifnull(mh.UPDATED_USER, '') as updatedUser
    , mh.VERSION as version 
from
    M_HIERARCHY mh
    left outer join (
        select
            m.EQUIPMENT_ID
            , m.EQUIPMENT_NAME
            , m.HIERARCHY_ID1
            , m.HIERARCHY_ID2
            , m.HIERARCHY_ID3
        from
            M_EQUIPMENT m
        where
            m.DELETE_FLG = 0 
            and not exists ( 
                select
                    1 
                from
                    M_EQUIPMENT meSub 
                where
                    m.EQUIPMENT_ID = meSub.EQUIPMENT_ID 
                    and m.VERSION < meSub.VERSION
            )
    ) me
    on mh.HIERARCHY_LEVEL = 1 and mh.HIERARCHY_ID = me.HIERARCHY_ID1
    or mh.HIERARCHY_LEVEL = 2 and mh.HIERARCHY_ID = me.HIERARCHY_ID2
    or mh.HIERARCHY_LEVEL = 3 and mh.HIERARCHY_ID = me.HIERARCHY_ID3
where
    mh.DELETE_FLG = 0
    and not exists ( 
        select
            1 
        from
            M_HIERARCHY mhSub 
        where
            mh.HIERARCHY_ID = mhSub.HIERARCHY_ID 
            and mh.VERSION < mhSub.VERSION
    ) 
    %(p_whereParams)s
order by
    mh.HIERARCHY_LEVEL
    , mh.HIERARCHY_ID
;

