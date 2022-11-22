select
    mh.HIERARCHY_ID as hierarchyId
    , mh.HIERARCHY_NAME as hierarchyName
    , mh.HIERARCHY_LEVEL as hierarchyLevel
    , mh.VERSION as version
    , mh.DELETE_FLG as deleteFlg
from
    M_HIERARCHY mh
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
;

