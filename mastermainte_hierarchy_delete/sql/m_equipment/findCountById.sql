select
    count(me.EQUIPMENT_ID) as count
from
    M_EQUIPMENT me
where
    me.DELETE_FLG = 0
and (
        me.HIERARCHY_ID1 = '%(hierarchyId)s'
    or  me.HIERARCHY_ID2 = '%(hierarchyId)s'
    or  me.HIERARCHY_ID2 = '%(hierarchyId)s'
    )
and not exists(
        select
            1
        from
            M_EQUIPMENT meSub
        where
            me.EQUIPMENT_ID = meSub.EQUIPMENT_ID
        and me.VERSION < meSub.VERSION
    )
;