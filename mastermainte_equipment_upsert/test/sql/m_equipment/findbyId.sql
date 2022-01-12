select
    me.EQUIPMENT_ID as equipmentId
    , me.EQUIPMENT_NAME as equipmentName
    , me.X_COORDINATE as xCoordinate
    , me.Y_COORDINATE as yCoordinate
    , me.VERSION as version
    , me.DELETE_FLG as deleteFlg
from
    M_EQUIPMENT me
where
	me.DELETE_FLG = 0
and not exists ( 
        select
            1 
        from
            M_EQUIPMENT meSub 
        where
            me.EQUIPMENT_ID = meSub.EQUIPMENT_ID 
            and me.VERSION < meSub.VERSION
    ) 
;

