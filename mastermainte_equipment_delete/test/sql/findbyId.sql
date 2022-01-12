select
	me.VERSION as version
    , me.DELETE_FLG as deleteFlg
    , me.X_COORDINATE as xCoordinate
    , me.Y_COORDINATE as yCoordinate
from
    M_EQUIPMENT me
where
	not exists (
		select
			1
		from
			M_EQUIPMENT meSub
		where
			me.EQUIPMENT_ID = meSub.EQUIPMENT_ID
		and me.VERSION < meSub.VERSION
	)
	and me.EQUIPMENT_ID = '%(equipmentId)s'
;

