select
	me.X_COORDINATE as xCoordinate
    , me.Y_COORDINATE as yCoordinate
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
	%(p_whereParams)s
;

