select
	me.VERSION as version
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
	%(p_whereParams)s
;

