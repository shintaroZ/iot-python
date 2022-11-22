select
	mh.VERSION as version
from
    M_HIERARCHY mh
where
	not exists (
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

