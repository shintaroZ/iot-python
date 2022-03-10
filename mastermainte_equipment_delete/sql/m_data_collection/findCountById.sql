select
    count(m.EQUIPMENT_ID) as count
from
    M_DATA_COLLECTION m 
where
    m.DELETE_FLG = 0
and m.EQUIPMENT_ID = '%(equipmentId)s'
and not exists ( 
        select
            1 
        from
            M_DATA_COLLECTION mdcSub 
        where
            m.DEVICE_ID = mdcSub.DEVICE_ID 
            and m.SENSOR_ID = mdcSub.SENSOR_ID 
            and m.VERSION < mdcSub.VERSION
    )
;