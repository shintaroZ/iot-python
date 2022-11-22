select
    count(*) savingFlg
from
    M_DATA_COLLECTION mdc 
    inner join M_LINK_FLG mlf 
        on mlf.DATA_COLLECTION_SEQ = mdc.DATA_COLLECTION_SEQ 
where
    mdc.DELETE_FLG = 0 
    and not exists ( 
        select
            1 
        from
            M_DATA_COLLECTION mdcSub 
        where
            mdc.DEVICE_ID = mdcSub.DEVICE_ID 
            and mdc.SENSOR_ID = mdcSub.SENSOR_ID 
            and mdc.VERSION < mdcSub.VERSION
    ) 
    %(p_whereParams)s
    AND mdc.SENSOR_ID = "mike"
    AND mdc.EDGE_TYPE = 3
    AND mlf.SAVING_FLG = 1
order by
    mdc.DEVICE_ID
    , mdc.SENSOR_ID
;
