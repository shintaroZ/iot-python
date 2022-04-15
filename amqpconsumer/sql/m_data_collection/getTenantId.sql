select
    mdc.DEVICE_ID as tenantId
from
    M_DATA_COLLECTION mdc
where
    mdc.EDGE_TYPE = 2
    and  mdc.DELETE_FLG = 0 
group by
    mdc.DEVICE_ID
;