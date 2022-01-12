select
    count(*) as count
from
    M_DATA_COLLECTION 
where
    EQUIPMENT_ID = '%(equipmentId)s'
and DELETE_FLG = 0
;