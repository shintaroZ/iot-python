select
    EDGE_TYPE as edgeType
    , COLLECTION_TYPE as collectionType
    , COLLECTION_TYPE_NAME as collectionTypeName
    , DISP_ORDER as dispOrder
    , DATE_FORMAT(CREATED_AT, '%%Y/%%m/%%d %%H:%%i:%%s') as createdAt
from
    M_COLLECTION_TYPE
order by
	EDGE_TYPE
	, COLLECTION_TYPE
;