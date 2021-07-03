select
    EDGE_NAME as edgeName
from
    M_EDGE
where
    EDGE_ID = '%(p_edge_id)s'