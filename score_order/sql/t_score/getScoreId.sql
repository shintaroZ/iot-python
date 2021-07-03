select
    M.EDGE_NAME as edgeName
    , MAX(SUCCESS.SCORE_ID) as latestScoreId
    , MIN(ERR.SCORE_ID) as oldErrScoreId 
from
    M_EDGE M
    left outer join T_SCORE SUCCESS 
        on SUCCESS.EDGE_NAME = M.EDGE_NAME 
        and SUCCESS.DETECTION_DATETIME >= '%(p_one_hour_ago_datetime)s'
        and SUCCESS.DETECTION_FLAG = 0 
        and not exists ( 
            select
                1 
            from
                T_SCORE SUCCESS_SUB 
            where
                SUCCESS.EDGE_NAME = SUCCESS_SUB.EDGE_NAME 
                and SUCCESS.DETECTION_FLAG = SUCCESS_SUB.DETECTION_FLAG 
                and SUCCESS.DETECTION_DATETIME < SUCCESS_SUB.DETECTION_DATETIME
        )
    left outer join T_SCORE ERR 
        on ERR.EDGE_NAME = M.EDGE_NAME
        and ERR.DETECTION_DATETIME >= '%(p_one_hour_ago_datetime)s'
        and ERR.DETECTION_FLAG = 1 
        and not exists ( 
            select
                1 
            from
                T_SCORE ERR_SUB 
            where
                ERR.EDGE_NAME = ERR_SUB.EDGE_NAME 
                and ERR.DETECTION_FLAG = ERR_SUB.DETECTION_FLAG 
                and ERR.DETECTION_DATETIME < ERR_SUB.DETECTION_DATETIME
        ) 
where
    M.EDGE_ID = '%(p_edge_id)s'