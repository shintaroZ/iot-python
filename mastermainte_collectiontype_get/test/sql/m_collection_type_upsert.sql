insert 
into `M_COLLECTION_TYPE` ( 
    `EDGE_TYPE`
    , `COLLECTION_TYPE`
    , `COLLECTION_TYPE_NAME`
    , `DISP_ORDER`
    , `CREATED_AT`
) 
values 
  ('1', '1', '温度', '1', '2022/04/08 15:29:09')
, ('1', '2', '湿度', '2', '2022/04/08 15:29:09')
, ('1', '3', '音'  , '3', '2022/04/08 15:29:09')
, ('1', '4', '漏水', '4'    , '2022/04/08 15:29:09')
, ('1', '5', '照度', '5'    , '2022/04/08 15:29:09')
, ('1', '6', '二酸化炭素', '6', '2022/04/08 15:29:09')
, ('1', '7', '臭気', '7'    , '2022/04/08 15:29:09')
, ('2', '1', '音（スコア）', '1', '2022/04/08 15:29:09') 
ON DUPLICATE KEY UPDATE
    `EDGE_TYPE` = values(EDGE_TYPE)
    , `COLLECTION_TYPE` = values(COLLECTION_TYPE)
    , `COLLECTION_TYPE_NAME` = values(COLLECTION_TYPE_NAME)
    , `DISP_ORDER` = values(DISP_ORDER)
    , `CREATED_AT` = values(CREATED_AT)
;