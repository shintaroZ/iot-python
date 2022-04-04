insert 
into `T_SOUNDFILE_HISTORY`( 
    `CREATED_DATETIME`
    , `EDGE_NAME`
    , `STATUS`
    , `FILE_NAME`
    , `CREATED_AT`
) 
values ( 
    '%(p_created_datetime)s'
    , '%(p_edge_name)s'
    , '%(p_status)s'
    , '%(p_file_name)s'
    , '%(p_created_at)s'
);
