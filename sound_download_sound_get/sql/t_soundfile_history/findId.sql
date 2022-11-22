SELECT
     extract.DATA_COLLECTION_SEQ AS dataCollectionSeq
    ,DATE_FORMAT(extract.CREATED_DATETIME, '%%Y/%%m/%%d %%H:%%i:%%s') AS createdDateTime
    ,extract.FILE_TYPE AS fileType
    ,extract.FILE_NAME AS fileName
    ,DATE_FORMAT(extract.CREATED_AT, '%%Y/%%m/%%d %%H:%%i:%%s') AS createdAt
    ,DATE_FORMAT(extract.CREATED_DATETIME + INTERVAL extract.sec_size SECOND, '%%Y/%%m/%%d %%H:%%i:%%s') AS endDateTime
FROM
    (
         SELECT
             pre_extract.*
            ,TIME_TO_SEC(STR_TO_DATE(LEFT(pre_extract.pre_time, INSTR(pre_extract.pre_time, '.') - 1), '%%i:%%s')) AS sec_size
        FROM
            (
                 SELECT
                     T_SOUNDFILE_HISTORY.*
                    ,RIGHT(FILE_NAME, INSTR(REVERSE(FILE_NAME), REVERSE('_')) - 1) pre_time
                FROM
                    T_SOUNDFILE_HISTORY
                WHERE
                    FILE_TYPE = 3
            ) pre_extract
    ) extract
    %(p_whereParams)s
;