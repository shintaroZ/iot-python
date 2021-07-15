select
    mdc.DEVICE_ID as deviceId
    , mdc.SENSOR_ID as sensorId
    , mdc.DATA_COLLECTION_SEQ as dataCollectionSeq
    , mdc.SENSOR_NAME as sensorName
    , ifnull(mdc.SENSOR_UNIT, '') as sensorUnit
    , ifnull(mdc.STATUS_TRUE, '') as statusTrue
    , ifnull(mdc.STATUS_FALSE, '') as statusFalse
    , mdc.COLLECTION_VALUE_TYPE as collectionValueType
    , mdc.COLLECTION_TYPE as collectionType
    , mdc.REVISION_MAGNIFICATION as revisionMagnification
    , mdc.X_COORDINATE as xCoordinate
    , mdc.Y_COORDINATE as yCoordinate
    , mlf.SAVING_FLG as savingFlg
    , mlf.LIMIT_CHECK_FLG as limitCheckFlg
    
    , mlc.LIMIT_CHECK_SEQ as limitCheckSeq
    , mlc.LIMIT_COUNT_TYPE as limitCountType
    , mlc.LIMIT_COUNT as limitCount
    , mlc.LIMIT_COUNT_RESET_RANGE as limitCountResetRange
    , mlc.ACTION_RANGE as actionRange
    , mlc.NEXT_ACTION as nextAction
    
    , ml.LIMIT_SUB_NO as limitSubNo
    , ml.LIMIT_JUDGE_TYPE as limitJudgeType
    , ml.LIMIT_VALUE as limitValue
    
    , DATE_FORMAT(mdc.CREATED_AT, '%%Y/%%m/%%d %%H:%%i:%%s') as createdAt
    , greatest(ifnull(DATE_FORMAT(mdc.UPDATED_AT, '%%Y/%%m/%%d %%H:%%i:%%s'), '')
    		   , ifnull(DATE_FORMAT(mlf.UPDATED_AT, '%%Y/%%m/%%d %%H:%%i:%%s'), '')
    		   , ifnull(DATE_FORMAT(mlc.UPDATED_AT, '%%Y/%%m/%%d %%H:%%i:%%s'), '')
    		   , ifnull(DATE_FORMAT(ml.UPDATED_AT, '%%Y/%%m/%%d %%H:%%i:%%s'), '')) as updatedAt
    , CASE 
        when ifnull(mdc.UPDATED_AT, '0000-00-00 00:00:00') < ifnull(ml.UPDATED_AT, '0000-00-00 00:00:00')
         and ifnull(mlf.UPDATED_AT, '0000-00-00 00:00:00') < ifnull(ml.UPDATED_AT, '0000-00-00 00:00:00')
         and ifnull(mlc.UPDATED_AT, '0000-00-00 00:00:00') < ifnull(ml.UPDATED_AT, '0000-00-00 00:00:00')
            then ifnull(ml.UPDATED_USER, '')
         when ifnull(mdc.UPDATED_AT, '0000-00-00 00:00:00') < ifnull(mlc.UPDATED_AT, '0000-00-00 00:00:00')
         and ifnull(mlf.UPDATED_AT, '0000-00-00 00:00:00') < ifnull(mlc.UPDATED_AT, '0000-00-00 00:00:00')
         and ifnull(ml.UPDATED_AT, '0000-00-00 00:00:00') < ifnull(mlc.UPDATED_AT, '0000-00-00 00:00:00')
            then ifnull(mlc.UPDATED_USER, '')
         when ifnull(mdc.UPDATED_AT, '0000-00-00 00:00:00') < ifnull(mlf.UPDATED_AT, '0000-00-00 00:00:00')
         and ifnull(mlc.UPDATED_AT, '0000-00-00 00:00:00') < ifnull(mlf.UPDATED_AT, '0000-00-00 00:00:00')
         and ifnull(ml.UPDATED_AT, '0000-00-00 00:00:00') < ifnull(mlf.UPDATED_AT, '0000-00-00 00:00:00')
            then ifnull(mlf.UPDATED_USER, '')
        else ifnull(mdc.UPDATED_USER, '')
        end as updatedUser
    , greatest(ifnull(mdc.VERSION, 0), 
    		   ifnull(mlf.VERSION, 0), 
    		   ifnull(mlc.VERSION, 0), 
    		   ifnull(ml.VERSION, 0)) as version
from
    M_DATA_COLLECTION mdc 
	inner join M_LINK_FLG mlf 
    	on mlf.DATA_COLLECTION_SEQ = mdc.DATA_COLLECTION_SEQ 
	left outer join M_LIMIT_CHECK mlc 
		on mlc.DATA_COLLECTION_SEQ  = mdc.DATA_COLLECTION_SEQ 
	left outer join M_LIMIT ml 
	    on ml.LIMIT_CHECK_SEQ = mlc.LIMIT_CHECK_SEQ

%(p_whereParams)s	    

order by
	mdc.DEVICE_ID 
	,mdc.SENSOR_ID 
	,ml.LIMIT_SUB_NO
;

