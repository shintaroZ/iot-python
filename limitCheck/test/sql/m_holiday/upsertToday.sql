insert
into `M_PUBLIC_HOLIDAY`(
    `PUBLIC_HOLIDAY_DATE`
    , `PUBLIC_HOLIDAY_WEEK`
    , `PUBLIC_HOLIDAY_NAME`
    , `CREATED_AT`
    , `UPDATED_AT`
    , `VERSION`
)
values (
    CURRENT_DATE
    , '-'
    , 'UT_HOLIDAY'
    , CURRENT_TIMESTAMP
    , null
    , 0)
on duplicate key update
    PUBLIC_HOLIDAY_NAME = values(PUBLIC_HOLIDAY_NAME)
    , UPDATED_AT = CURRENT_TIMESTAMP
;
